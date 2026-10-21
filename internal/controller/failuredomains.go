package controller

import (
	"context"
	"fmt"
	"slices"
	"sort"
	"strings"

	configv1 "github.com/openshift/api/config/v1"
	machinev1beta1 "github.com/openshift/api/machine/v1beta1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/klog/v2"

	migrationv1alpha1 "github.com/openshift/vcf-migration-operator/api/v1alpha1"
	"github.com/openshift/vcf-migration-operator/internal/openshift"
)

// resolveFailureDomainMappings returns the effective source-to-destination
// failure domain mapping. An explicit mapping list must reference only known
// failure domain names and must cover each side exactly once; an omitted list
// is only valid when both sides have exactly one failure domain, in which case
// the inferred pair is returned.
func resolveFailureDomainMappings(mappings []migrationv1alpha1.FailureDomainMapping, sourceFDs, destFDs []configv1.VSpherePlatformFailureDomainSpec) ([]migrationv1alpha1.FailureDomainMapping, error) {
	sourceNames := make(map[string]bool, len(sourceFDs))
	for i := range sourceFDs {
		sourceNames[sourceFDs[i].Name] = true
	}
	destNames := make(map[string]bool, len(destFDs))
	for i := range destFDs {
		destNames[destFDs[i].Name] = true
	}

	if len(mappings) == 0 {
		if len(sourceFDs) == 1 && len(destFDs) == 1 {
			return []migrationv1alpha1.FailureDomainMapping{{Source: sourceFDs[0].Name, Destination: destFDs[0].Name}}, nil
		}
		return nil, fmt.Errorf("spec.failureDomainMappings is required: provide mappings when the source and destination failure domains are not a single pair (got %d source and %d destination failure domains)", len(sourceFDs), len(destFDs))
	}

	for i := range mappings {
		if !sourceNames[mappings[i].Source] {
			return nil, fmt.Errorf("failure domain mapping references unknown source failure domain %q", mappings[i].Source)
		}
		if !destNames[mappings[i].Destination] {
			return nil, fmt.Errorf("failure domain mapping references unknown destination failure domain %q", mappings[i].Destination)
		}
	}

	if len(mappings) != len(sourceFDs) || len(mappings) != len(destFDs) {
		return nil, fmt.Errorf("spec.failureDomainMappings must cover every source and destination failure domain exactly once: got %d mappings for %d source and %d destination failure domains", len(mappings), len(sourceFDs), len(destFDs))
	}

	return mappings, nil
}

// ensureMappingImmutable enforces the immutability of the effective failure
// domain mapping recorded in status at preflight. It deliberately does not
// re-resolve from Infrastructure: the Infrastructure legitimately changes
// during migration (destination failure domains added, source ones removed).
// Clearing spec.failureDomainMappings after preflight is allowed because the
// recorded effective mapping keeps driving every later phase.
func (r *VmwareCloudFoundationMigrationReconciler) ensureMappingImmutable(migration *migrationv1alpha1.VmwareCloudFoundationMigration) error {
	recorded := migration.Status.FailureDomainMappings
	if len(recorded) == 0 || len(migration.Spec.FailureDomainMappings) == 0 {
		return nil
	}

	// Compare order-insensitively: reordered but equivalent entries are valid.
	requested := mappingBySource(migration.Spec.FailureDomainMappings)
	bySource := mappingBySource(recorded)
	if len(requested) != len(bySource) {
		return fmt.Errorf("failureDomainMappings are immutable after preflight: got %d mappings, want %d as recorded in status", len(requested), len(bySource))
	}
	for source, dest := range requested {
		want, ok := bySource[source]
		if !ok {
			return fmt.Errorf("failureDomainMappings are immutable after preflight: source failure domain %q is not part of the recorded mapping", source)
		}
		if want != dest {
			return fmt.Errorf("failureDomainMappings are immutable after preflight: source failure domain %q is mapped to %q, want %q as recorded in status", source, dest, want)
		}
	}
	return nil
}

// mappingBySource keys failure domain mappings by source name so lists can be
// compared order-insensitively.
func mappingBySource(mappings []migrationv1alpha1.FailureDomainMapping) map[string]string {
	bySource := make(map[string]string, len(mappings))
	for i := range mappings {
		bySource[mappings[i].Source] = mappings[i].Destination
	}
	return bySource
}

// validateFailureDomainServers verifies that all destination failure domains
// reference the same, non-empty vCenter server. Multiple destination servers
// are out of scope for this release.
func validateFailureDomainServers(fds []configv1.VSpherePlatformFailureDomainSpec) error {
	for i := range fds {
		if fds[i].Server == "" {
			return fmt.Errorf("failure domain %q has no vCenter server configured", fds[i].Name)
		}
		if fds[i].Server != fds[0].Server {
			return fmt.Errorf("spec.failureDomains must use a single vCenter server: %q and %q are both present", fds[0].Server, fds[i].Server)
		}
	}
	return nil
}

// validateSourceVCenterCount verifies that the source Infrastructure has
// exactly one vCenter entry. This holds only at preflight time, before the
// destination vCenter is added.
func validateSourceVCenterCount(infra *configv1.Infrastructure) error {
	if infra.Spec.PlatformSpec.VSphere == nil {
		return fmt.Errorf("infrastructure spec has no vSphere platform configuration")
	}
	if n := len(infra.Spec.PlatformSpec.VSphere.VCenters); n != 1 {
		return fmt.Errorf("source infrastructure must have exactly one vCenter, found %d", n)
	}
	return nil
}

// validateSourceFDTopologyDisjoint verifies that source failure domains
// sharing a datacenter each specify a distinct, non-empty resource pool, so a
// source MachineSet can match at most one failure domain.
func validateSourceFDTopologyDisjoint(sourceFDs []configv1.VSpherePlatformFailureDomainSpec) error {
	for i := range sourceFDs {
		for j := i + 1; j < len(sourceFDs); j++ {
			a, b := &sourceFDs[i], &sourceFDs[j]
			if a.Topology.Datacenter != b.Topology.Datacenter {
				continue
			}
			if a.Topology.ResourcePool == "" || b.Topology.ResourcePool == "" || a.Topology.ResourcePool == b.Topology.ResourcePool {
				return fmt.Errorf("source failure domains %q and %q have overlapping topology in datacenter %q: each must specify a distinct resource pool", a.Name, b.Name, a.Topology.Datacenter)
			}
		}
	}
	return nil
}

// validateMachineSetNameCollisions verifies that the worker MachineSet names
// generated for the destination failure domains do not collide with existing
// MachineSets. It runs at preflight, before the mappings are recorded as
// immutable, so renaming the destination failure domain is still possible.
func (r *VmwareCloudFoundationMigrationReconciler) validateMachineSetNameCollisions(ctx context.Context, infraID string, mappings []migrationv1alpha1.FailureDomainMapping, sourceFDs, destFDs []configv1.VSpherePlatformFailureDomainSpec) error {
	log := klog.FromContext(ctx)

	msList, err := r.MachineClient.MachineV1beta1().MachineSets(openshift.MachineAPINamespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		return fmt.Errorf("listing machinesets in %s: %w", openshift.MachineAPINamespace, err)
	}
	log.V(2).Info("checking generated MachineSet names for collisions", "machinesets", len(msList.Items))

	generated := make(map[string]bool)
	bases := make([]string, 0, len(mappings))
	for i := range mappings {
		var fd *configv1.VSpherePlatformFailureDomainSpec
		for j := range destFDs {
			if destFDs[j].Name == mappings[i].Destination {
				fd = &destFDs[j]
				break
			}
		}
		if fd == nil {
			return fmt.Errorf("resolving destination failure domain %q for mapping from %q", mappings[i].Destination, mappings[i].Source)
		}
		base := workerMachineSetName(infraID, fd.Name)
		for _, previous := range bases {
			if base == previous || isSuffixedMachineSetName(base, previous) || isSuffixedMachineSetName(previous, base) {
				return fmt.Errorf("generated MachineSet names for %q and %q overlap; rename the destination failure domain", previous, base)
			}
		}
		bases = append(bases, base)
		source := findFailureDomainByName(sourceFDs, mappings[i].Source)
		if source == nil {
			return fmt.Errorf("resolving source failure domain %q", mappings[i].Source)
		}
		sourceSets, err := openshift.NewMachineManager(r.KubeClient, r.MachineClient, r.DynamicClient).GetMachineSetsByFailureDomain(ctx, source.Server, source)
		if err != nil {
			return fmt.Errorf("listing machinesets for source failure domain %q: %w", source.Name, err)
		}
		for si := 0; si < max(1, len(sourceSets)); si++ {
			name := suffixedWorkerMachineSetName(base, si)
			if len(name) > maxGeneratedMachineSetNameLen {
				return fmt.Errorf("generated MachineSet name %q exceeds the %d-character Kubernetes label limit; rename the destination failure domain", name, maxGeneratedMachineSetNameLen)
			}
			if generated[name] {
				return fmt.Errorf("generated MachineSet name %q collides with another destination", name)
			}
			generated[name] = true
		}
		for j := range msList.Items {
			name := msList.Items[j].Name
			if name == base || isSuffixedMachineSetName(name, base) {
				return fmt.Errorf("generated MachineSet name %q collides with existing MachineSet; rename the destination failure domain", name)
			}
		}
	}

	return nil
}

// suffixedWorkerMachineSetName returns the generated worker MachineSet name
// for the source MachineSet at index si (0-based) copied into a destination
// failure domain: the base name for the first copy, then base-2, base-3, ...
// so multiple source MachineSets mapped to one failure domain get distinct
// names.
func suffixedWorkerMachineSetName(base string, si int) string {
	if si == 0 {
		return base
	}
	return fmt.Sprintf("%s-%d", base, si+1)
}

// isSuffixedMachineSetName reports whether name is base with a numeric suffix
// (base-2, base-3, ...), as generated when one failure domain receives
// multiple copied source MachineSets.
func isSuffixedMachineSetName(name, base string) bool {
	suffix, ok := strings.CutPrefix(name, base+"-")
	if !ok || suffix == "" {
		return false
	}
	for _, r := range suffix {
		if r < '0' || r > '9' {
			return false
		}
	}
	return true
}

// recordedWorkerMachineSetNames returns the recorded destination MachineSet
// names matching base (base itself plus suffixed copies), sorted. Recorded
// names survive source cleanup, unlike the source MachineSets they were
// derived from.
func recordedWorkerMachineSetNames(recorded []string, base string) []string {
	var names []string
	for _, name := range recorded {
		if name == base || isSuffixedMachineSetName(name, base) {
			names = append(names, name)
		}
	}
	sort.Strings(names)
	return names
}

// workerMachineSetNamesForBase returns the names of the worker MachineSets
// generated for the destination failure domain whose base name is base: base
// itself plus every suffixed copy (base-2, base-3, ...) present in msList.
// base is always included so a missing base MachineSet still surfaces as an
// error from the caller's per-name lookups.
func workerMachineSetNamesForBase(msList []*machinev1beta1.MachineSet, base string) []string {
	names := []string{base}
	for _, ms := range msList {
		if isSuffixedMachineSetName(ms.Name, base) {
			names = append(names, ms.Name)
		}
	}
	return names
}

// matchSourceMachineSetFailureDomain resolves the source failure domain a
// MachineSet belongs to. It returns whether the MachineSet runs replicas
// (nil replicas default to 1 in MAPI, so nil counts as running capacity), the
// machine's workspace, and the single matching source failure domain, or nil
// when none matches.
func matchSourceMachineSetFailureDomain(ms *machinev1beta1.MachineSet, sourceFDs []configv1.VSpherePlatformFailureDomainSpec) (bool, *machinev1beta1.Workspace, *configv1.VSpherePlatformFailureDomainSpec, error) {
	providerSpec, err := openshift.ExtractVSphereProviderSpec(ms)
	if err != nil {
		return false, nil, nil, fmt.Errorf("extracting provider spec from machineset %q: %w", ms.Name, err)
	}
	if providerSpec.Workspace == nil {
		return false, nil, nil, fmt.Errorf("extracting provider spec from machineset %q: workspace is nil", ms.Name)
	}
	hasReplicas := ms.Spec.Replicas == nil || *ms.Spec.Replicas > 0
	matched := 0
	var matchedFD *configv1.VSpherePlatformFailureDomainSpec
	for i := range sourceFDs {
		if matchesSourceFailureDomain(providerSpec.Workspace, sourceFDs[i]) {
			matched++
			matchedFD = &sourceFDs[i]
		}
	}
	if matched > 1 {
		return false, nil, nil, fmt.Errorf("validating source machineset coverage: machineset %q matches multiple source failure domains", ms.Name)
	}
	return hasReplicas, providerSpec.Workspace, matchedFD, nil
}

// validateSourceMachineSetMappingCoverage checks that every source-vCenter
// MachineSet with running replicas matches exactly one source failure domain
// that has a mapping. Unlike validateSourceMachineSetCoverage it does not
// require recorded destination MachineSet names, so it runs at preflight
// before the replacements exist. Empty source failure domains and
// zero-replica MachineSets are fine: neither runs capacity that needs a
// replacement.
func validateSourceMachineSetMappingCoverage(ctx context.Context, msList []*machinev1beta1.MachineSet, mappings []migrationv1alpha1.FailureDomainMapping, sourceFDs []configv1.VSpherePlatformFailureDomainSpec) error {
	log := klog.FromContext(ctx)

	for _, ms := range msList {
		hasReplicas, workspace, matchedFD, err := matchSourceMachineSetFailureDomain(ms, sourceFDs)
		if err != nil {
			return err
		}
		if !hasReplicas {
			log.V(1).Info("skipping zero-replica source MachineSet for coverage gate", "machineSet", ms.Name)
			continue
		}
		if matchedFD == nil {
			return fmt.Errorf(
				"validating source machineset coverage: machineset %q (datacenter %q, resource pool %q) does not match any source failure domain",
				ms.Name, workspace.Datacenter, workspace.ResourcePool)
		}
		var mapping *migrationv1alpha1.FailureDomainMapping
		for i := range mappings {
			if mappings[i].Source == matchedFD.Name {
				mapping = &mappings[i]
				break
			}
		}
		if mapping == nil {
			return fmt.Errorf("validating source machineset coverage: machineset %q matches source failure domain %q which has no mapping", ms.Name, matchedFD.Name)
		}
	}
	return nil
}

// validateSourceMachineSetCoverage fails closed before source worker
// scale-down when a source-vCenter MachineSet with running replicas is not
// covered by the failure domain mapping with a verified replacement
// MachineSet. Zero-replica MachineSets are skipped and logged: they run no
// machines, so no replacement is needed. msList is the fresh source-vCenter
// listing the caller already has.
func validateSourceMachineSetCoverage(ctx context.Context, msList []*machinev1beta1.MachineSet, infraID string, mappings []migrationv1alpha1.FailureDomainMapping, sourceFDs []configv1.VSpherePlatformFailureDomainSpec, recorded []string) error {
	log := klog.FromContext(ctx)

	if err := validateSourceMachineSetMappingCoverage(ctx, msList, mappings, sourceFDs); err != nil {
		return err
	}

	for _, ms := range msList {
		hasReplicas, _, matchedFD, err := matchSourceMachineSetFailureDomain(ms, sourceFDs)
		if err != nil {
			return err
		}
		if !hasReplicas {
			log.V(1).Info("skipping zero-replica source MachineSet for coverage gate", "machineSet", ms.Name)
			continue
		}
		var mapping *migrationv1alpha1.FailureDomainMapping
		for i := range mappings {
			if mappings[i].Source == matchedFD.Name {
				mapping = &mappings[i]
				break
			}
		}
		if mapping == nil {
			continue
		}
		base := workerMachineSetName(infraID, mapping.Destination)
		if len(recordedWorkerMachineSetNames(recorded, base)) == 0 {
			return fmt.Errorf("validating source machineset coverage: machineset %q has no recorded destination MachineSet for failure domain %q", ms.Name, mapping.Destination)
		}
	}
	return nil
}

// matchesSourceFailureDomain reports whether a provider spec workspace falls
// within the failure domain: same datacenter and, when the failure domain
// specifies one, same resource pool. An empty failure-domain resource pool
// matches every pool in that datacenter. This is the same predicate as
// MachineManager.GetMachineSetsByFailureDomain.
func matchesSourceFailureDomain(workspace *machinev1beta1.Workspace, fd configv1.VSpherePlatformFailureDomainSpec) bool {
	if workspace.Datacenter != fd.Topology.Datacenter {
		return false
	}
	return fd.Topology.ResourcePool == "" || workspace.ResourcePool == fd.Topology.ResourcePool
}

// recoverUnrecordedDestinationNames re-records destination worker MachineSet
// names that were created but lost from status by a crash between creation and
// updateStatus. The destination failure domain list comes from the migration
// spec because sourceFDs holds only infrastructure (source) failure domains.
// Derived names are re-derived from the still-present source MachineSets in
// the creation loop's deterministic order; each unrecorded name must resolve
// to a MachineSet that exists and targets the destination failure domain's
// vCenter, so an incomplete creation fails closed instead of letting
// scale-down drop capacity.
func recoverUnrecordedDestinationNames(ctx context.Context, machineMgr *openshift.MachineManager, migration *migrationv1alpha1.VmwareCloudFoundationMigration, msList []*machinev1beta1.MachineSet, infraID string, mappings []migrationv1alpha1.FailureDomainMapping, sourceFDs []configv1.VSpherePlatformFailureDomainSpec) ([]string, error) {
	log := klog.FromContext(ctx)
	// Names are only recorded once every mapping validated, so an error
	// leaves WorkerMachineSetNames untouched.
	var recovered []string

	for i := range mappings {
		sourceFD := findFailureDomainByName(sourceFDs, mappings[i].Source)
		if sourceFD == nil {
			// The source failure domain is already cleaned up; the coverage
			// gate's zero-replica skip governs that case.
			log.V(1).Info("skipping name recovery, source failure domain absent", "sourceFailureDomain", mappings[i].Source)
			continue
		}
		var matched []*machinev1beta1.MachineSet
		for _, ms := range msList {
			providerSpec, err := openshift.ExtractVSphereProviderSpec(ms)
			if err != nil {
				return nil, fmt.Errorf("extracting provider spec from machineset %q: %w", ms.Name, err)
			}
			if providerSpec.Workspace == nil {
				return nil, fmt.Errorf("extracting provider spec from machineset %q: workspace is nil", ms.Name)
			}
			if matchesSourceFailureDomain(providerSpec.Workspace, *sourceFD) {
				matched = append(matched, ms)
			}
		}
		if len(matched) == 0 {
			// Empty source mapping stays intentionally empty.
			log.V(1).Info("skipping name recovery, no source MachineSets", "sourceFailureDomain", mappings[i].Source)
			continue
		}
		// Same deterministic order as the creation loop, keeping the suffixed
		// names (-2, -3, ...) stable across reconciles.
		sort.Slice(matched, func(a, b int) bool { return matched[a].Name < matched[b].Name })
		destFD := findFailureDomainByName(migration.Spec.FailureDomains, mappings[i].Destination)
		if destFD == nil {
			return nil, fmt.Errorf("resolving destination failure domain %q for mapping from %q", mappings[i].Destination, mappings[i].Source)
		}
		base := workerMachineSetName(infraID, mappings[i].Destination)
		for si := range matched {
			name := suffixedWorkerMachineSetName(base, si)
			if slices.Contains(migration.Status.WorkerMachineSetNames, name) || slices.Contains(recovered, name) {
				continue
			}
			existing, err := machineMgr.GetMachineSet(ctx, name)
			if err != nil {
				if apierrors.IsNotFound(err) {
					return nil, fmt.Errorf("recovering unrecorded destination MachineSets: machineset %q does not exist; re-run required", name)
				}
				return nil, fmt.Errorf("getting machineset %q: %w", name, err)
			}
			providerSpec, err := openshift.ExtractVSphereProviderSpec(existing)
			if err != nil {
				return nil, fmt.Errorf("extracting provider spec from machineset %q: %w", name, err)
			}
			// Same collision check as the creation loop: a MachineSet under a
			// generated name that targets another vCenter is a collision, not a
			// recovered replacement.
			if providerSpec.Workspace == nil || providerSpec.Workspace.Server != destFD.Server {
				return nil, fmt.Errorf("generated MachineSet name %q collides with existing MachineSet; rename the destination failure domain", name)
			}
			recovered = append(recovered, name)
			log.V(2).Info("recovered unrecorded destination MachineSet name", "name", name, "sourceMachineSet", matched[si].Name)
		}
	}
	migration.Status.WorkerMachineSetNames = append(migration.Status.WorkerMachineSetNames, recovered...)
	return recovered, nil
}
