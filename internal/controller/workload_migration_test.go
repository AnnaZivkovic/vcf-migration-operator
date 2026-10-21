package controller

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	configv1 "github.com/openshift/api/config/v1"
	machinev1beta1 "github.com/openshift/api/machine/v1beta1"
	migrationv1alpha1 "github.com/openshift/vcf-migration-operator/api/v1alpha1"
	"github.com/openshift/vcf-migration-operator/internal/openshift"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
)

// TestEnsureWorkloadMigratedFailureDomainMapping covers mapping-driven worker
// MachineSet creation: one destination MachineSet per mapped source MachineSet,
// each copying its source provider spec (VM size) with the destination failure
// domain's template and topology, replicas summed exactly per destination
// failure domain, source assignments kept disjoint, and explicit zero replica
// totals preserved.
func TestEnsureWorkloadMigratedFailureDomainMapping(t *testing.T) {
	ctx := context.Background()

	t.Run("creates one destination MachineSet per mapped source MachineSet", func(t *testing.T) {
		reconciler := newRolloutReconciler(t, []runtime.Object{
			newInfrastructureForMapping(
				newSourceFailureDomainForMapping("source-fd-1", "dc-source-1", "rp-a"),
				newSourceFailureDomainForMapping("source-fd-2", "dc-source-2", "rp-b"),
				newTargetFailureDomain("target-fd-1"),
				newTargetFailureDomain("target-fd-2"),
			),
			newSourceMachineSetForMapping("source-worker-a1", "dc-source-1", "rp-a", 2, 4),
			newSourceMachineSetForMapping("source-worker-a2", "dc-source-1", "rp-a", 1, 8),
			newSourceMachineSetForMapping("source-worker-b1", "dc-source-2", "rp-b", 3, 2),
		})
		migration := newMappingTestMigration(
			[]configv1.VSpherePlatformFailureDomainSpec{newTargetFailureDomain("target-fd-1"), newTargetFailureDomain("target-fd-2")},
			[]migrationv1alpha1.FailureDomainMapping{
				{Source: "source-fd-1", Destination: "target-fd-1"},
				{Source: "source-fd-2", Destination: "target-fd-2"},
			},
		)

		if _, err := reconciler.ensureWorkloadMigrated(ctx, migration); err != nil {
			t.Fatalf("ensureWorkloadMigrated: %v", err)
		}

		assertMappingMachineSet(t, ctx, reconciler, "test-infra-worker-target-fd-1", "target-fd-1", 2, 4)
		assertMappingMachineSet(t, ctx, reconciler, "test-infra-worker-target-fd-1-2", "target-fd-1", 1, 8)
		assertMappingMachineSet(t, ctx, reconciler, "test-infra-worker-target-fd-2", "target-fd-2", 3, 2)
		assertMachineSetAbsent(t, ctx, reconciler, "test-infra-worker-target-fd-2-2")
	})

	t.Run("keeps source failure domain assignments disjoint", func(t *testing.T) {
		// Two source failure domains share a datacenter but own distinct
		// resource pools; each destination failure domain must inherit exactly
		// its own machinesets' provider specs and replica counts.
		reconciler := newRolloutReconciler(t, []runtime.Object{
			newInfrastructureForMapping(
				newSourceFailureDomainForMapping("source-fd-1", "dc-source", "rp-a"),
				newSourceFailureDomainForMapping("source-fd-2", "dc-source", "rp-b"),
				newTargetFailureDomain("target-fd-1"),
				newTargetFailureDomain("target-fd-2"),
			),
			newSourceMachineSetForMapping("source-worker-a1", "dc-source", "rp-a", 2, 4),
			newSourceMachineSetForMapping("source-worker-a2", "dc-source", "rp-a", 3, 8),
			newSourceMachineSetForMapping("source-worker-b1", "dc-source", "rp-b", 1, 2),
		})
		migration := newMappingTestMigration(
			[]configv1.VSpherePlatformFailureDomainSpec{newTargetFailureDomain("target-fd-1"), newTargetFailureDomain("target-fd-2")},
			[]migrationv1alpha1.FailureDomainMapping{
				{Source: "source-fd-1", Destination: "target-fd-1"},
				{Source: "source-fd-2", Destination: "target-fd-2"},
			},
		)

		if _, err := reconciler.ensureWorkloadMigrated(ctx, migration); err != nil {
			t.Fatalf("ensureWorkloadMigrated: %v", err)
		}

		assertMappingMachineSet(t, ctx, reconciler, "test-infra-worker-target-fd-1", "target-fd-1", 2, 4)
		assertMappingMachineSet(t, ctx, reconciler, "test-infra-worker-target-fd-1-2", "target-fd-1", 3, 8)
		assertMappingMachineSet(t, ctx, reconciler, "test-infra-worker-target-fd-2", "target-fd-2", 1, 2)
		// No double counting: the second failure domain gets no spillover
		// machineset and no replica floor is applied.
		assertMachineSetAbsent(t, ctx, reconciler, "test-infra-worker-target-fd-2-2")
	})

	t.Run("preserves an explicit zero replica total", func(t *testing.T) {
		reconciler := newRolloutReconciler(t, []runtime.Object{
			newInfrastructureForMapping(
				newSourceFailureDomainForMapping("source-fd-1", "dc-source", "rp-a"),
				newTargetFailureDomain("target-fd-1"),
			),
			newSourceMachineSetForMapping("source-worker-a1", "dc-source", "rp-a", 0, 4),
			newSourceMachineSetForMapping("source-worker-a2", "dc-source", "rp-a", 0, 8),
		})
		migration := newMappingTestMigration(
			[]configv1.VSpherePlatformFailureDomainSpec{newTargetFailureDomain("target-fd-1")},
			[]migrationv1alpha1.FailureDomainMapping{{Source: "source-fd-1", Destination: "target-fd-1"}},
		)

		if _, err := reconciler.ensureWorkloadMigrated(ctx, migration); err != nil {
			t.Fatalf("ensureWorkloadMigrated: %v", err)
		}

		assertMappingMachineSet(t, ctx, reconciler, "test-infra-worker-target-fd-1", "target-fd-1", 0, 4)
		assertMappingMachineSet(t, ctx, reconciler, "test-infra-worker-target-fd-1-2", "target-fd-1", 0, 8)
		// No MachineSet may be forced to 1 to satisfy a minimum replica floor.
		for _, name := range []string{"test-infra-worker-target-fd-1", "test-infra-worker-target-fd-1-2"} {
			ms := getMappingMachineSet(t, ctx, reconciler, name)
			if ms.Spec.Replicas != nil && *ms.Spec.Replicas != 0 {
				t.Fatalf("machineset %q replicas = %d, want 0 (zero replica totals are preserved)", name, *ms.Spec.Replicas)
			}
		}
	})

	t.Run("skips a mapped source FD with no source MachineSets", func(t *testing.T) {
		reconciler := newRolloutReconciler(t, []runtime.Object{
			newInfrastructureForMapping(
				newSourceFailureDomainForMapping("source-fd-empty", "dc-source", "rp-empty"),
				newSourceFailureDomainForMapping("source-fd-1", "dc-source", "rp-a"),
				newTargetFailureDomain("target-fd-1"),
				newTargetFailureDomain("target-fd-2"),
			),
			newSourceMachineSetForMapping("source-worker-a1", "dc-source", "rp-a", 2, 4),
		})
		migration := newMappingTestMigration(
			[]configv1.VSpherePlatformFailureDomainSpec{newTargetFailureDomain("target-fd-1"), newTargetFailureDomain("target-fd-2")},
			[]migrationv1alpha1.FailureDomainMapping{
				{Source: "source-fd-empty", Destination: "target-fd-1"},
				{Source: "source-fd-1", Destination: "target-fd-2"},
			},
		)

		_, err := reconciler.ensureWorkloadMigrated(ctx, migration)
		if err != nil {
			t.Fatalf("ensureWorkloadMigrated: %v", err)
		}
		mgr := openshift.NewMachineManager(reconciler.KubeClient, reconciler.MachineClient, reconciler.DynamicClient)
		if _, err := mgr.GetMachineSet(ctx, workerMachineSetName("test-infra", "target-fd-1")); err == nil {
			t.Fatal("unexpected target MachineSet for empty source")
		}
		// A second reconcile must reach worker readiness rather than attempt
		// creation again or demand a nonexistent target for the empty source.
		if _, err := reconciler.ensureWorkloadMigrated(ctx, migration); err != nil {
			t.Fatalf("second ensureWorkloadMigrated: %v", err)
		}
		for _, cond := range migration.Status.Conditions {
			if cond.Type == migrationv1alpha1.ConditionWorkloadMigrated && !strings.Contains(cond.Message, "Waiting for target workers") {
				t.Fatalf("second reconcile message = %q, want worker readiness gate", cond.Message)
			}
		}
	})

	t.Run("errors when an existing MachineSet with a generated name targets another vCenter", func(t *testing.T) {
		reconciler := newRolloutReconciler(t, []runtime.Object{
			newInfrastructureForMapping(
				newSourceFailureDomainForMapping("source-fd-1", "dc-source", "rp-a"),
				newSourceFailureDomainForMapping("source-fd-2", "dc-source", "rp-b"),
				newTargetFailureDomain("target-fd-1"),
				newTargetFailureDomain("target-fd-2"),
			),
			// Occupies the generated name for target-fd-1 but targets a
			// different vCenter, so idempotent skipping must not apply.
			newTargetMachineSetForRollout(workerMachineSetName("test-infra", "target-fd-1"), "stray.example.com", 1),
			newSourceMachineSetForMapping("source-worker-a1", "dc-source", "rp-a", 2, 4),
			newSourceMachineSetForMapping("source-worker-b1", "dc-source", "rp-b", 1, 2),
		})
		migration := newMappingTestMigration(
			[]configv1.VSpherePlatformFailureDomainSpec{newTargetFailureDomain("target-fd-1"), newTargetFailureDomain("target-fd-2")},
			[]migrationv1alpha1.FailureDomainMapping{
				{Source: "source-fd-1", Destination: "target-fd-1"},
				{Source: "source-fd-2", Destination: "target-fd-2"},
			},
		)

		_, err := reconciler.ensureWorkloadMigrated(ctx, migration)
		if err == nil || !strings.Contains(err.Error(), "collides with existing MachineSet") {
			t.Fatalf("ensureWorkloadMigrated error = %v, want error containing %q", err, "collides with existing MachineSet")
		}
	})
}

// newSourceFailureDomainForMapping builds a source failure domain hosted on
// source.example.com with the given datacenter and resource pool.
func newSourceFailureDomainForMapping(name, datacenter, resourcePool string) configv1.VSpherePlatformFailureDomainSpec {
	return configv1.VSpherePlatformFailureDomainSpec{
		Name:   name,
		Server: "source.example.com",
		Topology: configv1.VSpherePlatformTopology{
			Datacenter:   datacenter,
			ResourcePool: resourcePool,
		},
	}
}

// newTargetFailureDomain builds a destination failure domain from the migration
// spec, hosted on target.example.com with a per-name template so copied provider
// specs can be asserted against the destination template.
func newTargetFailureDomain(name string) configv1.VSpherePlatformFailureDomainSpec {
	return configv1.VSpherePlatformFailureDomainSpec{
		Name:   name,
		Server: "target.example.com",
		Topology: configv1.VSpherePlatformTopology{
			Template:       fmt.Sprintf("/dc-target/vm/template-%s", name),
			Datacenter:     "dc-target",
			Datastore:      "ds-target",
			ResourcePool:   "rp-target",
			ComputeCluster: "cl-target",
		},
	}
}

// newInfrastructureForMapping mirrors the post-destination-init Infrastructure:
// both vCenters and both source and destination failure domains are present.
func newInfrastructureForMapping(fds ...configv1.VSpherePlatformFailureDomainSpec) *configv1.Infrastructure {
	return &configv1.Infrastructure{
		ObjectMeta: metav1.ObjectMeta{Name: openshift.InfrastructureName},
		Spec: configv1.InfrastructureSpec{
			PlatformSpec: configv1.PlatformSpec{
				Type: configv1.VSpherePlatformType,
				VSphere: &configv1.VSpherePlatformSpec{
					VCenters: []configv1.VSpherePlatformVCenterSpec{
						{Server: "source.example.com"},
						{Server: "target.example.com"},
					},
					FailureDomains: fds,
				},
			},
		},
		Status: configv1.InfrastructureStatus{
			InfrastructureName: "test-infra",
		},
	}
}

// newMappingTestMigration builds a migration whose destination failure domains
// and preflight-recorded effective mapping are set, as Reconcile would see
// them once the workload phase starts.
func newMappingTestMigration(destFDs []configv1.VSpherePlatformFailureDomainSpec, mappings []migrationv1alpha1.FailureDomainMapping) *migrationv1alpha1.VmwareCloudFoundationMigration {
	return &migrationv1alpha1.VmwareCloudFoundationMigration{
		ObjectMeta: metav1.ObjectMeta{Name: migrationv1alpha1.SingletonName, Generation: 1},
		Spec: migrationv1alpha1.VmwareCloudFoundationMigrationSpec{
			FailureDomains: destFDs,
		},
		Status: migrationv1alpha1.VmwareCloudFoundationMigrationStatus{
			FailureDomainMappings: mappings,
		},
	}
}

// newSourceMachineSetForMapping builds a source MachineSet in the given
// datacenter and resource pool with a distinct VM size (numCPUs) so tests can
// assert that each destination MachineSet copies its own source provider spec.
func newSourceMachineSetForMapping(name, datacenter, resourcePool string, replicas, numCPUs int32) *machinev1beta1.MachineSet {
	return &machinev1beta1.MachineSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: openshift.MachineAPINamespace,
		},
		Spec: machinev1beta1.MachineSetSpec{
			Replicas: &replicas,
			Selector: metav1.LabelSelector{
				MatchLabels: map[string]string{
					"machine.openshift.io/cluster-api-machineset": name,
				},
			},
			Template: machinev1beta1.MachineTemplateSpec{
				ObjectMeta: machinev1beta1.ObjectMeta{
					Labels: map[string]string{
						"machine.openshift.io/cluster-api-machineset": name,
					},
				},
				Spec: machinev1beta1.MachineSpec{
					ProviderSpec: machinev1beta1.ProviderSpec{
						Value: &runtime.RawExtension{Raw: []byte(fmt.Sprintf(
							`{"kind":"VSphereMachineProviderSpec","apiVersion":"machine.openshift.io/v1beta1","numCPUs":%d,"workspace":{"server":"source.example.com","datacenter":%q,"resourcePool":%q}}`,
							numCPUs, datacenter, resourcePool,
						))},
					},
				},
			},
		},
	}
}

func getMappingMachineSet(t *testing.T, ctx context.Context, reconciler *VmwareCloudFoundationMigrationReconciler, name string) *machinev1beta1.MachineSet {
	t.Helper()
	ms, err := reconciler.MachineClient.MachineV1beta1().MachineSets(openshift.MachineAPINamespace).Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("getting machineset %q: %v", name, err)
	}
	return ms
}

// assertMappingMachineSet verifies a copied destination MachineSet: exact
// replicas from its source MachineSet, the source MachineSet's VM size, and
// the destination failure domain's template and vCenter.
func assertMappingMachineSet(t *testing.T, ctx context.Context, reconciler *VmwareCloudFoundationMigrationReconciler, name, fdName string, replicas, numCPUs int32) {
	t.Helper()
	ms := getMappingMachineSet(t, ctx, reconciler, name)
	if ms.Spec.Replicas == nil || *ms.Spec.Replicas != replicas {
		t.Fatalf("machineset %q replicas = %v, want %d", name, ms.Spec.Replicas, replicas)
	}
	providerSpec := mappingTestProviderSpec(t, ms)
	if providerSpec.NumCPUs != numCPUs {
		t.Fatalf("machineset %q numCPUs = %d, want %d (source provider spec must be copied)", name, providerSpec.NumCPUs, numCPUs)
	}
	wantTemplate := fmt.Sprintf("/dc-target/vm/template-%s", fdName)
	if providerSpec.Template != wantTemplate {
		t.Fatalf("machineset %q template = %q, want %q (destination failure domain template)", name, providerSpec.Template, wantTemplate)
	}
	if providerSpec.Workspace == nil || providerSpec.Workspace.Server != "target.example.com" {
		t.Fatalf("machineset %q workspace = %+v, want server target.example.com", name, providerSpec.Workspace)
	}
}

func assertMachineSetAbsent(t *testing.T, ctx context.Context, reconciler *VmwareCloudFoundationMigrationReconciler, name string) {
	t.Helper()
	if _, err := reconciler.MachineClient.MachineV1beta1().MachineSets(openshift.MachineAPINamespace).Get(ctx, name, metav1.GetOptions{}); err == nil {
		t.Fatalf("machineset %q exists, want absent", name)
	}
}

func mappingTestProviderSpec(t *testing.T, ms *machinev1beta1.MachineSet) *machinev1beta1.VSphereMachineProviderSpec {
	t.Helper()
	providerSpec := &machinev1beta1.VSphereMachineProviderSpec{}
	if err := json.Unmarshal(ms.Spec.Template.Spec.ProviderSpec.Value.Raw, providerSpec); err != nil {
		t.Fatalf("unmarshalling provider spec from machineset %q: %v", ms.Name, err)
	}
	return providerSpec
}
