package controller

import (
	"context"
	"slices"
	"strings"
	"testing"

	configv1 "github.com/openshift/api/config/v1"
	machinev1beta1 "github.com/openshift/api/machine/v1beta1"
	"k8s.io/apimachinery/pkg/runtime"

	migrationv1alpha1 "github.com/openshift/vcf-migration-operator/api/v1alpha1"
	"github.com/openshift/vcf-migration-operator/internal/openshift"
)

// newOutOfTopologySourceMachineSetForRollout builds a source-vCenter
// MachineSet placed in a datacenter covered by no source failure domain,
// mirroring newSourceMachineSetForRolloutNamed.
func newOutOfTopologySourceMachineSetForRollout(name string, replicas int32) *machinev1beta1.MachineSet {
	ms := newTargetMachineSetForRollout(name, "source.example.com", replicas)
	ms.Spec.Template.Spec.ProviderSpec.Value = &runtime.RawExtension{Raw: []byte(
		`{"kind":"VSphereMachineProviderSpec","apiVersion":"machine.openshift.io/v1beta1","workspace":{"server":"source.example.com","datacenter":"dc-other","resourcePool":"rp-other"}}`)}
	return ms
}

func TestValidateSourceMachineSetCoverage(t *testing.T) {
	const infraID = "test-infra"
	base := workerMachineSetName(infraID, "target-fd-1")
	mappings := []migrationv1alpha1.FailureDomainMapping{{Source: "source-fd-1", Destination: "target-fd-1"}}
	sourceFDs := []configv1.VSpherePlatformFailureDomainSpec{newSourceFailureDomainForRollout()}

	// case 7 reuses the out-of-topology MachineSet with nil replicas.
	nilReplica := newOutOfTopologySourceMachineSetForRollout("source-worker-nil", 1)
	nilReplica.Spec.Replicas = nil
	// case 6 reuses a mapped MachineSet whose provider spec cannot be parsed.
	unparseable := newSourceMachineSetForRollout(1)
	unparseable.Spec.Template.Spec.ProviderSpec.Value = nil

	tests := []struct {
		name      string
		ms        *machinev1beta1.MachineSet
		rec       []string
		sourceFDs []configv1.VSpherePlatformFailureDomainSpec
		wantErr   string
	}{
		{
			name:    "mapped >0 with recorded replacement passes",
			ms:      newSourceMachineSetForRollout(1),
			rec:     []string{base, base + "-2"},
			wantErr: "",
		},
		{
			name:    "mapped >0 without recorded replacement errors",
			ms:      newSourceMachineSetForRollout(1),
			rec:     nil,
			wantErr: "no recorded destination",
		},
		{
			name:    "unmapped >0 stops migration",
			ms:      newOutOfTopologySourceMachineSetForRollout("source-worker-b", 1),
			rec:     []string{base, base + "-2"},
			wantErr: "does not match any source failure domain",
		},
		{
			name:    "unmapped zero-replica skipped",
			ms:      newOutOfTopologySourceMachineSetForRollout("source-worker-b", 0),
			rec:     []string{base, base + "-2"},
			wantErr: "",
		},
		{
			name:    "mapped zero-replica skipped",
			ms:      newSourceMachineSetForRollout(0),
			rec:     nil,
			wantErr: "",
		},
		{
			name:    "unparseable provider spec errors",
			ms:      unparseable,
			rec:     []string{base, base + "-2"},
			wantErr: "extracting provider spec",
		},
		{
			name:    "nil replicas treated as >0 and stops migration",
			ms:      nilReplica,
			rec:     []string{base, base + "-2"},
			wantErr: "does not match any source failure domain",
		},
		{
			name: "matching two failure domains errors",
			ms:   newSourceMachineSetForRollout(1),
			rec:  []string{base, base + "-2"},
			sourceFDs: []configv1.VSpherePlatformFailureDomainSpec{
				newSourceFailureDomainForRollout(),
				{Name: "source-fd-2", Server: "source.example.com", Topology: configv1.VSpherePlatformTopology{Datacenter: "dc-source", ResourcePool: ""}},
			},
			wantErr: "matches multiple source failure domains",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fds := tt.sourceFDs
			if fds == nil {
				fds = sourceFDs
			}
			err := validateSourceMachineSetCoverage(context.Background(), []*machinev1beta1.MachineSet{tt.ms}, infraID, mappings, fds, tt.rec)
			if tt.wantErr == "" {
				if err != nil {
					t.Fatalf("validateSourceMachineSetCoverage: %v", err)
				}
				return
			}
			if err == nil {
				t.Fatalf("validateSourceMachineSetCoverage: nil error, want containing %q", tt.wantErr)
			}
			if !strings.Contains(err.Error(), tt.wantErr) {
				t.Fatalf("validateSourceMachineSetCoverage: %v, want containing %q", err, tt.wantErr)
			}
		})
	}
}

// TestRecoverUnrecordedDestinationNames covers the restart path where
// destination MachineSets were created but a crash before updateStatus lost
// the WorkerMachineSetNames recording: derived names must be re-derived from
// the still-present source MachineSets and verified against the destination
// vCenter before being recorded again.
func TestRecoverUnrecordedDestinationNames(t *testing.T) {
	const infraID = "test-infra"
	base := workerMachineSetName(infraID, "target-fd-1")
	sourceFD := newSourceFailureDomainForRollout()
	destFD := configv1.VSpherePlatformFailureDomainSpec{Name: "target-fd-1", Server: "target.example.com"}
	mappings := []migrationv1alpha1.FailureDomainMapping{{Source: "source-fd-1", Destination: "target-fd-1"}}

	// Listed out of order: recovery must sort by name so suffixed indices
	// line up with the creation loop's deterministic order.
	msList := []*machinev1beta1.MachineSet{
		newSourceMachineSetForRolloutNamed("source-worker-b", 1),
		newSourceMachineSetForRolloutNamed("source-worker-a", 1),
	}

	newMigration := func(recorded []string) *migrationv1alpha1.VmwareCloudFoundationMigration {
		return &migrationv1alpha1.VmwareCloudFoundationMigration{
			Spec: migrationv1alpha1.VmwareCloudFoundationMigrationSpec{
				FailureDomains: []configv1.VSpherePlatformFailureDomainSpec{destFD},
			},
			Status: migrationv1alpha1.VmwareCloudFoundationMigrationStatus{
				WorkerMachineSetNames: recorded,
			},
		}
	}

	tests := []struct {
		name         string
		objects      []runtime.Object
		msList       []*machinev1beta1.MachineSet
		recorded     []string
		sourceFDs    []configv1.VSpherePlatformFailureDomainSpec
		wantErr      string
		wantRecorded []string
	}{
		{
			name: "both destination MachineSets recorded when creation completed",
			objects: []runtime.Object{
				newTargetMachineSetForRollout(base, "target.example.com", 1),
				newTargetMachineSetForRollout(base+"-2", "target.example.com", 1),
			},
			msList:       msList,
			recorded:     nil,
			wantErr:      "",
			wantRecorded: []string{base, base + "-2"},
		},
		{
			name: "already recorded name not duplicated",
			objects: []runtime.Object{
				newTargetMachineSetForRollout(base, "target.example.com", 1),
				newTargetMachineSetForRollout(base+"-2", "target.example.com", 1),
			},
			msList:       msList,
			recorded:     []string{base},
			wantErr:      "",
			wantRecorded: []string{base, base + "-2"},
		},
		{
			name: "missing destination MachineSet fails closed",
			objects: []runtime.Object{
				newTargetMachineSetForRollout(base, "target.example.com", 1),
			},
			msList:       msList,
			recorded:     nil,
			wantErr:      "does not exist",
			wantRecorded: nil,
		},
		{
			name: "destination MachineSet on wrong vCenter is a collision",
			objects: []runtime.Object{
				newTargetMachineSetForRollout(base, "target.example.com", 1),
				newTargetMachineSetForRollout(base+"-2", "other.example.com", 1),
			},
			msList:       msList,
			recorded:     nil,
			wantErr:      "collides",
			wantRecorded: nil,
		},
		{
			name:         "source failure domain absent from infrastructure skips mapping",
			objects:      []runtime.Object{newTargetMachineSetForRollout(base, "target.example.com", 1)},
			msList:       msList,
			recorded:     nil,
			sourceFDs:    []configv1.VSpherePlatformFailureDomainSpec{{Name: "source-fd-gone", Server: "source.example.com"}},
			wantErr:      "",
			wantRecorded: nil,
		},
		{
			name:         "source mapping with no matching MachineSets stays empty",
			objects:      []runtime.Object{newTargetMachineSetForRollout(base, "target.example.com", 1)},
			msList:       []*machinev1beta1.MachineSet{newOutOfTopologySourceMachineSetForRollout("source-worker-other", 1)},
			recorded:     nil,
			wantErr:      "",
			wantRecorded: nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fds := tt.sourceFDs
			if fds == nil {
				fds = []configv1.VSpherePlatformFailureDomainSpec{sourceFD}
			}
			r := newRolloutReconciler(t, tt.objects)
			machineMgr := openshift.NewMachineManager(r.KubeClient, r.MachineClient, r.DynamicClient)
			migration := newMigration(tt.recorded)
			_, err := recoverUnrecordedDestinationNames(context.Background(), machineMgr, migration, tt.msList, infraID, mappings, fds)
			if tt.wantErr == "" {
				if err != nil {
					t.Fatalf("recoverUnrecordedDestinationNames: %v", err)
				}
			} else {
				if err == nil {
					t.Fatalf("recoverUnrecordedDestinationNames: nil error, want containing %q", tt.wantErr)
				}
				if !strings.Contains(err.Error(), tt.wantErr) {
					t.Fatalf("recoverUnrecordedDestinationNames: %v, want containing %q", err, tt.wantErr)
				}
			}
			if !slices.Equal(migration.Status.WorkerMachineSetNames, tt.wantRecorded) {
				t.Fatalf("WorkerMachineSetNames = %v, want %v", migration.Status.WorkerMachineSetNames, tt.wantRecorded)
			}
		})
	}
}

func TestGeneratedDestinationNameCollisions(t *testing.T) {
	const target = "target"
	for _, names := range [][]string{{target, "target-2"}, {"target-2", target}, {"Target", target}, {"target-a", "target-b"}} {
		t.Run(names[0]+"_"+names[1], func(t *testing.T) {
			r := newRolloutReconciler(t, nil)
			source := []configv1.VSpherePlatformFailureDomainSpec{{Name: "source-a", Server: cleanupTestSource}, {Name: "source-b", Server: cleanupTestSource}}
			dest := []configv1.VSpherePlatformFailureDomainSpec{{Name: names[0]}, {Name: names[1]}}
			mappings := []migrationv1alpha1.FailureDomainMapping{{Source: "source-a", Destination: names[0]}, {Source: "source-b", Destination: names[1]}}
			err := r.validateMachineSetNameCollisions(context.Background(), "infra", mappings, source, dest)
			wantError := names[0] != "target-a"
			if (err != nil) != wantError {
				t.Fatalf("validateMachineSetNameCollisions = %v, want error %v", err, wantError)
			}
		})
	}
}

func TestWorkerReadinessDesiredReplicas(t *testing.T) {
	for _, replicas := range []int32{0, 1, 2} {
		ms := newTargetMachineSetForRollout(workerMachineSetName("infra", "target"), "target.example.com", replicas)
		machine := newReadyWorkerMachineForRollout(ms.Name)
		r := newRolloutReconciler(t, []runtime.Object{ms, machine, newReadyNodeForRollout(machine.Status.NodeRef.Name)})
		mgr := openshift.NewMachineManager(r.KubeClient, r.MachineClient, r.DynamicClient)
		got, err := checkWorkerReadiness(context.Background(), mgr, []configv1.VSpherePlatformFailureDomainSpec{{Name: "target"}}, "infra")
		if err != nil {
			t.Fatalf("checkWorkerReadiness: %v", err)
		}
		if got.Ready != (replicas <= 1) {
			t.Fatalf("replicas %d: Ready = %v", replicas, got.Ready)
		}
	}
}

func TestRecordedWorkerMachineSetsRequiredAfterSourceCleanup(t *testing.T) {
	source := newSourceFailureDomainForRollout()
	dest := configv1.VSpherePlatformFailureDomainSpec{Name: "target-fd-1", Server: "target.example.com"}
	destFDs := []configv1.VSpherePlatformFailureDomainSpec{dest}
	mappings := []migrationv1alpha1.FailureDomainMapping{{Source: source.Name, Destination: dest.Name}}
	base := workerMachineSetName("test-infra", dest.Name)
	recorded := []string{base, base + "-2"}

	tests := []struct {
		name         string
		objects      []runtime.Object
		recorded     []string
		wantExist    bool
		wantWorkerFD bool
	}{
		{
			name:         "recorded names still required when source cleaned up and targets deleted",
			objects:      []runtime.Object{newInfrastructureForRollout()},
			recorded:     recorded,
			wantExist:    false,
			wantWorkerFD: true,
		},
		{
			name: "recorded names satisfied when source cleaned up and targets exist",
			objects: []runtime.Object{
				newInfrastructureForRollout(),
				newTargetMachineSetForRollout(base, "target.example.com", 1),
				newTargetMachineSetForRollout(base+"-2", "target.example.com", 1),
			},
			recorded:     recorded,
			wantExist:    true,
			wantWorkerFD: true,
		},
		{
			name:         "originally empty mapping with nothing recorded stays empty",
			objects:      []runtime.Object{newInfrastructureForRollout()},
			recorded:     nil,
			wantExist:    true,
			wantWorkerFD: false,
		},
		{
			name: "missing recorded copy still fails existence check",
			objects: []runtime.Object{
				newInfrastructureForRollout(),
				newTargetMachineSetForRollout(base, "target.example.com", 1),
			},
			recorded:     recorded,
			wantExist:    false,
			wantWorkerFD: true,
		},
		{
			// Regression: when source MachineSets still exist, previously
			// recorded names must keep being required even if the current
			// source list would derive fewer names.
			name: "recorded names preserved when fewer source MachineSets remain",
			objects: []runtime.Object{
				newInfrastructureForRollout(),
				newSourceMachineSetForRollout(1),
				newTargetMachineSetForRollout(base, "target.example.com", 1),
			},
			recorded:     recorded,
			wantExist:    false,
			wantWorkerFD: true,
		},
		{
			name: "merged recorded and derived names satisfied",
			objects: []runtime.Object{
				newInfrastructureForRollout(),
				newSourceMachineSetForRollout(1),
				newTargetMachineSetForRollout(base, "target.example.com", 1),
				newTargetMachineSetForRollout(base+"-2", "target.example.com", 1),
			},
			recorded:     recorded,
			wantExist:    true,
			wantWorkerFD: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := newRolloutReconciler(t, tt.objects)
			mgr := openshift.NewMachineManager(r.KubeClient, r.MachineClient, r.DynamicClient)
			exist, err := targetMachineSetsExist(context.Background(), mgr, "test-infra", mappings, []configv1.VSpherePlatformFailureDomainSpec{source}, destFDs, tt.recorded)
			if err != nil {
				t.Fatalf("targetMachineSetsExist: %v", err)
			}
			if exist != tt.wantExist {
				t.Fatalf("targetMachineSetsExist = %v, want %v", exist, tt.wantExist)
			}
			workerFDs, err := workerReadinessFailureDomains(context.Background(), mgr, "test-infra", mappings, []configv1.VSpherePlatformFailureDomainSpec{source}, destFDs, tt.recorded)
			if err != nil {
				t.Fatalf("workerReadinessFailureDomains: %v", err)
			}
			if (len(workerFDs) > 0) != tt.wantWorkerFD {
				t.Fatalf("workerReadinessFailureDomains = %v, want contains %v", workerFDs, tt.wantWorkerFD)
			}
		})
	}
}
