/*
Copyright 2026.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package controller

import (
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	configv1 "github.com/openshift/api/config/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"

	migrationv1alpha1 "github.com/openshift/vcf-migration-operator/api/v1alpha1"
)

// newMappingResource builds a migration with two destination failure domains
// (dst-a, dst-b) and raw failureDomainMappings expressed at the wire level, so
// schema markers (MinItems, listMapKey, XValidation) are exercised even for
// values Go's omitempty would drop. A nil mappings argument omits the key.
func newMappingResource(generateName string, mappings []interface{}) *unstructured.Unstructured {
	resource := &migrationv1alpha1.VmwareCloudFoundationMigration{
		ObjectMeta: metav1.ObjectMeta{GenerateName: generateName, Namespace: "default"},
		Spec: migrationv1alpha1.VmwareCloudFoundationMigrationSpec{
			TargetVCenterCredentialsSecret: migrationv1alpha1.SecretReference{Name: "target-vcenter-creds"},
			FailureDomains: []configv1.VSpherePlatformFailureDomainSpec{
				{
					Name: "dst-a", Region: "region-a", Zone: "zone-a", Server: "vcenter.example.com",
					Topology: configv1.VSpherePlatformTopology{Datacenter: "DC1", ComputeCluster: "/DC1/host/Cluster1", Datastore: "/DC1/datastore/DS1", Networks: []string{"VM Network"}, Template: "/DC1/vm/template-a"},
				},
				{
					Name: "dst-b", Region: "region-b", Zone: "zone-b", Server: "vcenter.example.com",
					Topology: configv1.VSpherePlatformTopology{Datacenter: "DC1", ComputeCluster: "/DC1/host/Cluster1", Datastore: "/DC1/datastore/DS1", Networks: []string{"VM Network"}, Template: "/DC1/vm/template-b"},
				},
			},
		},
	}
	obj, err := runtime.DefaultUnstructuredConverter.ToUnstructured(resource)
	ExpectWithOffset(1, err).NotTo(HaveOccurred())
	if mappings != nil {
		obj["spec"].(map[string]interface{})["failureDomainMappings"] = mappings
	}
	unstructuredResource := &unstructured.Unstructured{Object: obj}
	unstructuredResource.SetAPIVersion("migration.openshift.io/v1alpha1")
	unstructuredResource.SetKind("VmwareCloudFoundationMigration")
	return unstructuredResource
}

var _ = Describe("failure domain mapping spec validation", func() {
	DescribeTable("creation is validated against mapping schema markers",
		func(generateName string, mappings []interface{}, wantErr string) {
			resource := newMappingResource(generateName, mappings)
			if wantErr == "" {
				Expect(k8sClient.Create(ctx, resource)).To(Succeed())
				DeferCleanup(func() { Expect(k8sClient.Delete(ctx, resource)).To(Succeed()) })
			} else {
				Expect(k8sClient.Create(ctx, resource)).To(MatchError(ContainSubstring(wantErr)))
			}
		},
		Entry("rejects a mapping list with duplicate destinations",
			"duplicate-destinations-", []interface{}{
				map[string]interface{}{"source": "src-a", "destination": "dst-a"},
				map[string]interface{}{"source": "src-b", "destination": "dst-a"},
			}, "destination must be unique"),
		Entry("rejects an empty mappings list",
			"empty-mappings-", []interface{}{}, "should have at least 1 items"),
		Entry("rejects duplicate source names",
			"duplicate-sources-", []interface{}{
				map[string]interface{}{"source": "src-a", "destination": "dst-a"},
				map[string]interface{}{"source": "src-a", "destination": "dst-b"},
			}, "Duplicate value"),
		Entry("accepts a valid mapping list",
			"valid-mappings-", []interface{}{
				map[string]interface{}{"source": "src-a", "destination": "dst-a"},
				map[string]interface{}{"source": "src-b", "destination": "dst-b"},
			}, ""),
		Entry("accepts omission of the field",
			"omitted-mappings-", nil, ""),
	)
})
