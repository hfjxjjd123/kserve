/*
Copyright 2025 The KServe Authors.

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

package configcache

import (
	"testing"

	"github.com/kserve/kserve/pkg/apis/serving/v1beta1"
	"github.com/kserve/kserve/pkg/credentials"
	"github.com/kserve/kserve/pkg/credentials/gcs"
	"github.com/kserve/kserve/pkg/credentials/s3"
	"github.com/kserve/kserve/pkg/types"
)

// TestDeepCopyIsolation verifies that modifying returned configs doesn't affect the original.
func TestDeepCopyIsolation(t *testing.T) {
	tests := []struct {
		name     string
		testFunc func(t *testing.T)
	}{
		{
			name: "IngressConfig isolation",
			testFunc: func(t *testing.T) {
				className := "nginx"
				domains := []string{"example.com", "test.com"}
				src := &v1beta1.IngressConfig{
					IngressClassName:         &className,
					AdditionalIngressDomains: &domains,
					IngressDomain:            "default.com",
				}

				dst := src.DeepCopy()

				// Modify the copy
				*dst.IngressClassName = "traefik"
				(*dst.AdditionalIngressDomains)[0] = "modified.com"
				dst.IngressDomain = "modified-default.com"

				// Verify original is unchanged
				if *src.IngressClassName != "nginx" {
					t.Errorf("Original IngressClassName was modified: got %s, want nginx", *src.IngressClassName)
				}
				if (*src.AdditionalIngressDomains)[0] != "example.com" {
					t.Errorf("Original AdditionalIngressDomains was modified: got %s, want example.com", (*src.AdditionalIngressDomains)[0])
				}
				if src.IngressDomain != "default.com" {
					t.Errorf("Original IngressDomain was modified: got %s, want default.com", src.IngressDomain)
				}
			},
		},
		{
			name: "DeployConfig isolation",
			testFunc: func(t *testing.T) {
				defaultRollout := v1beta1.RolloutSpec{MaxSurge: "25%", MaxUnavailable: "25%"}
				src := &v1beta1.DeployConfig{
					DefaultDeploymentMode: "Serverless",
					DeploymentRolloutStrategy: &v1beta1.DeploymentRolloutStrategy{
						DefaultRollout: &defaultRollout,
					},
				}

				dst := src.DeepCopy()

				// Modify the copy
				dst.DefaultDeploymentMode = "RawDeployment"
				dst.DeploymentRolloutStrategy.DefaultRollout.MaxSurge = "50%"

				// Verify original is unchanged
				if src.DefaultDeploymentMode != "Serverless" {
					t.Errorf("Original DefaultDeploymentMode was modified: got %s, want Serverless", src.DefaultDeploymentMode)
				}
				if src.DeploymentRolloutStrategy.DefaultRollout.MaxSurge != "25%" {
					t.Errorf("Original DefaultRollout was modified: got %s, want 25%%", src.DeploymentRolloutStrategy.DefaultRollout.MaxSurge)
				}
			},
		},
		{
			name: "InferenceServicesConfig isolation",
			testFunc: func(t *testing.T) {
				src := &v1beta1.InferenceServicesConfig{
					ServiceAnnotationDisallowedList: []string{"annotation1", "annotation2"},
					ServiceLabelDisallowedList:      []string{"label1", "label2"},
				}

				dst := src.DeepCopy()

				// Modify the copy
				dst.ServiceAnnotationDisallowedList[0] = "modified"
				dst.ServiceLabelDisallowedList[0] = "modified"

				// Verify original is unchanged
				if src.ServiceAnnotationDisallowedList[0] != "annotation1" {
					t.Errorf("Original ServiceAnnotationDisallowedList was modified: got %s, want annotation1", src.ServiceAnnotationDisallowedList[0])
				}
				if src.ServiceLabelDisallowedList[0] != "label1" {
					t.Errorf("Original ServiceLabelDisallowedList was modified: got %s, want label1", src.ServiceLabelDisallowedList[0])
				}
			},
		},
		{
			name: "StorageInitializerConfig isolation",
			testFunc: func(t *testing.T) {
				uid := int64(1000)
				src := &types.StorageInitializerConfig{
					Image:         "kserve/storage-initializer:latest",
					CpuRequest:    "100m",
					MemoryRequest: "200Mi",
					UidModelcar:   &uid,
				}

				dst := src.DeepCopy()

				// Modify the copy
				dst.Image = "modified:latest"
				*dst.UidModelcar = 2000

				// Verify original is unchanged
				if src.Image != "kserve/storage-initializer:latest" {
					t.Errorf("Original Image was modified: got %s, want kserve/storage-initializer:latest", src.Image)
				}
				if *src.UidModelcar != 1000 {
					t.Errorf("Original UidModelcar was modified: got %d, want 1000", *src.UidModelcar)
				}
			},
		},
		{
			name: "CredentialConfig isolation with nested structs",
			testFunc: func(t *testing.T) {
				src := &credentials.CredentialConfig{
					S3: s3.S3Config{
						S3AccessKeyIDName:     "access-key",
						S3SecretAccessKeyName: "secret-key",
						S3Endpoint:            "https://s3.amazonaws.com",
					},
					GCS: gcs.GCSConfig{
						GCSCredentialFileName: "gcs-creds.json",
					},
					StorageSpecSecretName: "storage-secret",
				}

				dst := src.DeepCopy()

				// Modify the copy
				dst.S3.S3AccessKeyIDName = "modified-access-key"
				dst.GCS.GCSCredentialFileName = "modified-gcs.json"
				dst.StorageSpecSecretName = "modified-secret"

				// Verify original is unchanged
				if src.S3.S3AccessKeyIDName != "access-key" {
					t.Errorf("Original S3.S3AccessKeyIDName was modified: got %s, want access-key", src.S3.S3AccessKeyIDName)
				}
				if src.GCS.GCSCredentialFileName != "gcs-creds.json" {
					t.Errorf("Original GCS.GCSCredentialFileName was modified: got %s, want gcs-creds.json", src.GCS.GCSCredentialFileName)
				}
				if src.StorageSpecSecretName != "storage-secret" {
					t.Errorf("Original StorageSpecSecretName was modified: got %s, want storage-secret", src.StorageSpecSecretName)
				}
			},
		},
		{
			name: "LocalModelConfig isolation",
			testFunc: func(t *testing.T) {
				fsGroup := int64(1000)
				ttl := int32(3600)
				freq := int64(60)
				src := &v1beta1.LocalModelConfig{
					FSGroup:                      &fsGroup,
					JobTTLSecondsAfterFinished:   &ttl,
					ReconcilationFrequencyInSecs: &freq,
				}

				dst := src.DeepCopy()

				// Modify the copy
				*dst.FSGroup = 2000
				*dst.JobTTLSecondsAfterFinished = 7200
				*dst.ReconcilationFrequencyInSecs = 120

				// Verify original is unchanged
				if *src.FSGroup != 1000 {
					t.Errorf("Original FSGroup was modified: got %d, want 1000", *src.FSGroup)
				}
				if *src.JobTTLSecondsAfterFinished != 3600 {
					t.Errorf("Original JobTTLSecondsAfterFinished was modified: got %d, want 3600", *src.JobTTLSecondsAfterFinished)
				}
				if *src.ReconcilationFrequencyInSecs != 60 {
					t.Errorf("Original ReconcilationFrequencyInSecs was modified: got %d, want 60", *src.ReconcilationFrequencyInSecs)
				}
			},
		},
		{
			name: "SecurityConfig isolation",
			testFunc: func(t *testing.T) {
				src := &v1beta1.SecurityConfig{
					AutoMountServiceAccountToken: true,
				}

				dst := src.DeepCopy()

				// Modify the copy
				dst.AutoMountServiceAccountToken = false

				// Verify original is unchanged
				if src.AutoMountServiceAccountToken != true {
					t.Errorf("Original AutoMountServiceAccountToken was modified: got %v, want true", src.AutoMountServiceAccountToken)
				}
			},
		},
		{
			name: "ServiceConfig isolation",
			testFunc: func(t *testing.T) {
				src := &v1beta1.ServiceConfig{
					ServiceClusterIPNone: true,
				}

				dst := src.DeepCopy()

				// Modify the copy
				dst.ServiceClusterIPNone = false

				// Verify original is unchanged
				if src.ServiceClusterIPNone != true {
					t.Errorf("Original ServiceClusterIPNone was modified: got %v, want true", src.ServiceClusterIPNone)
				}
			},
		},
		{
			name: "MultiNodeConfig isolation",
			testFunc: func(t *testing.T) {
				src := &v1beta1.MultiNodeConfig{
					CustomGPUResourceTypeList: []string{"nvidia.com/gpu", "amd.com/gpu"},
				}

				dst := src.DeepCopy()

				// Modify the copy
				dst.CustomGPUResourceTypeList[0] = "modified.com/gpu"

				// Verify original is unchanged
				if src.CustomGPUResourceTypeList[0] != "nvidia.com/gpu" {
					t.Errorf("Original CustomGPUResourceTypeList was modified: got %s, want nvidia.com/gpu", src.CustomGPUResourceTypeList[0])
				}
			},
		},
		{
			name: "OtelCollectorConfig isolation",
			testFunc: func(t *testing.T) {
				src := &v1beta1.OtelCollectorConfig{
					ScrapeInterval:         "30s",
					MetricReceiverEndpoint: "http://receiver:8889",
					Resource: v1beta1.ResourceConfig{
						CPULimit:    "1",
						MemoryLimit: "1Gi",
					},
				}

				dst := src.DeepCopy()

				// Modify the copy
				dst.ScrapeInterval = "60s"
				dst.Resource.CPULimit = "2"

				// Verify original is unchanged
				if src.ScrapeInterval != "30s" {
					t.Errorf("Original ScrapeInterval was modified: got %s, want 30s", src.ScrapeInterval)
				}
				if src.Resource.CPULimit != "1" {
					t.Errorf("Original Resource.CPULimit was modified: got %s, want 1", src.Resource.CPULimit)
				}
			},
		},
		{
			name: "AutoscalerConfig isolation",
			testFunc: func(t *testing.T) {
				src := &v1beta1.AutoscalerConfig{
					ScaleUpStabilizationWindowSeconds:   "60",
					ScaleDownStabilizationWindowSeconds: "300",
				}

				dst := src.DeepCopy()

				// Modify the copy
				dst.ScaleUpStabilizationWindowSeconds = "120"

				// Verify original is unchanged
				if src.ScaleUpStabilizationWindowSeconds != "60" {
					t.Errorf("Original ScaleUpStabilizationWindowSeconds was modified: got %s, want 60", src.ScaleUpStabilizationWindowSeconds)
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.testFunc(t)
		})
	}
}

// TestDeepCopyNilHandling verifies that deep copy functions handle nil inputs correctly
func TestDeepCopyNilHandling(t *testing.T) {
	t.Run("IngressConfig nil handling", func(t *testing.T) {
		var src *v1beta1.IngressConfig
		if result := src.DeepCopy(); result != nil {
			t.Errorf("expected nil, got %v", result)
		}
	})
	t.Run("DeployConfig nil handling", func(t *testing.T) {
		var src *v1beta1.DeployConfig
		if result := src.DeepCopy(); result != nil {
			t.Errorf("expected nil, got %v", result)
		}
	})
	t.Run("InferenceServicesConfig nil handling", func(t *testing.T) {
		var src *v1beta1.InferenceServicesConfig
		if result := src.DeepCopy(); result != nil {
			t.Errorf("expected nil, got %v", result)
		}
	})
	t.Run("StorageInitializerConfig nil handling", func(t *testing.T) {
		var src *types.StorageInitializerConfig
		if result := src.DeepCopy(); result != nil {
			t.Errorf("expected nil, got %v", result)
		}
	})
	t.Run("CredentialConfig nil handling", func(t *testing.T) {
		var src *credentials.CredentialConfig
		if result := src.DeepCopy(); result != nil {
			t.Errorf("expected nil, got %v", result)
		}
	})
	t.Run("LocalModelConfig nil handling", func(t *testing.T) {
		var src *v1beta1.LocalModelConfig
		if result := src.DeepCopy(); result != nil {
			t.Errorf("expected nil, got %v", result)
		}
	})
	t.Run("SecurityConfig nil handling", func(t *testing.T) {
		var src *v1beta1.SecurityConfig
		if result := src.DeepCopy(); result != nil {
			t.Errorf("expected nil, got %v", result)
		}
	})
	t.Run("ServiceConfig nil handling", func(t *testing.T) {
		var src *v1beta1.ServiceConfig
		if result := src.DeepCopy(); result != nil {
			t.Errorf("expected nil, got %v", result)
		}
	})
	t.Run("MultiNodeConfig nil handling", func(t *testing.T) {
		var src *v1beta1.MultiNodeConfig
		if result := src.DeepCopy(); result != nil {
			t.Errorf("expected nil, got %v", result)
		}
	})
	t.Run("OtelCollectorConfig nil handling", func(t *testing.T) {
		var src *v1beta1.OtelCollectorConfig
		if result := src.DeepCopy(); result != nil {
			t.Errorf("expected nil, got %v", result)
		}
	})
	t.Run("AutoscalerConfig nil handling", func(t *testing.T) {
		var src *v1beta1.AutoscalerConfig
		if result := src.DeepCopy(); result != nil {
			t.Errorf("expected nil, got %v", result)
		}
	})
}

// TestShallowCopyRegression verifies that deep copy functions return new pointers, not shallow copies.
func TestShallowCopyRegression(t *testing.T) {
	t.Run("SecurityConfig is not shallow copied", func(t *testing.T) {
		src := &v1beta1.SecurityConfig{
			AutoMountServiceAccountToken: true,
		}

		dst := src.DeepCopy()

		if dst == src {
			t.Errorf("deep copy failed:DeepCopy returned same pointer (shallow copy)!")
		}

		dst.AutoMountServiceAccountToken = false
		if src.AutoMountServiceAccountToken != true {
			t.Errorf("SecurityConfig isolation failed")
		}
	})

	t.Run("ServiceConfig is not shallow copied", func(t *testing.T) {
		src := &v1beta1.ServiceConfig{
			ServiceClusterIPNone: true,
		}

		dst := src.DeepCopy()

		if dst == src {
			t.Errorf("deep copy failed:DeepCopy returned same pointer (shallow copy)!")
		}

		dst.ServiceClusterIPNone = false
		if src.ServiceClusterIPNone != true {
			t.Errorf("ServiceConfig isolation failed")
		}
	})

	t.Run("CredentialConfig is not shallow copied", func(t *testing.T) {
		src := &credentials.CredentialConfig{
			S3: s3.S3Config{
				S3AccessKeyIDName: "original-key",
			},
		}

		dst := src.DeepCopy()

		if dst == src {
			t.Errorf("deep copy failed:DeepCopy returned same pointer (shallow copy)!")
		}

		// Critical: verify nested struct is also deep copied
		dst.S3.S3AccessKeyIDName = "modified-key"
		if src.S3.S3AccessKeyIDName != "original-key" {
			t.Errorf("deep copy failed: nested S3Config was shallow copied, original was modified")
		}
	})
}

// TestDeepCopyMethodVerification verifies the DeepCopy methods work correctly.
func TestDeepCopyMethodVerification(t *testing.T) {
	t.Run("OtelCollectorConfig isolation", func(t *testing.T) {
		src := &v1beta1.OtelCollectorConfig{
			ScrapeInterval: "30s",
			Resource: v1beta1.ResourceConfig{
				CPULimit: "1",
			},
		}

		dst := src.DeepCopy()

		dst.Resource.CPULimit = "2"
		if src.Resource.CPULimit != "1" {
			t.Errorf("deep copy failed to isolate nested struct")
		}

		if dst == src {
			t.Errorf("DeepCopy returned same pointer")
		}
	})

	t.Run("AutoscalerConfig isolation", func(t *testing.T) {
		src := &v1beta1.AutoscalerConfig{
			ScaleUpStabilizationWindowSeconds: "60",
		}

		dst := src.DeepCopy()

		dst.ScaleUpStabilizationWindowSeconds = "120"
		if src.ScaleUpStabilizationWindowSeconds != "60" {
			t.Errorf("deep copy failed")
		}

		if dst == src {
			t.Errorf("DeepCopy returned same pointer")
		}
	})
}
