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
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/event"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	"github.com/kserve/kserve/pkg/apis/serving/v1beta1"
	"github.com/kserve/kserve/pkg/credentials"
	"github.com/kserve/kserve/pkg/types"
)

var log = logf.Log.WithName("ConfigCache")

// Options for creating a ConfigCache
type Options struct {
	ConfigMapName      string
	ConfigMapNamespace string
}

// ConfigCache provides thread-safe access to parsed ConfigMap configurations
// with watch-based invalidation for zero API calls during normal operation.
type ConfigCache interface {
	// GetIngressConfig returns the parsed ingress configuration
	GetIngressConfig() (*v1beta1.IngressConfig, error)

	// GetDeployConfig returns the parsed deployment configuration
	GetDeployConfig() (*v1beta1.DeployConfig, error)

	// GetInferenceServicesConfig returns the parsed inference services configuration
	GetInferenceServicesConfig() (*v1beta1.InferenceServicesConfig, error)

	// GetStorageInitializerConfig returns the parsed storage initializer configuration
	GetStorageInitializerConfig() (*types.StorageInitializerConfig, error)

	// GetCredentialConfig returns the parsed credential configuration
	GetCredentialConfig() (*credentials.CredentialConfig, error)

	// GetLocalModelConfig returns the parsed local model configuration
	GetLocalModelConfig() (*v1beta1.LocalModelConfig, error)

	// GetSecurityConfig returns the parsed security configuration
	GetSecurityConfig() (*v1beta1.SecurityConfig, error)

	// GetServiceConfig returns the parsed service configuration
	GetServiceConfig() (*v1beta1.ServiceConfig, error)

	// GetMultiNodeConfig returns the parsed multi-node configuration
	GetMultiNodeConfig() (*v1beta1.MultiNodeConfig, error)

	// GetOtelCollectorConfig returns the parsed OpenTelemetry collector configuration
	GetOtelCollectorConfig() (*v1beta1.OtelCollectorConfig, error)

	// GetAutoscalerConfig returns the parsed autoscaler configuration
	GetAutoscalerConfig() (*v1beta1.AutoscalerConfig, error)

	// Get returns the raw ConfigMap for cases where controllers need direct access
	Get(ctx context.Context) (*corev1.ConfigMap, error)

	// WaitForCacheSync blocks until the cache has been initialized
	WaitForCacheSync(ctx context.Context) error

	// Start initializes the cache by loading the ConfigMap
	Start(ctx context.Context) error
}

// cacheImpl is the concrete implementation of ConfigCache
type cacheImpl struct {
	// reader is used for direct API reads (non-cached) during startup
	// This ensures the cache can initialize before the manager's cache starts
	reader             client.Reader
	configMapName      string
	configMapNamespace string

	// Mutex for thread-safe access to cached configs
	mu sync.RWMutex

	// Raw ConfigMap for controllers that need direct access
	configMap *corev1.ConfigMap

	// Pre-parsed configurations (single parse, many reads)
	ingressConfig              *v1beta1.IngressConfig
	deployConfig               *v1beta1.DeployConfig
	inferenceServicesConfig    *v1beta1.InferenceServicesConfig
	storageInitializerConfig   *types.StorageInitializerConfig
	credentialConfig           *credentials.CredentialConfig
	localModelConfig           *v1beta1.LocalModelConfig
	securityConfig             *v1beta1.SecurityConfig
	serviceConfig              *v1beta1.ServiceConfig
	multiNodeConfig            *v1beta1.MultiNodeConfig
	otelCollectorConfig        *v1beta1.OtelCollectorConfig
	autoscalerConfig           *v1beta1.AutoscalerConfig

	// Synchronization for initialization
	initialized bool
	initCond    *sync.Cond
}

// NewConfigCache creates a new ConfigCache instance
// The reader parameter should be mgr.GetAPIReader() to ensure it works before the manager starts.
// Using mgr.GetClient() will cause startup failures as the manager's cache is not yet running.
func NewConfigCache(reader client.Reader, opts Options) ConfigCache {
	c := &cacheImpl{
		reader:             reader,
		configMapName:      opts.ConfigMapName,
		configMapNamespace: opts.ConfigMapNamespace,
	}
	c.initCond = sync.NewCond(&c.mu)
	return c
}

// Start initializes the cache by loading the ConfigMap
func (c *cacheImpl) Start(ctx context.Context) error {
    log.Info("Starting ConfigMap cache (Non-blocking mode)", "configMapName", c.configMapName)

    go func() {
        backoff := 3 * time.Second
        for {
            select {
            case <-ctx.Done():
                return
            default:
                if err := c.loadConfigMap(ctx); err != nil {
                    log.Error(err, "ConfigMap not found or not defined yet", 
                        "configMapName", c.configMapName, "retryIn", backoff)
                    
                    time.Sleep(backoff)
                    continue
                }

                c.mu.Lock()
                c.initialized = true
                c.initCond.Broadcast()
                c.mu.Unlock()

                log.Info("ConfigMap cache initialized successfully")
                return
            }
        }
    }()

    return nil
}

// WaitForCacheSync blocks until the cache has been initialized
func (c *cacheImpl) WaitForCacheSync(ctx context.Context) error {
	done := make(chan struct{})

	go func() {
		c.mu.Lock()
		for !c.initialized {
			c.initCond.Wait()
		}
		c.mu.Unlock()
		close(done)
	}()

	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return fmt.Errorf("timeout waiting for cache sync: %w", ctx.Err())
	}
}

// loadConfigMap fetches the ConfigMap from the API server and parses all configs atomically
// Uses the non-cached reader to ensure this works before the manager starts
func (c *cacheImpl) loadConfigMap(ctx context.Context) error {
	configMap := &corev1.ConfigMap{}
	key := client.ObjectKey{
		Name:      c.configMapName,
		Namespace: c.configMapNamespace,
	}

	if err := c.reader.Get(ctx, key, configMap); err != nil {
		return fmt.Errorf("failed to get ConfigMap: %w", err)
	}

	return c.parseAndUpdateConfigs(ctx, configMap)
}

// parseAndUpdateConfigs parses all configurations from the ConfigMap and updates the cache atomically
func (c *cacheImpl) parseAndUpdateConfigs(ctx context.Context, configMap *corev1.ConfigMap) error {
	// Parse all configs outside the lock
	ingressConfig, err := v1beta1.NewIngressConfig(configMap)
	if err != nil {
		return fmt.Errorf("failed to parse ingress config: %w", err)
	}

	deployConfig, err := v1beta1.NewDeployConfig(configMap)
	if err != nil {
		return fmt.Errorf("failed to parse deploy config: %w", err)
	}

	inferenceServicesConfig, err := v1beta1.NewInferenceServicesConfig(configMap)
	if err != nil {
		return fmt.Errorf("failed to parse inference services config: %w", err)
	}

	storageInitializerConfig, err := v1beta1.GetStorageInitializerConfigs(configMap)
	if err != nil {
		return fmt.Errorf("failed to parse storage initializer config: %w", err)
	}

	credentialConfig, err := credentials.GetCredentialConfig(configMap)
	if err != nil {
		return fmt.Errorf("failed to parse credential config: %w", err)
	}

	localModelConfig, err := v1beta1.NewLocalModelConfig(configMap)
	if err != nil {
		return fmt.Errorf("failed to parse local model config: %w", err)
	}

	securityConfig, err := v1beta1.NewSecurityConfig(configMap)
	if err != nil {
		return fmt.Errorf("failed to parse security config: %w", err)
	}

	serviceConfig, err := v1beta1.NewServiceConfig(configMap)
	if err != nil {
		return fmt.Errorf("failed to parse service config: %w", err)
	}

	multiNodeConfig, err := v1beta1.NewMultiNodeConfig(configMap)
	if err != nil {
		return fmt.Errorf("failed to parse multi-node config: %w", err)
	}

	otelCollectorConfig, err := v1beta1.NewOtelCollectorConfig(configMap)
	if err != nil {
		return fmt.Errorf("failed to parse otel collector config: %w", err)
	}

	autoscalerConfig, err := v1beta1.NewAutoscalerConfig(configMap)
	if err != nil {
		return fmt.Errorf("failed to parse autoscaler config: %w", err)
	}

	// Update all configs atomically with write lock
	c.mu.Lock()
	defer c.mu.Unlock()

	c.configMap = configMap
	c.ingressConfig = ingressConfig
	c.deployConfig = deployConfig
	c.inferenceServicesConfig = inferenceServicesConfig
	c.storageInitializerConfig = storageInitializerConfig
	c.credentialConfig = &credentialConfig
	c.localModelConfig = localModelConfig
	c.securityConfig = securityConfig
	c.serviceConfig = serviceConfig
	c.multiNodeConfig = multiNodeConfig
	c.otelCollectorConfig = otelCollectorConfig
	c.autoscalerConfig = autoscalerConfig

	log.Info("ConfigMap configs updated successfully")
	return nil
}

// handleConfigMapUpdate is called when the ConfigMap is updated via watch
func (c *cacheImpl) handleConfigMapUpdate(ctx context.Context, cm *corev1.ConfigMap) {
	log.Info("ConfigMap update detected, refreshing cache", "configMapName", cm.Name)

	if err := c.parseAndUpdateConfigs(ctx, cm); err != nil {
		log.Error(err, "Failed to update cache after ConfigMap change")
		// Don't fail here - keep using old cached values
	}
}

// Deep copy helpers to prevent accidental mutations of cached configs
//
// DEEP COPY STRATEGY:
// 1. Use generated DeepCopy() methods where available (AutoscalerConfig, OtelCollectorConfig)
// 2. Use JSON round-trip for configs without generated methods (safe, handles future field additions)
// 3. Manual deep copy for performance-critical configs with known structure
//
// WARNING: JSON round-trip adds ~10-50µs overhead but provides guaranteed safety against
// future struct changes (pointer/slice/map field additions). This is acceptable since
// config reads happen from cache, not on every reconciliation.

// jsonDeepCopy performs a deep copy using JSON marshaling/unmarshaling.
// This is safer than manual copying because it handles:
// - Pointers, slices, maps automatically
// - Nested structs recursively
// - Future field additions without code changes
func jsonDeepCopy(src interface{}, dst interface{}) error {
	if src == nil {
		return fmt.Errorf("source is nil")
	}
	data, err := json.Marshal(src)
	if err != nil {
		return fmt.Errorf("failed to marshal: %w", err)
	}
	if err := json.Unmarshal(data, dst); err != nil {
		return fmt.Errorf("failed to unmarshal: %w", err)
	}
	return nil
}

func deepCopyIngressConfig(src *v1beta1.IngressConfig) *v1beta1.IngressConfig {
	if src == nil {
		return nil
	}
	dst := *src
	// Deep copy pointer to string
	if src.IngressClassName != nil {
		className := *src.IngressClassName
		dst.IngressClassName = &className
	}
	// Deep copy pointer to slice - must handle all cases to avoid shared pointers
	if src.AdditionalIngressDomains != nil {
		if *src.AdditionalIngressDomains != nil {
			domains := make([]string, len(*src.AdditionalIngressDomains))
			copy(domains, *src.AdditionalIngressDomains)
			dst.AdditionalIngressDomains = &domains
		} else {
			// Pointer points to nil slice - create new pointer to nil slice
			nilSlice := []string(nil)
			dst.AdditionalIngressDomains = &nilSlice
		}
	}
	return &dst
}

func deepCopyDeployConfig(src *v1beta1.DeployConfig) *v1beta1.DeployConfig {
	if src == nil {
		return nil
	}
	dst := *src
	if src.DeploymentRolloutStrategy != nil {
		strategy := *src.DeploymentRolloutStrategy
		if src.DeploymentRolloutStrategy.DefaultRollout != nil {
			rollout := *src.DeploymentRolloutStrategy.DefaultRollout
			strategy.DefaultRollout = &rollout
		}
		dst.DeploymentRolloutStrategy = &strategy
	}
	return &dst
}

func deepCopyInferenceServicesConfig(src *v1beta1.InferenceServicesConfig) *v1beta1.InferenceServicesConfig {
	if src == nil {
		return nil
	}
	dst := *src
	if src.ServiceAnnotationDisallowedList != nil {
		dst.ServiceAnnotationDisallowedList = make([]string, len(src.ServiceAnnotationDisallowedList))
		copy(dst.ServiceAnnotationDisallowedList, src.ServiceAnnotationDisallowedList)
	}
	if src.ServiceLabelDisallowedList != nil {
		dst.ServiceLabelDisallowedList = make([]string, len(src.ServiceLabelDisallowedList))
		copy(dst.ServiceLabelDisallowedList, src.ServiceLabelDisallowedList)
	}
	return &dst
}

func deepCopyStorageInitializerConfig(src *types.StorageInitializerConfig) *types.StorageInitializerConfig {
	if src == nil {
		return nil
	}
	dst := *src
	if src.UidModelcar != nil {
		uid := *src.UidModelcar
		dst.UidModelcar = &uid
	}
	return &dst
}

func deepCopyCredentialConfig(src *credentials.CredentialConfig) *credentials.CredentialConfig {
	if src == nil {
		return nil
	}
	// Use JSON deep copy for safety - CredentialConfig contains nested S3Config and GCSConfig
	// structs which are currently all strings but may gain pointer/slice fields in the future
	dst := &credentials.CredentialConfig{}
	if err := jsonDeepCopy(src, dst); err != nil {
		log.Error(err, "Failed to deep copy CredentialConfig, returning shallow copy as fallback")
		fallback := *src
		return &fallback
	}
	return dst
}

func deepCopyLocalModelConfig(src *v1beta1.LocalModelConfig) *v1beta1.LocalModelConfig {
	if src == nil {
		return nil
	}
	dst := *src
	if src.FSGroup != nil {
		fsGroup := *src.FSGroup
		dst.FSGroup = &fsGroup
	}
	if src.JobTTLSecondsAfterFinished != nil {
		ttl := *src.JobTTLSecondsAfterFinished
		dst.JobTTLSecondsAfterFinished = &ttl
	}
	if src.ReconcilationFrequencyInSecs != nil {
		freq := *src.ReconcilationFrequencyInSecs
		dst.ReconcilationFrequencyInSecs = &freq
	}
	return &dst
}

func deepCopySecurityConfig(src *v1beta1.SecurityConfig) *v1beta1.SecurityConfig {
	if src == nil {
		return nil
	}
	// Use JSON deep copy for future-proofing - currently only has bool field,
	// but may gain pointer/slice/map fields in the future
	dst := &v1beta1.SecurityConfig{}
	if err := jsonDeepCopy(src, dst); err != nil {
		log.Error(err, "Failed to deep copy SecurityConfig, returning shallow copy as fallback")
		fallback := *src
		return &fallback
	}
	return dst
}

func deepCopyServiceConfig(src *v1beta1.ServiceConfig) *v1beta1.ServiceConfig {
	if src == nil {
		return nil
	}
	// Use JSON deep copy for future-proofing - currently only has bool field,
	// but may gain pointer/slice/map fields in the future
	dst := &v1beta1.ServiceConfig{}
	if err := jsonDeepCopy(src, dst); err != nil {
		log.Error(err, "Failed to deep copy ServiceConfig, returning shallow copy as fallback")
		fallback := *src
		return &fallback
	}
	return dst
}

func deepCopyMultiNodeConfig(src *v1beta1.MultiNodeConfig) *v1beta1.MultiNodeConfig {
	if src == nil {
		return nil
	}
	dst := *src
	if src.CustomGPUResourceTypeList != nil {
		dst.CustomGPUResourceTypeList = make([]string, len(src.CustomGPUResourceTypeList))
		copy(dst.CustomGPUResourceTypeList, src.CustomGPUResourceTypeList)
	}
	return &dst
}

func deepCopyOtelCollectorConfig(src *v1beta1.OtelCollectorConfig) *v1beta1.OtelCollectorConfig {
	if src == nil {
		return nil
	}
	// Use generated DeepCopy() method - OtelCollectorConfig has autogenerated deep copy
	// via kubebuilder code generation in zz_generated.deepcopy.go
	return src.DeepCopy()
}

func deepCopyAutoscalerConfig(src *v1beta1.AutoscalerConfig) *v1beta1.AutoscalerConfig {
	if src == nil {
		return nil
	}
	// Use generated DeepCopy() method - AutoscalerConfig has autogenerated deep copy
	// via kubebuilder code generation in zz_generated.deepcopy.go
	return src.DeepCopy()
}

// Thread-safe getter methods with read locks
// All getters return deep copies to prevent accidental mutations

func (c *cacheImpl) GetIngressConfig() (*v1beta1.IngressConfig, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if !c.initialized {
		return nil, fmt.Errorf("cache not initialized")
	}
	return deepCopyIngressConfig(c.ingressConfig), nil
}

func (c *cacheImpl) GetDeployConfig() (*v1beta1.DeployConfig, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if !c.initialized {
		return nil, fmt.Errorf("cache not initialized")
	}
	return deepCopyDeployConfig(c.deployConfig), nil
}

func (c *cacheImpl) GetInferenceServicesConfig() (*v1beta1.InferenceServicesConfig, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if !c.initialized {
		return nil, fmt.Errorf("cache not initialized")
	}
	return deepCopyInferenceServicesConfig(c.inferenceServicesConfig), nil
}

func (c *cacheImpl) GetStorageInitializerConfig() (*types.StorageInitializerConfig, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if !c.initialized {
		return nil, fmt.Errorf("cache not initialized")
	}
	return deepCopyStorageInitializerConfig(c.storageInitializerConfig), nil
}

func (c *cacheImpl) GetCredentialConfig() (*credentials.CredentialConfig, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if !c.initialized {
		return nil, fmt.Errorf("cache not initialized")
	}
	return deepCopyCredentialConfig(c.credentialConfig), nil
}

func (c *cacheImpl) GetLocalModelConfig() (*v1beta1.LocalModelConfig, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if !c.initialized {
		return nil, fmt.Errorf("cache not initialized")
	}
	return deepCopyLocalModelConfig(c.localModelConfig), nil
}

func (c *cacheImpl) GetSecurityConfig() (*v1beta1.SecurityConfig, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if !c.initialized {
		return nil, fmt.Errorf("cache not initialized")
	}
	return deepCopySecurityConfig(c.securityConfig), nil
}

func (c *cacheImpl) GetServiceConfig() (*v1beta1.ServiceConfig, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if !c.initialized {
		return nil, fmt.Errorf("cache not initialized")
	}
	return deepCopyServiceConfig(c.serviceConfig), nil
}

func (c *cacheImpl) GetMultiNodeConfig() (*v1beta1.MultiNodeConfig, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if !c.initialized {
		return nil, fmt.Errorf("cache not initialized")
	}
	return deepCopyMultiNodeConfig(c.multiNodeConfig), nil
}

func (c *cacheImpl) GetOtelCollectorConfig() (*v1beta1.OtelCollectorConfig, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if !c.initialized {
		return nil, fmt.Errorf("cache not initialized")
	}
	return deepCopyOtelCollectorConfig(c.otelCollectorConfig), nil
}

func (c *cacheImpl) GetAutoscalerConfig() (*v1beta1.AutoscalerConfig, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if !c.initialized {
		return nil, fmt.Errorf("cache not initialized")
	}
	return deepCopyAutoscalerConfig(c.autoscalerConfig), nil
}

func (c *cacheImpl) Get(ctx context.Context) (*corev1.ConfigMap, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if !c.initialized {
		return nil, fmt.Errorf("cache not initialized")
	}

	if c.configMap == nil {
		return nil, fmt.Errorf("configMap is nil")
	}

	return c.configMap.DeepCopy(), nil
}

// SetupConfigMapWatch configures the cache to receive ConfigMap update events
// Uses controller-runtime v0.19+ API with builder pattern
func SetupConfigMapWatch(mgr manager.Manager, cache ConfigCache, configMapName, configMapNamespace string) error {
	cacheImpl, ok := cache.(*cacheImpl)
	if !ok {
		return fmt.Errorf("invalid cache implementation")
	}

	// Create a predicate to filter only the specific ConfigMap we care about
	configMapPredicate := predicate.Funcs{
		CreateFunc: func(e event.CreateEvent) bool {
			cm, ok := e.Object.(*corev1.ConfigMap)
			if !ok {
				return false
			}
			return cm.Name == configMapName && cm.Namespace == configMapNamespace
		},
		UpdateFunc: func(e event.UpdateEvent) bool {
			cm, ok := e.ObjectNew.(*corev1.ConfigMap)
			if !ok {
				return false
			}
			return cm.Name == configMapName && cm.Namespace == configMapNamespace
		},
		DeleteFunc: func(e event.DeleteEvent) bool {
			// Don't reconcile on deletes - keep using cached config
			return false
		},
		GenericFunc: func(e event.GenericEvent) bool {
			return false
		},
	}

	// Create a reconciler that updates the cache when the ConfigMap changes
	reconciler := &configMapCacheReconciler{
		client:    mgr.GetClient(),
		cache:     cacheImpl,
		cmName:    configMapName,
		cmNamespace: configMapNamespace,
	}

	// Use builder pattern to set up the watch
	err := builder.ControllerManagedBy(mgr).
		Named("configmap-cache-watcher").
		For(&corev1.ConfigMap{}, builder.WithPredicates(configMapPredicate)).
		Complete(reconciler)

	if err != nil {
		return fmt.Errorf("failed to setup ConfigMap watch: %w", err)
	}

	log.Info("ConfigMap watch configured successfully", "configMapName", configMapName, "namespace", configMapNamespace)
	return nil
}

// configMapCacheReconciler reconciles the ConfigMap and updates the cache.
// This reconciler is triggered by the watch when the target ConfigMap changes.
type configMapCacheReconciler struct {
	client      client.Client
	cache       *cacheImpl
	cmName      string
	cmNamespace string
}

// Reconcile updates the cache when the ConfigMap changes.
// Returns nil error intentionally to avoid requeueing - this allows the system to continue
// operating with cached config during transient failures (network issues, API server unavailable).
// Trade-off: Favors system availability over strict config consistency.
// If the ConfigMap is deleted, the cache retains the last known good configuration.
func (r *configMapCacheReconciler) Reconcile(ctx context.Context, req reconcile.Request) (reconcile.Result, error) {
	// Only reconcile if it's our target ConfigMap
	if req.Name != r.cmName || req.Namespace != r.cmNamespace {
		return reconcile.Result{}, nil
	}

	log.Info("Reconciling ConfigMap for cache update", "name", req.Name, "namespace", req.Namespace)

	// Fetch the latest ConfigMap
	configMap := &corev1.ConfigMap{}
	if err := r.client.Get(ctx, req.NamespacedName, configMap); err != nil {
		log.Error(err, "Failed to get ConfigMap during reconciliation", "name", req.Name, "namespace", req.Namespace)
		// Return nil to avoid requeueing - keeps system operational with cached config
		return reconcile.Result{}, nil
	}

	// Update the cache
	r.cache.handleConfigMapUpdate(ctx, configMap)

	return reconcile.Result{}, nil
}
