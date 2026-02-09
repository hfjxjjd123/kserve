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
	"fmt"
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"
	ctrlcache "sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/event"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/predicate"

	"github.com/kserve/kserve/pkg/apis/serving/v1beta1"
	"github.com/kserve/kserve/pkg/credentials"
	"github.com/kserve/kserve/pkg/types"
)

var log = logf.Log.WithName("ConfigCache")

type Options struct {
	ConfigMapName      string
	ConfigMapNamespace string
}

// ConfigCache provides thread-safe access to parsed ConfigMap configurations.
type ConfigCache interface {
	GetIngressConfig() (*v1beta1.IngressConfig, error)
	GetDeployConfig() (*v1beta1.DeployConfig, error)
	GetInferenceServicesConfig() (*v1beta1.InferenceServicesConfig, error)
	GetStorageInitializerConfig() (*types.StorageInitializerConfig, error)
	GetCredentialConfig() (*credentials.CredentialConfig, error)
	GetLocalModelConfig() (*v1beta1.LocalModelConfig, error)
	GetSecurityConfig() (*v1beta1.SecurityConfig, error)
	GetServiceConfig() (*v1beta1.ServiceConfig, error)
	GetMultiNodeConfig() (*v1beta1.MultiNodeConfig, error)
	GetOtelCollectorConfig() (*v1beta1.OtelCollectorConfig, error)
	GetAutoscalerConfig() (*v1beta1.AutoscalerConfig, error)
	Get(ctx context.Context) (*corev1.ConfigMap, error)
	WaitForCacheSync(ctx context.Context) error
	Start(ctx context.Context) error
}

type cacheImpl struct {
	mgrCache           ctrlcache.Cache
	configMapName      string
	configMapNamespace string

	mu sync.RWMutex

	configMap                  *corev1.ConfigMap
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

	initialized bool
	initCond    *sync.Cond
}

// SetupConfigCache creates and registers a ConfigCache with the manager.
// It performs three steps atomically:
//  1. Creates the cache backed by the manager's informer
//  2. Performs initial load via API reader (safe before mgr.Start())
//  3. Registers the cache as a Runnable with mgr.Add() for ongoing updates
//
// This enforces the lifecycle contract - you cannot forget to register the cache.
// The cache will receive updates via informer after mgr.Start() is called.
func SetupConfigCache(mgr manager.Manager, opts Options) (ConfigCache, error) {
	c := &cacheImpl{
		mgrCache:           mgr.GetCache(),
		configMapName:      opts.ConfigMapName,
		configMapNamespace: opts.ConfigMapNamespace,
	}
	c.initCond = sync.NewCond(&c.mu)

	// Step 1: Initial load via API reader — direct API call, safe before mgr.Start()
	cm := &corev1.ConfigMap{}
	key := client.ObjectKey{Name: opts.ConfigMapName, Namespace: opts.ConfigMapNamespace}
	if err := mgr.GetAPIReader().Get(context.Background(), key, cm); err != nil {
		return nil, fmt.Errorf("failed to read ConfigMap %s/%s: %w", opts.ConfigMapNamespace, opts.ConfigMapName, err)
	}
	if err := c.parseAndUpdateConfigs(context.Background(), cm); err != nil {
		return nil, fmt.Errorf("failed to parse ConfigMap: %w", err)
	}

	// Step 2: Register with manager for ongoing updates (enforces lifecycle coupling)
	if err := mgr.Add(c); err != nil {
		return nil, fmt.Errorf("failed to register ConfigMap cache with manager: %w", err)
	}

	log.Info("ConfigMap cache setup complete", "configMapName", opts.ConfigMapName, "namespace", opts.ConfigMapNamespace)
	return c, nil
}

// Start implements manager.Runnable. It is called by the manager after the informer cache
// is synced, so GetInformer is safe to call here. It blocks until the context is cancelled.
func (c *cacheImpl) Start(ctx context.Context) error {
	log.Info("Starting ConfigMap cache watch",
		"configMapName", c.configMapName, "namespace", c.configMapNamespace)

	informer, err := c.mgrCache.GetInformer(ctx, &corev1.ConfigMap{})
	if err != nil {
		return fmt.Errorf("failed to get ConfigMap informer: %w", err)
	}

	handler := &configMapCacheEventHandler{
		cache:       c,
		cmName:      c.configMapName,
		cmNamespace: c.configMapNamespace,
	}

	reg, err := informer.AddEventHandler(handler)
	if err != nil {
		return fmt.Errorf("failed to add event handler to informer: %w", err)
	}

	log.Info("ConfigMap watch started", "configMapName", c.configMapName, "namespace", c.configMapNamespace)

	// Block until context is cancelled (Runnable contract)
	<-ctx.Done()

	// Clean up event handler on shutdown
	if err := informer.RemoveEventHandler(reg); err != nil {
		log.Error(err, "failed to remove event handler during shutdown")
	}

	return nil
}

func (c *cacheImpl) WaitForCacheSync(ctx context.Context) error {
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for {
		c.mu.RLock()
		initialized := c.initialized
		c.mu.RUnlock()

		if initialized {
			return nil
		}

		select {
		case <-ticker.C:
			continue
		case <-ctx.Done():
			return fmt.Errorf("timeout waiting for cache sync: %w", ctx.Err())
		}
	}
}

func (c *cacheImpl) parseAndUpdateConfigs(ctx context.Context, configMap *corev1.ConfigMap) error {
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

	if !c.initialized {
		c.initialized = true
		c.initCond.Broadcast()
		log.Info("ConfigMap cache initialized")
	}

	log.Info("ConfigMap configs updated successfully")
	return nil
}

func (c *cacheImpl) handleConfigMapUpdate(ctx context.Context, cm *corev1.ConfigMap) {
	log.Info("ConfigMap update detected, refreshing cache", "configMapName", cm.Name)

	if err := c.parseAndUpdateConfigs(ctx, cm); err != nil {
		log.Error(err, "Failed to update cache after ConfigMap change")
	}
}

// All typed getters return deep copies of cached configs.

func (c *cacheImpl) GetIngressConfig() (*v1beta1.IngressConfig, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if !c.initialized {
		return nil, fmt.Errorf("cache not initialized")
	}
	return c.ingressConfig.DeepCopy(), nil
}

func (c *cacheImpl) GetDeployConfig() (*v1beta1.DeployConfig, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if !c.initialized {
		return nil, fmt.Errorf("cache not initialized")
	}
	return c.deployConfig.DeepCopy(), nil
}

func (c *cacheImpl) GetInferenceServicesConfig() (*v1beta1.InferenceServicesConfig, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if !c.initialized {
		return nil, fmt.Errorf("cache not initialized")
	}
	return c.inferenceServicesConfig.DeepCopy(), nil
}

func (c *cacheImpl) GetStorageInitializerConfig() (*types.StorageInitializerConfig, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if !c.initialized {
		return nil, fmt.Errorf("cache not initialized")
	}
	return c.storageInitializerConfig.DeepCopy(), nil
}

func (c *cacheImpl) GetCredentialConfig() (*credentials.CredentialConfig, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if !c.initialized {
		return nil, fmt.Errorf("cache not initialized")
	}
	return c.credentialConfig.DeepCopy(), nil
}

func (c *cacheImpl) GetLocalModelConfig() (*v1beta1.LocalModelConfig, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if !c.initialized {
		return nil, fmt.Errorf("cache not initialized")
	}
	return c.localModelConfig.DeepCopy(), nil
}

func (c *cacheImpl) GetSecurityConfig() (*v1beta1.SecurityConfig, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if !c.initialized {
		return nil, fmt.Errorf("cache not initialized")
	}
	return c.securityConfig.DeepCopy(), nil
}

func (c *cacheImpl) GetServiceConfig() (*v1beta1.ServiceConfig, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if !c.initialized {
		return nil, fmt.Errorf("cache not initialized")
	}
	return c.serviceConfig.DeepCopy(), nil
}

func (c *cacheImpl) GetMultiNodeConfig() (*v1beta1.MultiNodeConfig, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if !c.initialized {
		return nil, fmt.Errorf("cache not initialized")
	}
	return c.multiNodeConfig.DeepCopy(), nil
}

func (c *cacheImpl) GetOtelCollectorConfig() (*v1beta1.OtelCollectorConfig, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if !c.initialized {
		return nil, fmt.Errorf("cache not initialized")
	}
	return c.otelCollectorConfig.DeepCopy(), nil
}

func (c *cacheImpl) GetAutoscalerConfig() (*v1beta1.AutoscalerConfig, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if !c.initialized {
		return nil, fmt.Errorf("cache not initialized")
	}
	return c.autoscalerConfig.DeepCopy(), nil
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

// InferenceServiceConfigPredicate filters ConfigMap events for controller watches.
func InferenceServiceConfigPredicate(configMapName, configMapNamespace string) predicate.Predicate {
	return predicate.Funcs{
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
			return false
		},
		GenericFunc: func(e event.GenericEvent) bool {
			return false
		},
	}
}


type configMapCacheEventHandler struct {
	cache       *cacheImpl
	cmName      string
	cmNamespace string
}

func (h *configMapCacheEventHandler) OnAdd(obj interface{}, isInInitialList bool) {
	cm, ok := obj.(*corev1.ConfigMap)
	if !ok {
		log.Error(fmt.Errorf("unexpected object type"), "Expected ConfigMap", "got", obj)
		return
	}

	if cm.Name != h.cmName || cm.Namespace != h.cmNamespace {
		return
	}

	log.Info("ConfigMap created, updating cache", "name", cm.Name, "namespace", cm.Namespace)
	h.cache.handleConfigMapUpdate(context.Background(), cm)
}

func (h *configMapCacheEventHandler) OnUpdate(oldObj, newObj interface{}) {
	cm, ok := newObj.(*corev1.ConfigMap)
	if !ok {
		log.Error(fmt.Errorf("unexpected object type"), "Expected ConfigMap", "got", newObj)
		return
	}

	if cm.Name != h.cmName || cm.Namespace != h.cmNamespace {
		return
	}

	log.Info("ConfigMap updated, updating cache", "name", cm.Name, "namespace", cm.Namespace)
	h.cache.handleConfigMapUpdate(context.Background(), cm)
}

func (h *configMapCacheEventHandler) OnDelete(obj interface{}) {
	cm, ok := obj.(*corev1.ConfigMap)
	if !ok {
		tombstone, ok := obj.(client.ObjectKey)
		if !ok {
			log.Error(fmt.Errorf("unexpected object type"), "Expected ConfigMap or tombstone", "got", obj)
			return
		}
		log.Info("ConfigMap deleted (tombstone), keeping cached config", "key", tombstone)
		return
	}

	if cm.Name != h.cmName || cm.Namespace != h.cmNamespace {
		return
	}

	log.Info("ConfigMap deleted, retaining cached config", "name", cm.Name, "namespace", cm.Namespace)
}
