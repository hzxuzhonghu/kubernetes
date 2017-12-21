/*
Copyright 2017 The Kubernetes Authors.

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

package resourceconfig

import (
	"fmt"
	"strconv"
	"strings"

	"k8s.io/apimachinery/pkg/apimachinery/registered"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	serverstore "k8s.io/apiserver/pkg/server/storage"
	utilflag "k8s.io/apiserver/pkg/util/flag"
)

// MergeResourceEncodingConfigs merges the given defaultResourceConfig with specific GroupVersionResource overrides.
func MergeResourceEncodingConfigs(
	defaultResourceEncoding *serverstore.DefaultResourceEncodingConfig,
	resourceEncodingOverrides []schema.GroupVersionResource,
) *serverstore.DefaultResourceEncodingConfig {
	resourceEncodingConfig := defaultResourceEncoding
	for _, gvr := range resourceEncodingOverrides {
		resourceEncodingConfig.SetResourceEncoding(gvr.GroupResource(), gvr.GroupVersion(),
			schema.GroupVersion{Group: gvr.Group, Version: runtime.APIVersionInternal})
	}
	return resourceEncodingConfig
}

// MergeGroupEncodingConfigs merges the given defaultResourceConfig with specific GroupVersion overrides.
func MergeGroupEncodingConfigs(
	defaultResourceEncoding *serverstore.DefaultResourceEncodingConfig,
	storageEncodingOverrides map[string]schema.GroupVersion,
) *serverstore.DefaultResourceEncodingConfig {
	resourceEncodingConfig := defaultResourceEncoding
	for group, storageEncodingVersion := range storageEncodingOverrides {
		resourceEncodingConfig.SetVersionEncoding(group, storageEncodingVersion, schema.GroupVersion{Group: group, Version: runtime.APIVersionInternal})
	}
	return resourceEncodingConfig
}

// MergeAPIResourceConfigs merges the given defaultAPIResourceConfig with the given resourceConfigOverrides.
// Exclude the groups not registered in registry, and check if version is
// not registered in group, then it will fail.
func MergeAPIResourceConfigs(
	defaultAPIResourceConfig *serverstore.ResourceConfig,
	resourceConfigOverrides utilflag.ConfigurationMap,
	registry *registered.APIRegistrationManager,
) (*serverstore.ResourceConfig, error) {
	resourceConfig := defaultAPIResourceConfig
	overrides := resourceConfigOverrides

	// "api/all=false" allows users to selectively enable specific api versions.
	allAPIFlagValue, ok := overrides["api/all"]
	if ok {
		if allAPIFlagValue == "false" {
			// Disable all group versions.
			resourceConfig.DisableVersions(registry.RegisteredGroupVersions()...)
		} else if allAPIFlagValue == "true" {
			resourceConfig.EnableVersions(registry.RegisteredGroupVersions()...)
		}
	}

	// "api/legacy=false" allows users to disable legacy api versions.
	disableLegacyAPIs := false
	legacyAPIFlagValue, ok := overrides["api/legacy"]
	if ok && legacyAPIFlagValue == "false" {
		disableLegacyAPIs = true
	}
	_ = disableLegacyAPIs // hush the compiler while we don't have legacy APIs to disable.

	// "<resourceSpecifier>={true|false} allows users to enable/disable API.
	// This takes preference over api/all and api/legacy, if specified.
	// Iterate through all group/version overrides specified in runtimeConfig.
	for key := range overrides {
		// Have already handled them above. Can skip them here.
		if key == "api/all" || key == "api/legacy" {
			continue
		}

		tokens := strings.Split(key, "/")
		if len(tokens) != 2 {
			continue
		}
		groupVersionString := tokens[0] + "/" + tokens[1]
		// HACK: Hack for "v1" legacy group version.
		// Remove when we stop supporting the legacy group version.
		if groupVersionString == "api/v1" {
			groupVersionString = "v1"
		}
		groupVersion, err := schema.ParseGroupVersion(groupVersionString)
		if err != nil {
			return nil, fmt.Errorf("invalid key %s", key)
		}

		// Exclude group not registered into the registry.
		if !registry.IsRegistered(groupVersion.Group) {
			continue
		}

		// Verify that the groupVersion is registered into registry.
		if !registry.IsRegisteredVersion(groupVersion) {
			return nil, fmt.Errorf("group version %s that has not been registered", groupVersion.String())
		}
		enabled, err := getRuntimeConfigValue(overrides, key, false)
		if err != nil {
			return nil, err
		}
		if enabled {
			resourceConfig.EnableVersions(groupVersion)
		} else {
			resourceConfig.DisableVersions(groupVersion)
		}
	}

	// Iterate through all group/version/resource overrides specified in runtimeConfig.
	for key := range overrides {
		tokens := strings.Split(key, "/")
		if len(tokens) != 3 {
			continue
		}
		groupVersionString := tokens[0] + "/" + tokens[1]
		// HACK: Hack for "v1" legacy group version.
		// Remove when we stop supporting the legacy group version.
		if groupVersionString == "api/v1" {
			groupVersionString = "v1"
		}
		resource := tokens[2]
		groupVersion, err := schema.ParseGroupVersion(groupVersionString)
		if err != nil {
			return nil, fmt.Errorf("invalid key %s", key)
		}
		// Exclude group not registered into the registry.
		if !registry.IsRegistered(groupVersion.Group) {
			continue
		}

		// Verify that the groupVersion is registered into registry.
		if !registry.IsRegisteredVersion(groupVersion) {
			return nil, fmt.Errorf("group version %s has not been registered", groupVersion.String())
		}

		if !resourceConfig.AnyResourcesForVersionEnabled(groupVersion) {
			return nil, fmt.Errorf("%v is disabled, you cannot configure its resources individually", groupVersion)
		}

		enabled, err := getRuntimeConfigValue(overrides, key, false)
		if err != nil {
			return nil, err
		}
		if enabled {
			resourceConfig.EnableResources(groupVersion.WithResource(resource))
		} else {
			resourceConfig.DisableResources(groupVersion.WithResource(resource))
		}
	}
	return resourceConfig, nil
}

func getRuntimeConfigValue(overrides utilflag.ConfigurationMap, apiKey string, defaultValue bool) (bool, error) {
	flagValue, ok := overrides[apiKey]
	if ok {
		if flagValue == "" {
			return true, nil
		}
		boolValue, err := strconv.ParseBool(flagValue)
		if err != nil {
			return false, fmt.Errorf("invalid value of %s: %s, err: %v", apiKey, flagValue, err)
		}
		return boolValue, nil
	}
	return defaultValue, nil
}

// ParseGroups takes in resourceConfig and returns parsed groups.
func ParseGroups(resourceConfig utilflag.ConfigurationMap) ([]string, error) {
	groups := []string{}
	for key := range resourceConfig {
		if key == "api/all" || key == "api/legacy" {
			continue
		}
		tokens := strings.Split(key, "/")
		if len(tokens) != 2 && len(tokens) != 3 {
			return groups, fmt.Errorf("runtime-config invalid key %s", key)
		}
		groupVersionString := tokens[0] + "/" + tokens[1]
		if groupVersionString == "api/v1" {
			groupVersionString = "v1"
		}
		groupVersion, err := schema.ParseGroupVersion(groupVersionString)
		if err != nil {
			return nil, fmt.Errorf("runtime-config invalid key %s", key)
		}
		groups = append(groups, groupVersion.Group)
	}

	return groups, nil
}
