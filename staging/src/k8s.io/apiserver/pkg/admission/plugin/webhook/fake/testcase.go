/*
Copyright 2018 The Kubernetes Authors.

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

package fake

import (
	"net/url"

	registrationv1beta1 "k8s.io/api/admissionregistration/v1beta1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apiserver/pkg/admission"
	"k8s.io/apiserver/pkg/admission/plugin/webhook/generic"
	"k8s.io/apiserver/pkg/admission/plugin/webhook/namespace"
	"k8s.io/apiserver/pkg/admission/plugin/webhook/testcerts"
	"k8s.io/apiserver/pkg/authentication/user"
	"k8s.io/client-go/informers"
	fakeClientSet "k8s.io/client-go/kubernetes/fake"
)

var matchEverythingRules = []registrationv1beta1.RuleWithOperations{{
	Operations: []registrationv1beta1.OperationType{registrationv1beta1.OperationAll},
	Rule: registrationv1beta1.Rule{
		APIGroups:   []string{"*"},
		APIVersions: []string{"*"},
		Resources:   []string{"*/*"},
	},
}}

func NewNamespaceMatcher(name string) *namespace.Matcher {
	client := fakeClientSet.NewSimpleClientset(&corev1.Namespace{
		ObjectMeta: metav1.ObjectMeta{
			Name: name,
			Labels: map[string]string{
				"runlevel": "0",
			},
		},
	})
	informerFactory := informers.NewSharedInformerFactory(client, 0)

	return &namespace.Matcher{
		NamespaceLister: informerFactory.Core().V1().Namespaces().Lister(),
		Client:          client,
	}
}

func NewAttribute(namespace string) admission.Attributes {
	// Set up a test object for the call
	kind := corev1.SchemeGroupVersion.WithKind("Pod")
	name := "my-pod"
	object := corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Labels: map[string]string{
				"pod.name": name,
			},
			Name:      name,
			Namespace: namespace,
		},
		TypeMeta: metav1.TypeMeta{
			APIVersion: "v1",
			Kind:       "Pod",
		},
	}
	oldObject := corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace},
	}
	operation := admission.Update
	resource := corev1.Resource("pods").WithVersion("v1")
	subResource := ""
	userInfo := user.DefaultInfo{
		Name: "webhook-test",
		UID:  "webhook-test",
	}

	return admission.NewAttributesRecord(&object, &oldObject, kind, namespace, name, resource, subResource, operation, &userInfo)
}

type urlConfigGenerator struct {
	baseURL *url.URL
}

// CcfgURL returns a client config using the URL mechanism.
func (c urlConfigGenerator) ccfgURL(urlPath string) registrationv1beta1.WebhookClientConfig {
	u2 := *c.baseURL
	u2.Path = urlPath
	urlString := u2.String()
	return registrationv1beta1.WebhookClientConfig{
		URL:      &urlString,
		CABundle: testcerts.CACert,
	}
}

type Test struct {
	HookSource    generic.Source
	Path          string
	ExpectAllow   bool
	ErrorContains string
}

func NewTestCases(url *url.URL) map[string]Test {
	policyFail := registrationv1beta1.Fail
	policyIgnore := registrationv1beta1.Ignore
	ccfgURL := urlConfigGenerator{url}.ccfgURL

	return map[string]Test{
		"no match": {
			HookSource: NewSource(
				[]registrationv1beta1.Webhook{{
					Name:         "nomatch",
					ClientConfig: ccfgSVC("disallow"),
					Rules: []registrationv1beta1.RuleWithOperations{{
						Operations: []registrationv1beta1.OperationType{registrationv1beta1.Create},
					}},
				}}, nil),
			ExpectAllow: true,
		},
		"match & allow": {
			HookSource: NewSource(
				[]registrationv1beta1.Webhook{{
					Name:         "allow",
					ClientConfig: ccfgSVC("allow"),
					Rules:        matchEverythingRules,
				}}, nil),
			ExpectAllow: true,
		},
		"match & disallow": {
			HookSource: NewSource(
				[]registrationv1beta1.Webhook{{
					Name:         "disallow",
					ClientConfig: ccfgSVC("disallow"),
					Rules:        matchEverythingRules,
				}}, nil),
			ErrorContains: "without explanation",
		},
		"match & disallow ii": {
			HookSource: NewSource(
				[]registrationv1beta1.Webhook{{
					Name:         "disallowReason",
					ClientConfig: ccfgSVC("disallowReason"),
					Rules:        matchEverythingRules,
				}}, nil),

			ErrorContains: "you shall not pass",
		},
		"match & disallow & but allowed because namespaceSelector exempt the ns": {
			HookSource: NewSource(
				[]registrationv1beta1.Webhook{{
					Name:         "disallow",
					ClientConfig: ccfgSVC("disallow"),
					Rules:        newMatchEverythingRules(),
					NamespaceSelector: &metav1.LabelSelector{
						MatchExpressions: []metav1.LabelSelectorRequirement{{
							Key:      "runlevel",
							Values:   []string{"1"},
							Operator: metav1.LabelSelectorOpIn,
						}},
					},
				}}, nil),

			ExpectAllow: true,
		},
		"match & disallow & but allowed because namespaceSelector exempt the ns ii": {
			HookSource: NewSource(
				[]registrationv1beta1.Webhook{{
					Name:         "disallow",
					ClientConfig: ccfgSVC("disallow"),
					Rules:        newMatchEverythingRules(),
					NamespaceSelector: &metav1.LabelSelector{
						MatchExpressions: []metav1.LabelSelectorRequirement{{
							Key:      "runlevel",
							Values:   []string{"0"},
							Operator: metav1.LabelSelectorOpNotIn,
						}},
					},
				}}, nil),
			ExpectAllow: true,
		},
		"match & fail (but allow because fail open)": {
			HookSource: NewSource(
				[]registrationv1beta1.Webhook{{
					Name:          "internalErr A",
					ClientConfig:  ccfgSVC("internalErr"),
					Rules:         matchEverythingRules,
					FailurePolicy: &policyIgnore,
				}, {
					Name:          "internalErr B",
					ClientConfig:  ccfgSVC("internalErr"),
					Rules:         matchEverythingRules,
					FailurePolicy: &policyIgnore,
				}, {
					Name:          "internalErr C",
					ClientConfig:  ccfgSVC("internalErr"),
					Rules:         matchEverythingRules,
					FailurePolicy: &policyIgnore,
				}}, nil),

			ExpectAllow: true,
		},
		"match & fail (but disallow because fail closed on nil)": {
			HookSource: NewSource(
				[]registrationv1beta1.Webhook{{
					Name:         "internalErr A",
					ClientConfig: ccfgSVC("internalErr"),
					Rules:        matchEverythingRules,
				}, {
					Name:         "internalErr B",
					ClientConfig: ccfgSVC("internalErr"),
					Rules:        matchEverythingRules,
				}, {
					Name:         "internalErr C",
					ClientConfig: ccfgSVC("internalErr"),
					Rules:        matchEverythingRules,
				}}, nil),
			ExpectAllow: false,
		},
		"match & fail (but fail because fail closed)": {
			HookSource: NewSource(
				[]registrationv1beta1.Webhook{{
					Name:          "internalErr A",
					ClientConfig:  ccfgSVC("internalErr"),
					Rules:         matchEverythingRules,
					FailurePolicy: &policyFail,
				}, {
					Name:          "internalErr B",
					ClientConfig:  ccfgSVC("internalErr"),
					Rules:         matchEverythingRules,
					FailurePolicy: &policyFail,
				}, {
					Name:          "internalErr C",
					ClientConfig:  ccfgSVC("internalErr"),
					Rules:         matchEverythingRules,
					FailurePolicy: &policyFail,
				}}, nil),
			ExpectAllow: false,
		},
		"match & allow (url)": {
			HookSource: NewSource(
				[]registrationv1beta1.Webhook{{
					Name:         "allow",
					ClientConfig: ccfgURL("allow"),
					Rules:        matchEverythingRules,
				}}, nil),
			ExpectAllow: true,
		},
		"match & disallow (url)": {
			HookSource: NewSource(
				[]registrationv1beta1.Webhook{{
					Name:         "disallow",
					ClientConfig: ccfgURL("disallow"),
					Rules:        matchEverythingRules,
				}}, nil),
			ErrorContains: "without explanation",
		},
		"absent response and fail open": {
			HookSource: NewSource(
				[]registrationv1beta1.Webhook{{
					Name:          "nilResponse",
					ClientConfig:  ccfgURL("nilResponse"),
					FailurePolicy: &policyIgnore,
					Rules:         matchEverythingRules,
				}}, nil),

			ExpectAllow: true,
		},
		"absent response and fail closed": {
			HookSource: NewSource(
				[]registrationv1beta1.Webhook{{
					Name:          "nilResponse",
					ClientConfig:  ccfgURL("nilResponse"),
					FailurePolicy: &policyFail,
					Rules:         matchEverythingRules,
				}}, nil),
			ErrorContains: "Webhook response was absent",
		},
		// No need to test everything with the url case, since only the
		// connection is different.
	}
}

type CachedTest struct {
	Name        string
	HookSource  generic.Source
	ExpectAllow bool
	ExpectCache bool
}

func NewCachedClientTestcases(url *url.URL) []CachedTest {
	policyIgnore := registrationv1beta1.Ignore
	ccfgURL := urlConfigGenerator{url}.ccfgURL

	return []CachedTest{
		{
			Name: "cache 1",
			HookSource: NewSource(
				[]registrationv1beta1.Webhook{{
					Name:          "cache1",
					ClientConfig:  ccfgSVC("allow"),
					Rules:         newMatchEverythingRules(),
					FailurePolicy: &policyIgnore,
				}}, nil),
			ExpectAllow: true,
			ExpectCache: true,
		},
		{
			Name: "cache 2",
			HookSource: NewSource(
				[]registrationv1beta1.Webhook{{
					Name:          "cache2",
					ClientConfig:  ccfgSVC("internalErr"),
					Rules:         newMatchEverythingRules(),
					FailurePolicy: &policyIgnore,
				}}, nil),
			ExpectAllow: true,
			ExpectCache: true,
		},
		{
			Name: "cache 3",
			HookSource: NewSource(
				[]registrationv1beta1.Webhook{{
					Name:          "cache3",
					ClientConfig:  ccfgSVC("allow"),
					Rules:         newMatchEverythingRules(),
					FailurePolicy: &policyIgnore,
				}}, nil),
			ExpectAllow: true,
			ExpectCache: false,
		},
		{
			Name: "cache 4",
			HookSource: NewSource(
				[]registrationv1beta1.Webhook{{
					Name:          "cache4",
					ClientConfig:  ccfgURL("allow"),
					Rules:         newMatchEverythingRules(),
					FailurePolicy: &policyIgnore,
				}}, nil),
			ExpectAllow: true,
			ExpectCache: true,
		},
		{
			Name: "cache 5",
			HookSource: NewSource(
				[]registrationv1beta1.Webhook{{
					Name:          "cache5",
					ClientConfig:  ccfgURL("allow"),
					Rules:         newMatchEverythingRules(),
					FailurePolicy: &policyIgnore,
				}}, nil),
			ExpectAllow: true,
			ExpectCache: false,
		},
	}
}

// ccfgSVC returns a client config using the service reference mechanism.
func ccfgSVC(urlPath string) registrationv1beta1.WebhookClientConfig {
	return registrationv1beta1.WebhookClientConfig{
		Service: &registrationv1beta1.ServiceReference{
			Name:      "webhook-test",
			Namespace: "default",
			Path:      &urlPath,
		},
		CABundle: testcerts.CACert,
	}
}

func newMatchEverythingRules() []registrationv1beta1.RuleWithOperations {
	return []registrationv1beta1.RuleWithOperations{{
		Operations: []registrationv1beta1.OperationType{registrationv1beta1.OperationAll},
		Rule: registrationv1beta1.Rule{
			APIGroups:   []string{"*"},
			APIVersions: []string{"*"},
			Resources:   []string{"*/*"},
		},
	}}
}
