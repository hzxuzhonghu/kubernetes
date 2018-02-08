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

package validating

import (
	"net/url"
	"strings"
	"testing"

	"k8s.io/api/admission/v1beta1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apiserver/pkg/admission/plugin/webhook/fake"
	"k8s.io/apiserver/pkg/admission/plugin/webhook/namespace"
	"k8s.io/client-go/informers"
	fakeClientSet "k8s.io/client-go/kubernetes/fake"
)

// TestValidate tests that ValidatingWebhook#Validate works as expected
func TestValidate(t *testing.T) {
	scheme := runtime.NewScheme()
	v1beta1.AddToScheme(scheme)
	corev1.AddToScheme(scheme)

	testServer := fake.NewTestServer(t)
	testServer.StartTLS()
	defer testServer.Close()

	serverURL, err := url.ParseRequestURI(testServer.URL)
	if err != nil {
		t.Fatalf("this should never happen? %v", err)
	}
	wh, err := NewValidatingAdmissionWebhook(nil)
	if err != nil {
		t.Fatal(err)
	}

	wh.GetClientManager().SetAuthenticationInfoResolver(fake.NewAuthenticationInfoResolver(new(int32)))
	wh.GetClientManager().SetServiceResolver(fake.NewServiceResolver(*serverURL))
	wh.SetScheme(scheme)
	if err = wh.GetClientManager().Validate(); err != nil {
		t.Fatal(err)
	}
	ns := "webhook-test"
	wh.Webhook.SetNamespaceMatcher(fake.NewNamespaceMatcher(ns))

	table := fake.NewTestCases(serverURL)
	for name, tt := range table {
		if !strings.Contains(name, "no match") {
			continue
		}
		wh.Webhook.SetHookSource(tt.HookSource)
		err = wh.Validate(fake.NewAttribute(ns))
		if tt.ExpectAllow != (err == nil) {
			t.Errorf("%s: expected allowed=%v, but got err=%v", name, tt.ExpectAllow, err)
		}
		// ErrWebhookRejected is not an error for our purposes
		if tt.ErrorContains != "" {
			if err == nil || !strings.Contains(err.Error(), tt.ErrorContains) {
				t.Errorf("%s: expected an error saying %q, but got %v", name, tt.ErrorContains, err)
			}
		}
		if _, isStatusErr := err.(*errors.StatusError); err != nil && !isStatusErr {
			t.Errorf("%s: expected a StatusError, got %T", name, err)
		}
	}
}

// TestValidateCachedClient tests that ValidatingWebhook#Validate should cache restClient
func TestValidateCachedClient(t *testing.T) {
	scheme := runtime.NewScheme()
	v1beta1.AddToScheme(scheme)
	corev1.AddToScheme(scheme)

	testServer := fake.NewTestServer(t)
	testServer.StartTLS()
	defer testServer.Close()
	serverURL, err := url.ParseRequestURI(testServer.URL)
	if err != nil {
		t.Fatalf("this should never happen? %v", err)
	}
	wh, err := NewValidatingAdmissionWebhook(nil)
	if err != nil {
		t.Fatal(err)
	}
	wh.GetClientManager().SetServiceResolver(fake.NewServiceResolver(*serverURL))
	wh.SetScheme(scheme)

	ns := "webhook-test"
	client := fakeClientSet.NewSimpleClientset(&corev1.Namespace{
		ObjectMeta: metav1.ObjectMeta{
			Name: ns,
			Labels: map[string]string{
				"runlevel": "0",
			},
		},
	})
	informerFactory := informers.NewSharedInformerFactory(client, 0)
	stop := make(chan struct{})
	defer close(stop)
	informerFactory.Start(stop)
	informerFactory.WaitForCacheSync(stop)

	fakeNamespaceMatcher := namespace.Matcher{
		NamespaceLister: informerFactory.Core().V1().Namespaces().Lister(),
		Client:          client,
	}
	wh.Webhook.SetNamespaceMatcher(&fakeNamespaceMatcher)

	cases := fake.NewCachedClientTestcases(serverURL)
	for _, testcase := range cases {
		wh.Webhook.SetHookSource(testcase.HookSource)
		authInfoResolverCount := new(int32)
		r := fake.NewAuthenticationInfoResolver(authInfoResolverCount)
		wh.Webhook.GetClientManager().SetAuthenticationInfoResolver(r)
		if err = wh.Webhook.GetClientManager().Validate(); err != nil {
			t.Fatal(err)
		}

		err = wh.Validate(fake.NewAttribute(ns))
		if testcase.ExpectAllow != (err == nil) {
			t.Errorf("%s: expected allowed=%v, but got err=%v", testcase.Name, testcase.ExpectAllow, err)
		}

		if testcase.ExpectCache && *authInfoResolverCount != 1 {
			t.Errorf("%s: expected cacheclient, but got none", testcase.Name)
		}

		if !testcase.ExpectCache && *authInfoResolverCount != 0 {
			t.Errorf("%s: expected not cacheclient, but got cache", testcase.Name)
		}
	}
}
