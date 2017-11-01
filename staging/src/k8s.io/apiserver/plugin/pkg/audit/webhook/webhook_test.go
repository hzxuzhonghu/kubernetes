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

package webhook

import (
	stdjson "encoding/json"
	"io/ioutil"
	"net/http/httptest"
	"os"
	"testing"

	"github.com/stretchr/testify/require"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	auditinternal "k8s.io/apiserver/pkg/apis/audit"
	auditv1beta1 "k8s.io/apiserver/pkg/apis/audit/v1beta1"
	"k8s.io/client-go/tools/clientcmd/api/v1"
)

func newTestBackend(t *testing.T, endpoint string, groupVersion schema.GroupVersion) *backend {
	config := v1.Config{
		Clusters: []v1.NamedCluster{
			{Cluster: v1.Cluster{Server: endpoint, InsecureSkipTLSVerify: true}},
		},
	}
	f, err := ioutil.TempFile("", "k8s_audit_webhook_test_")
	require.NoError(t, err, "creating temp file")

	defer func() {
		f.Close()
		os.Remove(f.Name())
	}()

	// NOTE(ericchiang): Do we need to use a proper serializer?
	require.NoError(t, stdjson.NewEncoder(f).Encode(config), "writing kubeconfig")

	b, err := NewBackend(f.Name(), groupVersion)
	require.NoError(t, err, "initializing backend")

	return b.(*backend)
}

func TestWebhook(t *testing.T) {
	gotEvents := false
	defer func() { require.True(t, gotEvents, "no events received") }()

	s := httptest.NewServer(NewFakeWebhookHandler(t, &auditv1beta1.EventList{}, func(events runtime.Object) {
		gotEvents = true
	}))
	defer s.Close()

	backend := newTestBackend(t, s.URL, auditv1beta1.SchemeGroupVersion)

	// Ensure this doesn't return a serialization error.
	event := &auditinternal.Event{}
	require.NoError(t, backend.processEvents(event), "failed to send events")
}
