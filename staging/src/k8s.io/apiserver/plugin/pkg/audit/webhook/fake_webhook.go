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
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"reflect"
	"testing"

	"github.com/stretchr/testify/assert"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/serializer/json"
	"k8s.io/apiserver/pkg/audit"
)

// NewFakeWebhookHandler returns a handler which recieves webhook events and decodes the
// request body. The caller passes a callback which is called on each webhook POST.
// The object passed to cb is of the same type as list.
func NewFakeWebhookHandler(t *testing.T, list runtime.Object, cb func(events runtime.Object)) http.Handler {
	s := json.NewSerializer(json.DefaultMetaFactory, audit.Scheme, audit.Scheme, false)
	return &fakeWebhookHandler{
		t:          t,
		list:       list,
		onEvents:   cb,
		serializer: s,
	}
}

type fakeWebhookHandler struct {
	t *testing.T

	list     runtime.Object
	onEvents func(events runtime.Object)

	serializer runtime.Serializer
}

func (t *fakeWebhookHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	err := func() error {
		body, err := ioutil.ReadAll(r.Body)
		if err != nil {
			return fmt.Errorf("read webhook request body: %v", err)
		}

		obj, _, err := t.serializer.Decode(body, nil, t.list.DeepCopyObject())
		if err != nil {
			return fmt.Errorf("decode request body: %v", err)
		}
		if reflect.TypeOf(obj).Elem() != reflect.TypeOf(t.list).Elem() {
			return fmt.Errorf("expected %T, got %T", t.list, obj)
		}
		t.onEvents(obj)
		return nil
	}()

	if err == nil {
		io.WriteString(w, "{}")
		return
	}
	// In a goroutine, can't call Fatal.
	assert.NoError(t.t, err, "failed to read request body")
	http.Error(w, err.Error(), http.StatusInternalServerError)
}
