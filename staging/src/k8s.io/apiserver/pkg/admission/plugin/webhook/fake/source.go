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
	registrationv1beta1 "k8s.io/api/admissionregistration/v1beta1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apiserver/pkg/admission/plugin/webhook/generic"
)

type source struct {
	hooks []registrationv1beta1.Webhook
	err   error
}

func NewSource(hooks []registrationv1beta1.Webhook, err error) generic.Source {
	return &source{
		hooks: hooks,
		err:   err,
	}
}

func (h *source) Webhooks() []registrationv1beta1.Webhook {
	if h.err != nil {
		return nil
	}
	for i, hook := range h.hooks {
		if hook.NamespaceSelector == nil {
			h.hooks[i].NamespaceSelector = &metav1.LabelSelector{}
		}
	}
	return h.hooks
}

func (h *source) HasSynched() bool {
	return true
}

func (h *source) Run(stopCh <-chan struct{}) {}
