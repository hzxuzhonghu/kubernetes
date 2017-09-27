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

package log

import (
	"fmt"
	"io"
	"strings"
	"sync"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	auditinternal "k8s.io/apiserver/pkg/apis/audit"
	"k8s.io/apiserver/pkg/audit"
)

const (
	// FormatLegacy saves event in 1-line text format.
	FormatLegacy = "legacy"
	// FormatJson saves event in structured json format.
	FormatJson = "json"
)

// The plugin name reported in error metrics.
const pluginName = "log"

// AllowedFormats are the formats known by log backend.
var AllowedFormats = []string{
	FormatLegacy,
	FormatJson,
}

func NewBackend(out io.Writer, format string, groupVersion schema.GroupVersion) (audit.Backend, error) {
	return newBlockingBackend(out, format, groupVersion), nil
}

type blockingBackend struct {
	out          io.Writer
	format       string
	groupVersion schema.GroupVersion
	mu           sync.Mutex
}

var _ audit.Backend = &blockingBackend{}

func newBlockingBackend(out io.Writer, format string, groupVersion schema.GroupVersion) audit.Backend {
	return &blockingBackend{
		out:          out,
		format:       format,
		groupVersion: groupVersion,
		mu:           sync.Mutex{},
	}
}

func (b *blockingBackend) ProcessEvents(events ...*auditinternal.Event) {
	for _, ev := range events {
		line := ""
		switch b.format {
		case FormatLegacy:
			line = audit.EventString(ev) + "\n"
		case FormatJson:
			bs, err := runtime.Encode(audit.Codecs.LegacyCodec(b.groupVersion), ev)
			if err != nil {
				audit.HandlePluginError(pluginName, err, ev)
				return
			}
			line = string(bs[:])
		default:
			audit.HandlePluginError(pluginName, fmt.Errorf("log format %q is not in list of known formats (%s)",
				b.format, strings.Join(AllowedFormats, ",")), ev)
			return
		}

		b.mu.Lock()
		_, err := fmt.Fprint(b.out, line)
		b.mu.Unlock()
		if err != nil {
			audit.HandlePluginError(pluginName, err, ev)
		}
	}
}

func (b *blockingBackend) Run(stopCh <-chan struct{}) error {
	return nil
}

func (b *blockingBackend) Shutdown() {
	// Nothing to do here.
}
