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

package buffered

import (
	stdjson "encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"os"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pborman/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/runtime/serializer/json"
	"k8s.io/apimachinery/pkg/types"
	auditinternal "k8s.io/apiserver/pkg/apis/audit"
	auditv1beta1 "k8s.io/apiserver/pkg/apis/audit/v1beta1"
	"k8s.io/apiserver/pkg/audit"
	pluginlog "k8s.io/apiserver/plugin/pkg/audit/log"
	pluginwebhook "k8s.io/apiserver/plugin/pkg/audit/webhook"
	"k8s.io/client-go/tools/clientcmd/api/v1"
)

// newWebhookHandler returns a handler which recieves webhook events and decodes the
// request body. The caller passes a callback which is called on each webhook POST.
// The object passed to cb is of the same type as list.
func newWebhookHandler(t *testing.T, list runtime.Object, cb func(events runtime.Object)) http.Handler {
	s := json.NewSerializer(json.DefaultMetaFactory, audit.Scheme, audit.Scheme, false)
	return &testWebhookHandler{
		t:          t,
		list:       list,
		onEvents:   cb,
		serializer: s,
	}
}

type testWebhookHandler struct {
	t *testing.T

	list     runtime.Object
	onEvents func(events runtime.Object)

	serializer runtime.Serializer
}

func (t *testWebhookHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
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

func newWebhook(t *testing.T, endpoint string, groupVersion schema.GroupVersion) *bufferedBackend {
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

	backend, err := pluginwebhook.NewBackend(f.Name(), groupVersion, true)
	require.NoError(t, err, "initializing backend")

	backend = NewBackend(backend)
	return backend.(*bufferedBackend)
}

// waitForEmptyBuffer indicates when the sendBatchEvents method has read from the
// existing buffer. This lets test coordinate closing a timer and stop channel
// until the for loop has read from the buffer.
func waitForEmptyBuffer(b *bufferedBackend) {
	for len(b.buffer) != 0 {
		time.Sleep(time.Millisecond)
	}
}

func TestBatchWebhookMaxEvents(t *testing.T) {
	nRest := 10
	events := make([]*auditinternal.Event, defaultBatchMaxSize+nRest) // greater than max size.
	for i := range events {
		events[i] = &auditinternal.Event{}
	}

	got := make(chan int, 2)
	s := httptest.NewServer(newWebhookHandler(t, &auditv1beta1.EventList{}, func(events runtime.Object) {
		got <- len(events.(*auditv1beta1.EventList).Items)
	}))
	defer s.Close()

	backend := newWebhook(t, s.URL, auditv1beta1.SchemeGroupVersion)
	backend.ProcessEvents(events...)

	stopCh := make(chan struct{})
	timer := make(chan time.Time, 1)

	backend.processEvents(backend.collectEvents(stopCh, timer))
	require.Equal(t, defaultBatchMaxSize, <-got, "did not get batch max size")

	go func() {
		waitForEmptyBuffer(backend) // wait for the buffer to empty
		timer <- time.Now()         // Trigger the wait timeout
	}()

	backend.processEvents(backend.collectEvents(stopCh, timer))
	require.Equal(t, nRest, <-got, "failed to get the rest of the events")
}

func TestBatchWebhookStopCh(t *testing.T) {
	events := make([]*auditinternal.Event, 1) // less than max size.
	for i := range events {
		events[i] = &auditinternal.Event{}
	}

	expected := len(events)
	got := make(chan int, 2)
	s := httptest.NewServer(newWebhookHandler(t, &auditv1beta1.EventList{}, func(events runtime.Object) {
		got <- len(events.(*auditv1beta1.EventList).Items)
	}))
	defer s.Close()

	backend := newWebhook(t, s.URL, auditv1beta1.SchemeGroupVersion)
	backend.ProcessEvents(events...)

	stopCh := make(chan struct{})
	timer := make(chan time.Time)

	go func() {
		waitForEmptyBuffer(backend)
		close(stopCh) // stop channel has stopped
	}()
	backend.processEvents(backend.collectEvents(stopCh, timer))
	require.Equal(t, expected, <-got, "get queued events after timer expires")
}

func TestBatchWebhookProcessEventsAfterStop(t *testing.T) {
	events := make([]*auditinternal.Event, 1) // less than max size.
	for i := range events {
		events[i] = &auditinternal.Event{}
	}

	got := make(chan struct{})
	s := httptest.NewServer(newWebhookHandler(t, &auditv1beta1.EventList{}, func(events runtime.Object) {
		close(got)
	}))
	defer s.Close()

	backend := newWebhook(t, s.URL, auditv1beta1.SchemeGroupVersion)
	stopCh := make(chan struct{})

	backend.Run(stopCh)
	close(stopCh)
	<-backend.shutdownCh
	backend.ProcessEvents(events...)
	assert.Equal(t, 0, len(backend.buffer), "processed events after the backed has been stopped")
}

func TestBatchWebhookShutdown(t *testing.T) {
	events := make([]*auditinternal.Event, 1)
	for i := range events {
		events[i] = &auditinternal.Event{}
	}

	got := make(chan struct{})
	contReqCh := make(chan struct{})
	shutdownCh := make(chan struct{})
	s := httptest.NewServer(newWebhookHandler(t, &auditv1beta1.EventList{}, func(events runtime.Object) {
		close(got)
		<-contReqCh
	}))
	defer s.Close()

	backend := newWebhook(t, s.URL, auditv1beta1.SchemeGroupVersion)
	backend.ProcessEvents(events...)

	go func() {
		// Assume stopCh was closed.
		close(backend.buffer)
		backend.processEvents(backend.collectLastEvents())
	}()

	<-got

	go func() {
		close(backend.shutdownCh)
		backend.Shutdown()
		close(shutdownCh)
	}()

	// Wait for some time in case there's a bug that allows for the Shutdown
	// method to exit before all requests has been completed.
	time.Sleep(1 * time.Second)
	select {
	case <-shutdownCh:
		t.Fatal("Backend shut down before all requests finished")
	default:
		// Continue.
	}

	close(contReqCh)
	<-shutdownCh
}

func TestBatchWebhookEmptyBuffer(t *testing.T) {
	events := make([]*auditinternal.Event, 1) // less than max size.
	for i := range events {
		events[i] = &auditinternal.Event{}
	}

	expected := len(events)
	got := make(chan int, 2)
	s := httptest.NewServer(newWebhookHandler(t, &auditv1beta1.EventList{}, func(events runtime.Object) {
		got <- len(events.(*auditv1beta1.EventList).Items)
	}))
	defer s.Close()

	backend := newWebhook(t, s.URL, auditv1beta1.SchemeGroupVersion)

	stopCh := make(chan struct{})
	timer := make(chan time.Time, 1)

	timer <- time.Now() // Timer is done.

	// Buffer is empty, no events have been queued. This should exit but send no events.
	backend.processEvents(backend.collectEvents(stopCh, timer))

	// Send additional events after the sendBatchEvents has been called.
	backend.ProcessEvents(events...)
	go func() {
		waitForEmptyBuffer(backend)
		timer <- time.Now()
	}()

	backend.processEvents(backend.collectEvents(stopCh, timer))

	// Make sure we didn't get a POST with zero events.
	require.Equal(t, expected, <-got, "expected one event")
}

func TestBatchWebhookBufferFull(t *testing.T) {
	events := make([]*auditinternal.Event, defaultBatchBufferSize+1) // More than buffered size
	for i := range events {
		events[i] = &auditinternal.Event{}
	}
	s := httptest.NewServer(newWebhookHandler(t, &auditv1beta1.EventList{}, func(events runtime.Object) {
		// Do nothing.
	}))
	defer s.Close()

	backend := newWebhook(t, s.URL, auditv1beta1.SchemeGroupVersion)

	// Make sure this doesn't block.
	backend.ProcessEvents(events...)
}

func TestBatchWebhookRun(t *testing.T) {

	// Divisable by max batch size so we don't have to wait for a minute for
	// the test to finish.
	events := make([]*auditinternal.Event, defaultBatchMaxSize*3)
	for i := range events {
		events[i] = &auditinternal.Event{}
	}

	got := new(int64)
	want := len(events)

	wg := new(sync.WaitGroup)
	wg.Add(want)
	done := make(chan struct{})

	go func() {
		wg.Wait()
		// When the expected number of events have been received, close the channel.
		close(done)
	}()

	s := httptest.NewServer(newWebhookHandler(t, &auditv1beta1.EventList{}, func(obj runtime.Object) {
		events := obj.(*auditv1beta1.EventList)
		atomic.AddInt64(got, int64(len(events.Items)))
		wg.Add(-len(events.Items))
	}))
	defer s.Close()

	stopCh := make(chan struct{})
	defer close(stopCh)

	backend := newWebhook(t, s.URL, auditv1beta1.SchemeGroupVersion)

	// Test the Run codepath. E.g. that the spawned goroutines behave correctly.
	backend.Run(stopCh)

	backend.ProcessEvents(events...)

	select {
	case <-done:
		// Received all the events.
	case <-time.After(2 * time.Minute):
		t.Errorf("expected %d events got %d", want, atomic.LoadInt64(got))
	}
}

func TestBatchWebhookConcurrentRequests(t *testing.T) {
	events := make([]*auditinternal.Event, defaultBatchBufferSize) // Don't drop events
	for i := range events {
		events[i] = &auditinternal.Event{}
	}

	wg := new(sync.WaitGroup)
	wg.Add(len(events))

	s := httptest.NewServer(newWebhookHandler(t, &auditv1beta1.EventList{}, func(events runtime.Object) {
		wg.Add(-len(events.(*auditv1beta1.EventList).Items))

		// Since the webhook makes concurrent requests, blocking on the webhook response
		// shouldn't block the webhook from sending more events.
		//
		// Wait for all responses to be received before sending the response.
		wg.Wait()
	}))
	defer s.Close()

	stopCh := make(chan struct{})
	defer close(stopCh)

	backend := newWebhook(t, s.URL, auditv1beta1.SchemeGroupVersion)
	backend.Run(stopCh)

	backend.ProcessEvents(events...)
	// Wait for the webhook to receive all events.
	wg.Wait()
}

var bufferedLogcases = []struct {
	event    *auditinternal.Event
	expected string
}{
	{
		&auditinternal.Event{
			AuditID: types.UID(uuid.NewRandom().String()),
		},
		`[\d\:\-\.\+TZ]+ AUDIT: id="[\w-]+" stage="" ip="<unknown>" method="" user="<none>" groups="<none>" as="<self>" asgroups="<lookup>" namespace="<none>" uri="" response="<deferred>"`,
	},
	{
		&auditinternal.Event{
			ResponseStatus: &metav1.Status{
				Code: 200,
			},
			RequestURI: "/apis/rbac.authorization.k8s.io/v1/roles",
			SourceIPs: []string{
				"127.0.0.1",
			},
			RequestReceivedTimestamp: metav1.NewMicroTime(time.Now()),
			AuditID:                  types.UID(uuid.NewRandom().String()),
			Stage:                    auditinternal.StageRequestReceived,
			Verb:                     "get",
			User: auditinternal.UserInfo{
				Username: "admin",
				Groups: []string{
					"system:masters",
					"system:authenticated",
				},
			},
			ObjectRef: &auditinternal.ObjectReference{
				Namespace: "default",
			},
		},
		`[\d\:\-\.\+TZ]+ AUDIT: id="[\w-]+" stage="RequestReceived" ip="127.0.0.1" method="get" user="admin" groups="\\"system:masters\\",\\"system:authenticated\\"" as="<self>" asgroups="<lookup>" namespace="default" uri="/apis/rbac.authorization.k8s.io/v1/roles" response="200"`,
	},
	{
		&auditinternal.Event{
			AuditID: types.UID(uuid.NewRandom().String()),
			Level:   auditinternal.LevelMetadata,
			ObjectRef: &auditinternal.ObjectReference{
				Resource:    "foo",
				APIVersion:  "v1",
				Subresource: "bar",
			},
		},
		`[\d\:\-\.\+TZ]+ AUDIT: id="[\w-]+" stage="" ip="<unknown>" method="" user="<none>" groups="<none>" as="<self>" asgroups="<lookup>" namespace="<none>" uri="" response="<deferred>"`,
	},
}

type fakeWriter struct {
	got int
	wg  *sync.WaitGroup
}

func (f *fakeWriter) Write(p []byte) (n int, err error) {
	f.got++
	if f.wg != nil {
		f.wg.Done()
	}
	return len(p), nil
}

func newLog(t *testing.T, format string, groupVersion schema.GroupVersion) (*bufferedBackend, *fakeWriter) {
	f := &fakeWriter{}

	backend, err := pluginlog.NewBackend(f, format, groupVersion)
	require.NoError(t, err, "initializing backend")

	backend = NewBackend(backend)
	return backend.(*bufferedBackend), f
}

func newWgLog(t *testing.T, format string, groupVersion schema.GroupVersion, wg *sync.WaitGroup) (*bufferedBackend, *fakeWriter) {
	f := &fakeWriter{wg: wg}

	backend, err := pluginlog.NewBackend(f, format, groupVersion)
	require.NoError(t, err, "initializing backend")

	backend = NewBackend(backend)
	return backend.(*bufferedBackend), f
}

func TestBatchLogMaxEvents(t *testing.T) {
	nRest := 10
	events := make([]*auditinternal.Event, defaultBatchMaxSize+nRest) // greater than max size.
	for i := range events {
		events[i] = &auditinternal.Event{}
	}

	backend, f := newLog(t, pluginlog.FormatJson, auditv1beta1.SchemeGroupVersion)
	backend.ProcessEvents(events...)

	stopCh := make(chan struct{})
	timer := make(chan time.Time, 1)

	backend.backend.ProcessEvents(backend.collectEvents(stopCh, timer)...)
	require.Equal(t, defaultBatchMaxSize, f.got, "did not get batch max size")

	go func() {
		waitForEmptyBuffer(backend) // wait for the buffer to empty
		timer <- time.Now()         // Trigger the wait timeout
	}()

	backend.backend.ProcessEvents(backend.collectEvents(stopCh, timer)...)
	require.Equal(t, defaultBatchMaxSize+nRest, f.got, "failed to get the rest of the events")
}

func TestBatchLogStopCh(t *testing.T) {
	events := make([]*auditinternal.Event, 1) // less than max size.
	for i := range events {
		events[i] = &auditinternal.Event{}
	}

	expected := len(events)

	backend, f := newLog(t, pluginlog.FormatJson, auditv1beta1.SchemeGroupVersion)
	backend.ProcessEvents(events...)

	stopCh := make(chan struct{})
	timer := make(chan time.Time)

	go func() {
		waitForEmptyBuffer(backend)
		close(stopCh) // stop channel has stopped
	}()
	backend.backend.ProcessEvents(backend.collectEvents(stopCh, timer)...)
	require.Equal(t, expected, f.got, "get queued events after timer expires")
}

func TestBatchLogProcessEventsAfterStop(t *testing.T) {
	events := make([]*auditinternal.Event, 1) // less than max size.
	for i := range events {
		events[i] = &auditinternal.Event{}
	}

	backend, _ := newLog(t, pluginlog.FormatJson, auditv1beta1.SchemeGroupVersion)
	stopCh := make(chan struct{})

	backend.Run(stopCh)
	close(stopCh)
	<-backend.shutdownCh
	backend.ProcessEvents(events...)
	assert.Equal(t, 0, len(backend.buffer), "processed events after the backed has been stopped")
}

type fakeShutdownWriter struct {
	got int
	wg  *sync.WaitGroup
}

func (f *fakeShutdownWriter) Write(p []byte) (n int, err error) {
	f.got++
	if f.wg != nil {
		f.wg.Wait()
	}
	return len(p), nil
}

func newTestShutdownLog(t *testing.T, format string, groupVersion schema.GroupVersion, wg *sync.WaitGroup) (*bufferedBackend, *fakeShutdownWriter) {
	f := &fakeShutdownWriter{wg: wg}

	backend, err := pluginlog.NewBackend(f, format, groupVersion)
	require.NoError(t, err, "initializing backend")

	backend = NewBackend(backend)
	return backend.(*bufferedBackend), f
}

func TestBatchLogShutdown(t *testing.T) {
	events := make([]*auditinternal.Event, 1)
	for i := range events {
		events[i] = &auditinternal.Event{}
	}

	shutdownCh := make(chan struct{})
	wg := new(sync.WaitGroup)
	wg.Add(1)
	backend, f := newTestShutdownLog(t, pluginlog.FormatJson, auditv1beta1.SchemeGroupVersion, wg)
	backend.ProcessEvents(events...)

	go func() {
		// Assume stopCh was closed.
		close(backend.buffer)
		backend.processEvents(backend.collectLastEvents())
	}()
	for f.got == 0 {
		time.Sleep(time.Millisecond)
	}
	go func() {
		close(backend.shutdownCh)
		backend.Shutdown()
		close(shutdownCh)
	}()

	// Wait for some time in case there's a bug that allows for the Shutdown
	// method to exit before all requests has been completed.
	time.Sleep(1 * time.Second)
	select {
	case <-shutdownCh:
		t.Fatal("Backend shut down before all requests finished")
	default:
		// Continue.
	}
	wg.Done()
	<-shutdownCh
}

func TestBatchLogEmptyBuffer(t *testing.T) {
	events := make([]*auditinternal.Event, 1) // less than max size.
	for i := range events {
		events[i] = &auditinternal.Event{}
	}

	expected := len(events)

	backend, f := newLog(t, pluginlog.FormatJson, auditv1beta1.SchemeGroupVersion)

	stopCh := make(chan struct{})
	timer := make(chan time.Time, 1)

	timer <- time.Now() // Timer is done.

	// Buffer is empty, no events have been queued. This should exit but send no events.
	backend.processEvents(backend.collectEvents(stopCh, timer))

	// Send additional events after the sendBatchEvents has been called.
	backend.ProcessEvents(events...)
	go func() {
		waitForEmptyBuffer(backend)
		timer <- time.Now()
	}()

	backend.backend.ProcessEvents(backend.collectEvents(stopCh, timer)...)

	// Make sure we didn't get a POST with zero events.
	require.Equal(t, expected, f.got, "expected one event")
}

func TestBatchLogBufferFull(t *testing.T) {
	events := make([]*auditinternal.Event, defaultBatchBufferSize+1) // More than buffered size
	for i := range events {
		events[i] = &auditinternal.Event{}
	}

	backend, _ := newLog(t, pluginlog.FormatJson, auditv1beta1.SchemeGroupVersion)
	// Make sure this doesn't block.
	backend.ProcessEvents(events...)
}

func TestBatchLogRun(t *testing.T) {

	// Divisable by max batch size so we don't have to wait for a minute for
	// the test to finish.
	events := make([]*auditinternal.Event, defaultBatchMaxSize*3)
	for i := range events {
		events[i] = &auditinternal.Event{}
	}

	got := new(int64)
	want := len(events)

	wg := new(sync.WaitGroup)
	wg.Add(want)
	done := make(chan struct{})

	go func() {
		wg.Wait()
		// When the expected number of events have been received, close the channel.
		close(done)
	}()

	stopCh := make(chan struct{})
	defer close(stopCh)

	backend, _ := newWgLog(t, pluginlog.FormatJson, auditv1beta1.SchemeGroupVersion, wg)

	// Test the Run codepath. E.g. that the spawned goroutines behave correctly.
	backend.Run(stopCh)

	backend.ProcessEvents(events...)

	select {
	case <-done:
		// Received all the events.
	case <-time.After(2 * time.Minute):
		t.Errorf("expected %d events got %d", want, atomic.LoadInt64(got))
	}
}

func TestBatchLogConcurrentRequests(t *testing.T) {
	events := make([]*auditinternal.Event, defaultBatchBufferSize) // Don't drop events
	for i := range events {
		events[i] = &auditinternal.Event{}
	}

	wg := new(sync.WaitGroup)
	wg.Add(len(events))

	stopCh := make(chan struct{})
	defer close(stopCh)

	backend, _ := newWgLog(t, pluginlog.FormatJson, auditv1beta1.SchemeGroupVersion, wg)
	backend.Run(stopCh)

	backend.ProcessEvents(events...)
	// Wait for the webhook to receive all events.
	wg.Wait()
}
