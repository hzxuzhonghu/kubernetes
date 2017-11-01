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

// Package buffered implements the audit.Backend interface with buffered channel.
package buffered

import (
	"errors"
	"sync"
	"time"

	"k8s.io/apimachinery/pkg/util/runtime"
	auditinternal "k8s.io/apiserver/pkg/apis/audit"
	"k8s.io/apiserver/pkg/audit"
	"k8s.io/client-go/util/flowcontrol"
)

const (
	// Default configuration values for buffered backend.
	//
	// TODO(ericchiang): Make these value configurable. Maybe through a
	// kubeconfig extension?
	defaultBatchBufferSize = 10000            // Buffer up to 10000 events before starting discarding.
	defaultBatchMaxSize    = 400              // Only consume up to 400 events at a time.
	defaultBatchMaxWait    = 30 * time.Second // Consume events at least twice a minute.

	defaultBatchThrottleQPS   = 10 // Limit the consume rate by 10 QPS.
	defaultBatchThrottleBurst = 15 // Allow up to 15 QPS burst.
)

// The plugin name reported in error metrics.
const pluginName = "buffered"

// NewBackend returns an audit buffered backend.
func NewBackend(backend audit.Backend) audit.Backend {
	return &bufferedBackend{
		backend:      backend,
		buffer:       make(chan *auditinternal.Event, defaultBatchBufferSize),
		maxBatchSize: defaultBatchMaxSize,
		maxBatchWait: defaultBatchMaxWait,
		shutdownCh:   make(chan struct{}),
		throttle:     flowcontrol.NewTokenBucketRateLimiter(defaultBatchThrottleQPS, defaultBatchThrottleBurst),
	}
}

type bufferedBackend struct {
	backend audit.Backend
	// Channel to buffer events before underlying backend processing.
	buffer chan *auditinternal.Event
	// Maximum number of events that can be consumed at once.
	maxBatchSize int
	// Amount of time to wait after consuming events before force consuming another set.
	//
	// Receiving maxBatchSize events will always trigger a consume, regardless of
	// if this amount of time has been reached.
	maxBatchWait time.Duration

	// Channel to signal that the worker routine to stop and the buffer channel to close.
	stopCh <-chan struct{}

	// Channel to signal that the worker routine has stopped and therefore
	// it's safe to assume that no new requests will be initiated.
	shutdownCh chan struct{}

	// The consuming routine locks reqMutex for reading before initiating a new
	// goroutine to consume a request. This goroutine then unlocks reqMutex for
	// reading when completed. The Shutdown method locks reqMutex for writing
	// after the consuming routine has exited. When reqMutex is locked for writing,
	// all requests have been completed and no new will be spawned, since the
	// working routine is not running anymore.
	reqMutex sync.RWMutex

	// Limits the number of requests sent to the backend per second.
	throttle flowcontrol.RateLimiter
}

var _ audit.Backend = &bufferedBackend{}

func (b *bufferedBackend) Run(stopCh <-chan struct{}) error {
	b.stopCh = stopCh
	go func() {
		// Signal that the working routine has exited.
		defer close(b.shutdownCh)

		b.runWorkerRoutine()

		// Handle the events that were received after the last buffer
		// scraping and before this line. Since the buffer is closed, no new
		// events will come through.
		for {
			if last := func() bool {
				// Recover from any panic in order to try to process all remaining events.
				// Note, that in case of a panic, the return value will be false and
				// the loop execution will continue.
				defer runtime.HandleCrash()

				events := b.collectLastEvents()
				b.processEvents(events)
				return len(events) == 0
			}(); last {
				break
			}
		}
	}()
	return nil
}

func (b *bufferedBackend) Shutdown() {
	<-b.shutdownCh

	// Write locking reqMutex will guarantee that all requests will be completed
	// by the time the goroutine continues the execution. Since this line is
	// executed after shutdownCh was closed, no new requests will follow this
	// lock, because read lock is called in the same goroutine that closes
	// shutdownCh before exiting.
	b.reqMutex.Lock()
	b.reqMutex.Unlock()
}

// runWorkerRoutine runs a loop that collects events from the buffer. When
// b.stopCh is closed, runWorkerRoutine stops and closes the buffer.
func (b *bufferedBackend) runWorkerRoutine() {
	defer close(b.buffer)
	t := time.NewTimer(b.maxBatchWait)
	defer t.Stop()

	for {
		func() {
			// Recover from any panics caused by this function so a panic in the
			// goroutine can't bring down the main routine.
			defer runtime.HandleCrash()
			defer t.Reset(b.maxBatchWait)

			b.processEvents(b.collectEvents(t.C))
		}()

		select {
		case <-b.stopCh:
			return
		default:
		}
	}
}

// collectEvents attempts to collect some number of events in a batch.
//
// The following things can cause collectEvents to stop and return the list
// of events:
//
//   * Some maximum number of events are received.
//   * Timer has passed, all queued events are sent.
//   * b.stopCh is closed, all queued events are sent.
//
func (b *bufferedBackend) collectEvents(timer <-chan time.Time) []*auditinternal.Event {
	var events []*auditinternal.Event

L:
	for i := 0; i < b.maxBatchSize; i++ {
		select {
		case ev, ok := <-b.buffer:
			// Buffer channel was closed and no new events will follow.
			if !ok {
				break L
			}
			events = append(events, ev)
		case <-timer:
			// Timer has expired. Send whatever events are in the queue.
			break L
		case <-b.stopCh:
			// backend has been stopped. Send the last events.
			break L
		}
	}

	return events
}

// collectLastEvents assumes that the buffer was closed. It collects the first
// maxBatchSize events from the closed buffer into a batch and returns them.
func (b *bufferedBackend) collectLastEvents() []*auditinternal.Event {
	var events []*auditinternal.Event

	for i := 0; i < b.maxBatchSize; i++ {
		ev, ok := <-b.buffer
		if !ok {
			break
		}
		events = append(events, ev)
	}

	return events
}

// Func processEvents process the batch events in a goroutine use the
// real backend interface's ProcessEvents.
func (b *bufferedBackend) processEvents(events []*auditinternal.Event) {
	if len(events) == 0 {
		return
	}

	if b.throttle != nil {
		b.throttle.Accept()
	}

	// Locking reqMutex for read will guarantee that the shutdown process will
	// block until the goroutine started below is finished. At the same time, it
	// will not prevent other batches from being proceed further this point.
	b.reqMutex.RLock()
	go func() {
		defer b.reqMutex.RUnlock()
		defer runtime.HandleCrash()

		// Execute the real processing in a goroutine to keep it from blocking.
		// This lets the backend continue to drain the queue immediately.
		b.backend.ProcessEvents(events...)
	}()
	return
}

func (b *bufferedBackend) ProcessEvents(ev ...*auditinternal.Event) {
	// The following mechanism is in place to support the situation when audit
	// events are still coming after the backend was stopped.
	var sendErr error
	var evIndex int

	// If the backend was shut down and the buffer channel was closed, an
	// attempt to add an event to it will result in panic that we should
	// recover from.
	defer func() {
		if err := recover(); err != nil {
			sendErr = errors.New("audit buffer shut down")
		}
		if sendErr != nil {
			audit.HandlePluginError(pluginName, sendErr, ev[evIndex:]...)
		}
	}()

	for i, e := range ev {
		evIndex = i
		// Per the audit.Backend interface these events are reused after being
		// sent to the Sink. Deep copy and send the copy to the queue.
		event := e.DeepCopy()

		select {
		case b.buffer <- event:
		default:
			sendErr = errors.New("audit buffer queue blocked")
			return
		}
	}
}
