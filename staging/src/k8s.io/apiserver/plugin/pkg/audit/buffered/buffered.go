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

// Package buffered provides an implementation for the audit.Backend interface
// that batches incoming audit events and sends batches to the underlying audit.Backend.
package buffered

import (
	"errors"
	"sync"
	"time"

	"k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	auditinternal "k8s.io/apiserver/pkg/apis/audit"
	"k8s.io/apiserver/pkg/audit"
	"k8s.io/client-go/util/flowcontrol"
)

const (
	// Default configuration values for buffered underlyingBackend.
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

type bufferedBackend struct {
	// The underlying underlyingBackend that actually exports events.
	underlyingBackend audit.Backend

	// Channel to buffer events before sending to the underlying backend.
	buffer chan *auditinternal.Event
	// Maximum number of events in a batch sent to the underlying backend.
	maxBatchSize int
	// Amount of time to wait after sending a batch to the underlying backend before sending another one.
	//
	// Receiving maxBatchSize events will always trigger sending a batch, regardless of the amount of time passed.
	maxBatchWait time.Duration

	// Channel to signal that the worker routine has processed all remaining events and exited.
	// Once `shutdownCh` is closed no new events will be sent to the underlying backend.
	shutdownCh chan struct{}

	// WaitGroup to control the concurrency of sending batches to the underlying backend.
	// Worker routine calls Add before sending a batch and
	// then spawns a routine that calls Done after batch was processed by the underlying backend.
	// This WaitGroup is also used to wait for all sending routines to finish before shutting down audit backend.
	wg sync.WaitGroup

	// Limits the number of batches sent to the underlying backend per second.
	throttle flowcontrol.RateLimiter
}

var _ audit.Backend = &bufferedBackend{}

func NewBackend(backend audit.Backend) audit.Backend {
	return &bufferedBackend{
		underlyingBackend: backend,
		buffer:            make(chan *auditinternal.Event, defaultBatchBufferSize),
		maxBatchSize:      defaultBatchMaxSize,
		maxBatchWait:      defaultBatchMaxWait,
		shutdownCh:        make(chan struct{}),
		wg:                sync.WaitGroup{},
		throttle:          flowcontrol.NewTokenBucketRateLimiter(defaultBatchThrottleQPS, defaultBatchThrottleBurst),
	}
}

func (b *bufferedBackend) Run(stopCh <-chan struct{}) error {
	go func() {
		// Signal that the working routine has exited.
		defer close(b.shutdownCh)

		b.processIncomingEvents(stopCh)

		// Handle the events that were received after the last buffer
		// scraping and before this line. Since the buffer is closed, no new
		// events will come through.
		for {
			if last := func() bool {
				// Recover from any panic in order to try to process all remaining events.
				// Note, that in case of a panic, the return value will be false and
				// the loop execution will continue.
				defer runtime.HandleCrash()

				events := b.collectEvents(make(chan time.Time), wait.NeverStop)
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

	// Wait blocks here until all sending routines exit.
	b.wg.Wait()
}

// processIncomingEvents runs a loop that collects events from the buffer. When
// b.stopCh is closed, processIncomingEvents stops and closes the buffer.
func (b *bufferedBackend) processIncomingEvents(stopCh <-chan struct{}) {
	defer close(b.buffer)
	t := time.NewTimer(b.maxBatchWait)
	defer t.Stop()

	for {
		func() {
			// Recover from any panics caused by this function so a panic in the
			// goroutine can't bring down the main routine.
			defer runtime.HandleCrash()

			t.Reset(b.maxBatchWait)
			b.processEvents(b.collectEvents(t.C, stopCh))
		}()

		select {
		case <-stopCh:
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
func (b *bufferedBackend) collectEvents(timer <-chan time.Time, stopCh <-chan struct{}) []*auditinternal.Event {
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
		case <-stopCh:
			// underlyingBackend has been stopped. Send the last events.
			break L
		}
	}

	return events
}

// Func processEvents process the batch events in a goroutine use the
// real underlyingBackend interface's ProcessEvents.
func (b *bufferedBackend) processEvents(events []*auditinternal.Event) {
	if len(events) == 0 {
		return
	}

	// TODO(audit): Should control the number of active goroutines
	// if one goroutine takes 5 seconds to finish, the number of goroutines can be 5 * defaultBatchThrottleQPS
	if b.throttle != nil {
		b.throttle.Accept()
	}

	b.wg.Add(1)
	go func() {
		defer b.wg.Done()
		defer runtime.HandleCrash()

		// Execute the real processing in a goroutine to keep it from blocking.
		// This lets the underlyingBackend continue to drain the queue immediately.
		b.underlyingBackend.ProcessEvents(events...)
	}()
}

func (b *bufferedBackend) ProcessEvents(ev ...*auditinternal.Event) {
	// The following mechanism is in place to support the situation when audit
	// events are still coming after the underlyingBackend was stopped.
	var sendErr error
	var evIndex int

	// If the underlyingBackend was shut down and the buffer channel was closed, an
	// attempt to add an event to it will result in panic that we should
	// recover from.
	defer func() {
		if err := recover(); err != nil {
			sendErr = errors.New("panic: audit buffer shut down, can not send event to closed buffer channel")
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
