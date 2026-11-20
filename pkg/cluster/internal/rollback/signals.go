/*
Copyright 2026 The Kubernetes Authors.

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

package rollback

import (
	"context"
	"os"
	"os/signal"
)

// SignalPolicy defines what happens when the process is interrupted during
// creation.
type SignalPolicy struct {
	// Signals watched by the watcher. Defaults to os.Interrupt and SIGTERM.
	Signals []os.Signal
	// OnSecondSignal is invoked if another signal arrives while rollback is
	// running. Production wiring passes os.Exit(130); tests inject a hook so
	// the test process itself is not killed.
	OnSecondSignal func()
}

// signalWatcher connects OS signals to the creation/rollback lifecycle.
//
// Semantics (see DESIGN.md §7):
//   - first signal: cancel the creation context. The currently running step
//     unwinds, and rollback starts with a fresh, time-budgeted context so a
//     signal cannot itself prevent cleanup;
//   - second signal: invoke OnSecondSignal (force exit). Rollback cannot be
//     made safe against SIGKILL in any case; that residual risk is covered by
//     labels + ReconcileOrphans on the next run.
type signalWatcher struct {
	ch     chan os.Signal
	cancel context.CancelFunc
	policy SignalPolicy
}

// watchSignals starts a watcher bound to cancel. Stop must be called when
// creation finishes (including after rollback).
func watchSignals(cancel context.CancelFunc, policy SignalPolicy) *signalWatcher {
	if len(policy.Signals) == 0 {
		policy.Signals = []os.Signal{os.Interrupt, sigTerm}
	}
	ch := make(chan os.Signal, 2)
	signal.Notify(ch, policy.Signals...)
	w := &signalWatcher{ch: ch, cancel: cancel, policy: policy}
	go w.loop()
	return w
}

func (w *signalWatcher) loop() {
	first := true
	for range w.ch {
		if first {
			first = false
			w.cancel()
			continue
		}
		// second signal: stop waiting and get out. The hook exists instead of
		// a direct os.Exit so tests can observe the behavior deterministically.
		if w.policy.OnSecondSignal != nil {
			w.policy.OnSecondSignal()
			return
		}
	}
}

// Stop detaches from the watched signals.
func (w *signalWatcher) Stop() {
	signal.Stop(w.ch)
	close(w.ch)
}
