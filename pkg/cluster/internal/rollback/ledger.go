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
	"crypto/rand"
	"encoding/hex"
	"sync"
)

// EntryState marks where a ledger entry is in its lifecycle.
type EntryState string

const (
	// StateIntent was written before the resource was actually created. A
	// rollback that only ever sees intents must tolerate the resource not
	// existing: "not found" while deleting an intent is success.
	StateIntent EntryState = "intent"
	// StateConfirmed means the create call returned successfully and the
	// resource is believed to exist.
	StateConfirmed EntryState = "confirmed"
)

// LedgerEntry is one owned resource.
type LedgerEntry struct {
	Resource Resource
	State    EntryState
}

// Ledger is the in-memory ownership record for a single create attempt.
//
// Commit timing (see DESIGN.md §2): the intent is recorded *before* the
// runtime create call, and flipped to confirmed *after* it succeeds. This
// deliberately accepts "recorded but never created" entries (the process can
// be killed between the two) because an unnecessary delete is cheap while a
// missed resource leaks and breaks the next creation.
//
// The ledger alone cannot survive a forced process exit, so every entry is
// additionally protected by an attempt-scoped label on the resource and by a
// preflight snapshot of pre-existing resources. ReconcileOrphans rebuilds the
// ledger from those signals on a subsequent run.
type Ledger struct {
	// AttemptID uniquely identifies this create invocation. It is embedded in
	// every label so two creations that reuse a resource name can never be
	// confused.
	AttemptID string
	// ClusterName is the name requested for this creation.
	ClusterName string

	mu        sync.Mutex
	entries   []LedgerEntry
	index     map[ResourceKind]map[string]int // kind -> name -> position in entries
	preflight Snapshot
}

// NewLedger builds a ledger with a fresh random attempt ID.
func NewLedger(clusterName string, preflight Snapshot) *Ledger {
	return &Ledger{
		AttemptID:   newAttemptID(),
		ClusterName: clusterName,
		entries:     []LedgerEntry{},
		index: map[ResourceKind]map[string]int{
			KindContainer: {},
			KindNetwork:   {},
			KindVolume:    {},
			KindLocalPath: {},
		},
		preflight: preflight,
	}
}

func newAttemptID() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		// crypto/rand failing is not recoverable; panic rather than silently
		// reusing IDs, which would break the ownership guarantees.
		panic("rollback: cannot generate attempt id: " + err.Error())
	}
	return hex.EncodeToString(b[:])
}

// Labels returns the ownership labels every runtime resource of this attempt
// must carry.
func (l *Ledger) Labels() map[string]string {
	return map[string]string{
		AttemptIDLabel: l.AttemptID,
		ClusterLabel:   l.ClusterName,
	}
}

// RecordIntent appends an entry in StateIntent. Recording the same resource
// twice is a no-op so callers do not need to guard helper paths.
func (l *Ledger) RecordIntent(r Resource) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if _, ok := l.index[r.Kind][r.Name]; ok {
		return
	}
	l.index[r.Kind][r.Name] = len(l.entries)
	l.entries = append(l.entries, LedgerEntry{Resource: r, State: StateIntent})
}

// MarkConfirmed flips an entry to StateConfirmed. An unknown resource is
// recorded as confirmed directly (defensive: runtimes that surface implicit
// resources only after creation).
func (l *Ledger) MarkConfirmed(r Resource) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if pos, ok := l.index[r.Kind][r.Name]; ok {
		l.entries[pos].State = StateConfirmed
		return
	}
	l.index[r.Kind][r.Name] = len(l.entries)
	l.entries = append(l.entries, LedgerEntry{Resource: r, State: StateConfirmed})
}

// Entries returns a copy of the ledger in insertion order.
func (l *Ledger) Entries() []LedgerEntry {
	l.mu.Lock()
	defer l.mu.Unlock()
	out := make([]LedgerEntry, len(l.entries))
	copy(out, l.entries)
	return out
}

// ByKind returns entries of one kind in insertion order.
func (l *Ledger) ByKind(kind ResourceKind) []LedgerEntry {
	l.mu.Lock()
	defer l.mu.Unlock()
	var out []LedgerEntry
	for _, e := range l.entries {
		if e.Resource.Kind == kind {
			out = append(out, e)
		}
	}
	return out
}

// Preflight returns the snapshot captured before creation began. Exported for
// external test packages; production code goes through InPreflight /
// OwnedByThisAttempt.
func (l *Ledger) Preflight() Snapshot {
	return l.preflight
}

// InPreflight reports whether a resource existed before this creation began.
// Such resources are never owned by this attempt and must never be deleted.
func (l *Ledger) InPreflight(r Resource) bool {
	existing, ok := l.preflight.NamesByKind(r.Kind)[r.Name]
	if !ok {
		return false
	}
	// Defensive: a same-named resource that was replaced between snapshot and
	// now carries our attempt label and is therefore ours even though the name
	// was present in the snapshot. Without the label it is treated as foreign.
	if existing.Labels[AttemptIDLabel] == l.AttemptID {
		return false
	}
	return true
}

// OwnedByThisAttempt is the final ownership gate used by rollback before
// every delete. A resource is deletable only when ALL hold:
//  1. it is in the ledger (intent or confirmed), or carries this attempt's
//     label (orphan-adoption path after a lost ledger);
//  2. it was not present in the preflight snapshot (unless recreated with our
//     attempt label);
//  3. if it exists in the current snapshot, it carries this attempt's label.
func (l *Ledger) OwnedByThisAttempt(r Resource, current Snapshot) bool {
	if l.InPreflight(r) {
		return false
	}
	_, inLedger := l.index[r.Kind][r.Name]
	snapRes, inSnapshot := current.NamesByKind(r.Kind)[r.Name]
	hasAttemptLabel := inSnapshot && snapRes.Labels[AttemptIDLabel] == l.AttemptID
	if !inLedger && !hasAttemptLabel {
		return false
	}
	// A resource currently visible without our label (e.g. a pre-existing
	// same-named resource the create call failed to replace) is foreign even
	// if an intent entry exists. The intent alone is never enough to delete.
	if inSnapshot && !hasAttemptLabel {
		return false
	}
	return true
}

// ReconcileOrphans rebuilds ownership after the in-memory ledger was lost
// (forced process termination, daemon restart, ...). It adopts every runtime
// resource that carries this attempt's label, and every same-named resource
// the caller asks about that is absent from both the preflight snapshot and
// the current runtime view (an intent whose create never happened — nothing to
// do at runtime, but keeping the entry makes the inventory complete).
//
// Resources that only match by cluster/name but lack the attempt label are
// deliberately NOT adopted: they belong to an older creation or to a
// pre-existing cluster and must survive.
func (l *Ledger) ReconcileOrphans(ctx context.Context, rt Runtime) error {
	current, err := rt.Snapshot(ctx)
	if err != nil {
		return err
	}
	for _, list := range [][]Resource{current.Containers, current.Networks, current.Volumes} {
		for _, r := range list {
			if r.Labels[AttemptIDLabel] != l.AttemptID {
				continue
			}
			if l.InPreflight(r) {
				continue
			}
			l.MarkConfirmed(r)
		}
	}
	return nil
}
