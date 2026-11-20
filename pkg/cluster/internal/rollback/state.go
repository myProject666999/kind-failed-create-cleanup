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
	"encoding/json"
	"os"
	"path/filepath"
)

// StateFileName is the ledger snapshot written inside the cluster state
// directory. It is the cross-process half of the ownership story:
//
//   - the in-memory ledger drives rollback within the creating process;
//   - this file lets a *later* process reconstruct the exact same ledger
//     (same AttemptID, same preflight baseline) after the creator was killed,
//     so attempt-label matching in ReconcileOrphans keeps working.
//
// The file is created before any runtime resource exists and removed as the
// last step of a successful rollback. Its mere presence on a later run means
// "a creation never finished" and is what distinguishes killed-creation
// orphans from a healthy pre-existing same-name cluster.
const StateFileName = "creation-state.json"

// persistedState is the on-disk representation of a Ledger.
type persistedState struct {
	AttemptID   string        `json:"attemptId"`
	ClusterName string        `json:"clusterName"`
	Preflight   Snapshot      `json:"preflight"`
	Entries     []LedgerEntry `json:"entries"`
}

// Persist writes the ledger state into dir/StateFileName. The directory must
// already exist (it is itself one of the ledger's tracked local paths, created
// first in provision order).
func (l *Ledger) Persist(dir string) error {
	l.mu.Lock()
	state := persistedState{
		AttemptID:   l.AttemptID,
		ClusterName: l.ClusterName,
		Preflight:   l.preflight,
		Entries:     append([]LedgerEntry(nil), l.entries...),
	}
	l.mu.Unlock()

	data, err := json.MarshalIndent(state, "", "  ")
	if err != nil {
		return err
	}
	path := filepath.Join(dir, StateFileName)
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, data, 0o600); err != nil {
		return err
	}
	return os.Rename(tmp, path) // atomic replace
}

// LoadLedger reads a persisted ledger. A missing file returns an error
// wrapping os.ErrNotExist so callers can distinguish "no unfinished creation"
// from corruption.
func LoadLedger(path string) (*Ledger, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var state persistedState
	if err := json.Unmarshal(data, &state); err != nil {
		return nil, err
	}
	l := &Ledger{
		AttemptID:   state.AttemptID,
		ClusterName: state.ClusterName,
		entries:     append([]LedgerEntry(nil), state.Entries...),
		preflight:   state.Preflight,
		index: map[ResourceKind]map[string]int{
			KindContainer: {},
			KindNetwork:   {},
			KindVolume:    {},
			KindLocalPath: {},
		},
	}
	for i, e := range l.entries {
		l.index[e.Resource.Kind][e.Resource.Name] = i
	}
	return l, nil
}

// RemoveState deletes a persisted ledger. A missing file is success.
func RemoveState(dir string) error {
	err := os.Remove(filepath.Join(dir, StateFileName))
	if err != nil && !os.IsNotExist(err) {
		return err
	}
	return nil
}

// RecoverOrphans is the cross-process entry point used after a forced kill:
// it loads the persisted ledger, re-adopts every runtime resource carrying the
// persisted attempt label, and returns the rebuilt ledger ready for
// Rollbacker.Rollback.
//
// Resources are adopted by *attempt label*, never by name alone: a same-named
// resource that existed before the killed creation (and is therefore listed in
// the persisted preflight baseline) stays foreign even if it shares a name.
func RecoverOrphans(ctx context.Context, rt Runtime, statePath string) (*Ledger, error) {
	l, err := LoadLedger(statePath)
	if err != nil {
		return nil, err
	}
	if err := l.ReconcileOrphans(ctx, rt); err != nil {
		return nil, err
	}
	return l, nil
}
