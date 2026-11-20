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

package rollback_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"sigs.k8s.io/kind/pkg/cluster/internal/rollback"
	"sigs.k8s.io/kind/pkg/cluster/internal/rollback/fakeruntime"
)

// TestTransientNetworkDeleteRetried: the first network rm fails with rollback.ErrInUse
// because containers have not fully detached. After container removal, a
// bounded retry must succeed — rollback must not give up on the first
// transient failure nor retry forever.
func TestTransientNetworkDeleteRetried(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	// one-shot: first network removal is transiently in-use, later ones succeed
	h.rt.FailOnce(fakeruntime.OpRemoveNetwork, networkName,
		fmt.Errorf("%w: disconnect still propagating", rollback.ErrInUse))
	opts := h.baseOptions()
	opts.Initializer = failNodeAt(2)
	opts.Rollback.MaxAttempts = 4

	_, err := rollback.NewCreator(h.rt, nil).Create(context.Background(), h.spec, opts)
	if err == nil {
		t.Fatal("expected creation failure")
	}
	ce := asCreateError(t, err)
	if ce.RollbackErr != nil {
		t.Fatalf("retry should have resolved the in-use network: %v", ce.RollbackErr)
	}
	var attempts int
	for _, c := range h.rt.Calls() {
		if c.Op == fakeruntime.OpRemoveNetwork && c.Name == networkName {
			attempts++
		}
	}
	if attempts < 2 {
		t.Fatalf("expected at least 2 network removal attempts (transient + retry), got %d", attempts)
	}
	if attempts > opts.Rollback.MaxAttempts {
		t.Fatalf("retry budget exceeded: %d attempts", attempts)
	}
	h.assertAllOwnedGone()
	h.assertForeignUntouched()
}

// TestRetryBudgetExhausted: when the in-use condition never clears, rollback
// must stop after MaxAttempts and report the failure instead of looping
// without bound.
func TestRetryBudgetExhausted(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	h.rt.NetworkDeleteTransient = true
	// keep one container permanently attached so the network stays in-use
	stuck := h.spec.Nodes[0].Name
	h.rt.ContainerDeleteFailures[stuck] = errors.New("simulated daemon stall")
	opts := h.baseOptions()
	opts.Rollback.MaxAttempts = 3
	opts.Initializer = failNodeAt(2)

	start := time.Now()
	_, err := rollback.NewCreator(h.rt, nil).Create(context.Background(), h.spec, opts)
	elapsed := time.Since(start)
	if err == nil {
		t.Fatal("expected creation failure")
	}
	ce := asCreateError(t, err)
	if ce.RollbackErr == nil {
		t.Fatal("exhausted retries must be reported")
	}
	if !errors.Is(ce.RollbackErr, rollback.ErrInUse) {
		t.Fatalf("expected rollback.ErrInUse in rollback errors, got: %v", ce.RollbackErr)
	}
	// with the no-sleep hook the whole thing must return effectively instantly;
	// a hard guard catches an accidental unbounded loop.
	if elapsed > 5*time.Second {
		t.Fatalf("rollback took too long: %v", elapsed)
	}
	if h.rt.NetworkExists(networkName) != true {
		t.Fatal("in-use network should remain after budget exhaustion")
	}
	h.assertForeignUntouched()
}

// TestRollbackTimeoutHardBound: a context deadline shorter than the retry
// budget must cut cleanup off; the timeout guarantees one failed create cannot
// hang the terminal.
func TestRollbackTimeoutHardBound(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	h.rt.NetworkDeleteTransient = true
	h.rt.ContainerDeleteFailures[h.spec.Nodes[0].Name] = errors.New("stall")
	opts := h.baseOptions()
	opts.Rollback.MaxAttempts = 100
	opts.Rollback.RetryDelay = time.Millisecond
	opts.Rollback.MaxRetryDelay = time.Millisecond
	opts.Rollback.Timeout = 5 * time.Millisecond
	opts.Initializer = failNodeAt(2)

	done := make(chan error, 1)
	go func() {
		_, err := rollback.NewCreator(h.rt, nil).Create(context.Background(), h.spec, opts)
		done <- err
	}()
	select {
	case err := <-done:
		ce := asCreateError(t, err)
		if ce.RollbackErr == nil {
			t.Fatal("timeout-bounded rollback should report unresolved failures")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("rollback ignored its timeout budget")
	}
	h.assertForeignUntouched()
}

// TestSuccessfulCreateNoRollback: when every step succeeds, no delete call
// must happen and the ledger records every resource as confirmed.
func TestSuccessfulCreateNoRollback(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	opts := h.baseOptions()
	opts.Initializer = func(_ context.Context, _ rollback.Runtime, _ rollback.Resource) error { return nil }

	created, err := rollback.NewCreator(h.rt, nil).Create(context.Background(), h.spec, opts)
	if err != nil {
		t.Fatalf("unexpected failure: %v", err)
	}
	for _, c := range h.rt.Calls() {
		if strings.HasPrefix(string(c.Op), "Remove") || c.Op == fakeruntime.OpStopContainer {
			t.Fatalf("successful create made a cleanup call: %s:%s", c.Op, c.Name)
		}
	}
	if created.AttemptID == "" {
		t.Fatal("attempt id not assigned")
	}
	for _, e := range created.Ledger.Entries() {
		if e.State != rollback.StateConfirmed {
			t.Fatalf("resource %s/%s not confirmed: %s", e.Resource.Kind, e.Resource.Name, e.State)
		}
	}
	// three nodes + network + explicit volume + three anonymous volumes
	if got := h.rt.VolumeCount(); got != 5 { // 1 foreign + 1 explicit + 3 anonymous
		t.Fatalf("expected 5 volumes, got %d", got)
	}
	h.assertForeignUntouched()
}

// TestLogExportBeforeRollback: node logs are exported into a temp directory
// before any container is removed, and the path is surfaced in the error.
func TestLogExportBeforeRollback(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	for _, n := range h.spec.Nodes {
		h.rt.SetLogs(n.Name, []byte("boot logs of "+n.Name))
	}
	exporter := fakeruntime.NewLogExporter(h.rt)
	opts := h.baseOptions()
	opts.Rollback.Exporter = exporter
	opts.Rollback.LogsBaseDir = h.dir
	opts.Initializer = failNodeAt(2)

	_, err := rollback.NewCreator(h.rt, nil).Create(context.Background(), h.spec, opts)
	ce := asCreateError(t, err)
	if ce.LogsDir == "" {
		t.Fatal("logs directory not reported")
	}
	if exporter.Calls != 1 {
		t.Fatalf("expected one export call, got %d", exporter.Calls)
	}
	entries, err := os.ReadDir(ce.LogsDir)
	if err != nil {
		t.Fatalf("logs dir unreadable: %v", err)
	}
	if len(entries) != len(h.spec.Nodes) {
		t.Fatalf("expected %d node log dirs, got %d", len(h.spec.Nodes), len(entries))
	}
	data, err := os.ReadFile(filepath.Join(ce.LogsDir, h.spec.Nodes[0].Name, "logs.txt"))
	if err != nil || string(data) != "boot logs of "+h.spec.Nodes[0].Name {
		t.Fatalf("log content mismatch: %q %v", data, err)
	}
	// export happened before the first container removal
	firstRemove := len(h.rt.Calls())
	for i, c := range h.rt.Calls() {
		if c.Op == fakeruntime.OpRemoveContainer {
			firstRemove = i
			break
		}
	}
	sawLogs := false
	for _, c := range h.rt.Calls()[:firstRemove] {
		if c.Op == fakeruntime.OpContainerLogs {
			sawLogs = true
		}
	}
	if !sawLogs {
		t.Fatal("logs were not collected before container removal")
	}
	h.assertAllOwnedGone()
	h.assertForeignUntouched()
}

// TestLogExportFailureDoesNotBlockRollback: a broken exporter must be swallowed
// while cleanup proceeds normally.
func TestLogExportFailureDoesNotBlockRollback(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	exporter := fakeruntime.NewLogExporter(h.rt)
	exporter.Fail = errors.New("disk full")
	opts := h.baseOptions()
	opts.Rollback.Exporter = exporter
	opts.Rollback.LogsBaseDir = h.dir
	opts.Initializer = failNodeAt(2)

	_, err := rollback.NewCreator(h.rt, nil).Create(context.Background(), h.spec, opts)
	ce := asCreateError(t, err)
	if ce.LogsDir != "" {
		t.Fatal("failed export must not leave a directory path behind")
	}
	if ce.RollbackErr != nil {
		t.Fatalf("rollback must proceed despite export failure: %v", ce.RollbackErr)
	}
	h.assertAllOwnedGone()
}

// TestOrphanAdoptionAfterLostLedger simulates a process killed mid-creation:
// the in-memory ledger dies with the process but the persisted state file plus
// attempt labels let a fresh process rebuild the exact ledger and clean up.
func TestOrphanAdoptionAfterLostLedger(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	statePath := filepath.Join(h.spec.StateDir, rollback.StateFileName)

	// --- "first process": create resources, persisting after every step, then
	// drop every in-memory reference as though the process was SIGKILLed.
	if err := os.MkdirAll(h.spec.StateDir, 0o755); err != nil {
		t.Fatal(err)
	}
	preflight, err := h.rt.Snapshot(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	func() {
		ledger := rollback.NewLedger(clusterName, preflight)
		if err := ledger.Persist(h.spec.StateDir); err != nil {
			t.Fatal(err)
		}
		netRes := rollback.Resource{Kind: rollback.KindNetwork, Name: networkName, Labels: ledger.Labels()}
		ledger.RecordIntent(netRes)
		if err := ledger.Persist(h.spec.StateDir); err != nil {
			t.Fatal(err)
		}
		if _, err := h.rt.CreateNetwork(context.Background(), networkName, ledger.Labels()); err != nil {
			t.Fatal(err)
		}
		ledger.MarkConfirmed(netRes)
		volRes := rollback.Resource{Kind: rollback.KindVolume, Name: volumeName, Labels: ledger.Labels()}
		ledger.RecordIntent(volRes)
		if _, err := h.rt.CreateVolume(context.Background(), volumeName, ledger.Labels()); err != nil {
			t.Fatal(err)
		}
		ledger.MarkConfirmed(volRes)
		for _, n := range h.spec.Nodes {
			r := rollback.Resource{Kind: rollback.KindContainer, Name: n.Name, Labels: ledger.Labels()}
			ledger.RecordIntent(r)
			if _, err := h.rt.CreateContainer(context.Background(), rollback.ContainerSpec{
				Name: n.Name, Image: n.Image, Network: networkName, Labels: ledger.Labels(),
			}); err != nil {
				t.Fatal(err)
			}
			ledger.MarkConfirmed(r)
		}
		if err := ledger.Persist(h.spec.StateDir); err != nil {
			t.Fatal(err)
		}
	}() // ledger goes out of scope: the process is now "dead"

	// the state file must exist on disk carrying the original attempt id
	loaded, err := rollback.LoadLedger(statePath)
	if err != nil {
		t.Fatalf("persisted ledger missing: %v", err)
	}

	// --- "second process": no in-memory ledger. Recovery loads the file and
	// adopts runtime resources by the persisted attempt label.
	recovered, err := rollback.RecoverOrphans(context.Background(), h.rt, statePath)
	if err != nil {
		t.Fatalf("recovery failed: %v", err)
	}
	if recovered.AttemptID != loaded.AttemptID {
		t.Fatalf("attempt id mismatch: %q != %q", recovered.AttemptID, loaded.AttemptID)
	}
	ownedKinds := map[rollback.ResourceKind]int{}
	for _, e := range recovered.Entries() {
		ownedKinds[e.Resource.Kind]++
	}
	if ownedKinds[rollback.KindNetwork] != 1 || ownedKinds[rollback.KindContainer] != 3 ||
		ownedKinds[rollback.KindVolume] < 1 {
		t.Fatalf("orphan adoption incomplete: %v", ownedKinds)
	}

	// rollback through the rebuilt ledger cleans the orphans
	rb := rollback.NewRollbacker(h.rt, rollback.Options{
		RetryDelay: time.Millisecond, MaxRetryDelay: time.Millisecond,
		Sleep: h.noSleep,
	})
	res := rb.Rollback(context.Background(), recovered)
	if res.Failures != nil {
		t.Fatalf("orphan cleanup failed: %v", res.Failures)
	}
	if h.rt.NetworkExists(networkName) || h.rt.ContainerCount() != 1 || h.rt.NetworkCount() != 1 {
		t.Fatal("orphaned resources were not fully cleaned")
	}
	h.assertForeignUntouched()
}

// TestUnlabeledSameNameResourceNeverAdopted: an orphan adoption scan must not
// touch a same-named resource lacking the attempt label (an older cluster's
// resource that happens to share the name).
func TestUnlabeledSameNameResourceNeverAdopted(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	h.rt.SeedNetwork(networkName) // same name as our spec, but unlabeled
	preflight, _ := h.rt.Snapshot(context.Background())
	ledger := rollback.NewLedger(clusterName, preflight)
	ledger.RecordIntent(rollback.Resource{Kind: rollback.KindNetwork, Name: networkName}) // intent exists...

	if err := ledger.ReconcileOrphans(context.Background(), h.rt); err != nil {
		t.Fatal(err)
	}
	rb := rollback.NewRollbacker(h.rt, rollback.Options{
		RetryDelay: time.Millisecond, MaxRetryDelay: time.Millisecond,
		Sleep: h.noSleep,
	})
	res := rb.Rollback(context.Background(), ledger)
	if res.Failures != nil {
		t.Fatalf("foreign network should never be attempted: %v", res.Failures)
	}
	if !h.rt.NetworkExists(networkName) {
		t.Fatal("unlabeled same-name network was wrongly deleted")
	}
	for _, c := range h.rt.Calls() {
		if c.Op == fakeruntime.OpRemoveNetwork && c.Name == networkName {
			t.Fatal("delete attempted against unlabeled same-name resource")
		}
	}
}

// TestPreExistingNetworkReusedAndNotDeleted: a network present before creation
// is treated as a shared resource — reused without ownership and left in place
// after a failed create.
func TestPreExistingNetworkReusedAndNotDeleted(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	h.rt.SeedNetwork(networkName)
	opts := h.baseOptions()
	opts.Initializer = failNodeAt(2)

	_, err := rollback.NewCreator(h.rt, nil).Create(context.Background(), h.spec, opts)
	asCreateError(t, err)
	if !h.rt.NetworkExists(networkName) {
		t.Fatal("pre-existing network was deleted by rollback")
	}
	for _, c := range h.rt.Calls() {
		if c.Op == fakeruntime.OpCreateNetwork && c.Name == networkName {
			t.Fatal("pre-existing network should be reused, not recreated")
		}
		if c.Op == fakeruntime.OpRemoveNetwork && c.Name == networkName {
			t.Fatal("pre-existing network must never be removed")
		}
	}
	// containers still get cleaned
	for _, n := range h.spec.Nodes {
		if h.rt.ContainerExists(n.Name) {
			t.Fatalf("owned container %s not cleaned", n.Name)
		}
	}
	h.assertForeignUntouched()
}

// TestContextCancellationTriggersRollback: cancelling the creation context
// stops the in-flight step and still runs full cleanup.
func TestContextCancellationTriggersRollback(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	ctx, cancel := context.WithCancel(context.Background())
	var initialized int32
	opts := h.baseOptions()
	opts.Initializer = func(ctx context.Context, _ rollback.Runtime, node rollback.Resource) error {
		// the first node initializes normally; on the second node we cancel and
		// simulate an in-flight step that only unblocks via the context
		if atomic.AddInt32(&initialized, 1) == 1 {
			return nil
		}
		cancel() // interrupt during the second node's bootstrap
		<-ctx.Done()
		return ctx.Err()
	}

	_, err := rollback.NewCreator(h.rt, nil).Create(ctx, h.spec, opts)
	if err == nil {
		t.Fatal("expected cancellation failure")
	}
	ce := asCreateError(t, err)
	if ce.RollbackErr != nil {
		t.Fatalf("post-cancel rollback should be clean: %v", ce.RollbackErr)
	}
	h.assertAllOwnedGone()
	h.assertForeignUntouched()
}

// TestIntentWithoutResourceIsTolerated: a ledger entry whose resource was
// never created (process killed between RecordIntent and the create call)
// must produce a not-found delete that rollback counts as success.
func TestIntentWithoutResourceIsTolerated(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	preflight, _ := h.rt.Snapshot(context.Background())
	ledger := rollback.NewLedger(clusterName, preflight)
	ledger.RecordIntent(rollback.Resource{Kind: rollback.KindNetwork, Name: "never-created-net"})
	ledger.RecordIntent(rollback.Resource{Kind: rollback.KindVolume, Name: "never-created-vol"})
	ledger.RecordIntent(rollback.Resource{Kind: rollback.KindContainer, Name: "never-created-node"})

	rb := rollback.NewRollbacker(h.rt, rollback.Options{
		RetryDelay: time.Millisecond, MaxRetryDelay: time.Millisecond,
		Sleep: h.noSleep,
	})
	res := rb.Rollback(context.Background(), ledger)
	if res.Failures != nil {
		t.Fatalf("missing intents must be tolerated as not-found: %v", res.Failures)
	}
	// every not-found must still have been attempted exactly once
	counts := map[string]int{}
	for _, c := range h.rt.Calls() {
		counts[string(c.Op)+":"+c.Name]++
	}
	for _, want := range []string{
		"RemoveNetwork:never-created-net",
		"RemoveVolume:never-created-vol",
		"RemoveContainer:never-created-node",
	} {
		if counts[want] != 1 {
			t.Fatalf("expected exactly one attempt for %s, got %d", want, counts[want])
		}
	}
}

// TestContainerCreateFailureMidway: the second container fails to create. The
// first container, the network, volumes (explicit + the first container's
// anonymous one) and local paths must all roll back cleanly.
func TestContainerCreateFailureMidway(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	h.rt.Fail(fakeruntime.OpCreateContainer, h.spec.Nodes[1].Name,
		errors.New("create rejected: simulated runtime reject"))
	opts := h.baseOptions()

	_, err := rollback.NewCreator(h.rt, nil).Create(context.Background(), h.spec, opts)
	if err == nil {
		t.Fatal("expected creation failure")
	}
	ce := asCreateError(t, err)
	if ce.RollbackErr != nil {
		t.Fatalf("rollback should be clean: %v", ce.RollbackErr)
	}
	h.assertRollbackOrder()
	h.assertAllOwnedGone()
	h.assertForeignUntouched()
}

// TestAggregateErrorPreservesSentinels proves errors.Is works through both the
// multiError aggregate and the wrapping rollback.CreateError.
func TestAggregateErrorPreservesSentinels(t *testing.T) {
	t.Parallel()
	agg := rollback.Aggregate("x", []error{
		fmt.Errorf("wrap: %w", rollback.ErrInUse),
		fmt.Errorf("wrap: %w", rollback.ErrNotFound),
	})
	if !errors.Is(agg, rollback.ErrInUse) || !errors.Is(agg, rollback.ErrNotFound) {
		t.Fatal("sentinels lost in aggregate")
	}
	ce := &rollback.CreateError{
		CreateErr:   errors.New("init failed"),
		RollbackErr: agg,
	}
	if !errors.Is(ce, ce.CreateErr) {
		t.Fatal("creation error must remain the causal root")
	}
	var extracted interface {
		error
		Unwrap() []error
	}
	if !errors.As(ce, &extracted) || !errors.Is(extracted, rollback.ErrInUse) {
		t.Fatal("rollback aggregate must be reachable through errors.As")
	}
}

// TestStateFileLifecycle: the persisted ledger exists while creation is in
// flight, disappears after a clean rollback, and is intentionally kept when
// rollback leaves failures or when retain is requested.
func TestStateFileLifecycle(t *testing.T) {
	t.Parallel()

	t.Run("removed after clean rollback", func(t *testing.T) {
		t.Parallel()
		h := newHarness(t)
		opts := h.baseOptions()
		opts.Initializer = failNodeAt(2)
		if _, err := rollback.NewCreator(h.rt, nil).Create(context.Background(), h.spec, opts); err == nil {
			t.Fatal("expected failure")
		}
		if _, err := os.Stat(h.statePath()); !os.IsNotExist(err) {
			t.Fatalf("state file should be removed after clean rollback, err=%v", err)
		}
	})

	t.Run("absent after successful create", func(t *testing.T) {
		t.Parallel()
		h := newHarness(t)
		opts := h.baseOptions()
		opts.Initializer = func(context.Context, rollback.Runtime, rollback.Resource) error { return nil }
		if _, err := rollback.NewCreator(h.rt, nil).Create(context.Background(), h.spec, opts); err != nil {
			t.Fatalf("unexpected failure: %v", err)
		}
		if _, err := os.Stat(h.statePath()); !os.IsNotExist(err) {
			t.Fatalf("state file must not survive a successful creation, err=%v", err)
		}
	})

	t.Run("kept when retain is set", func(t *testing.T) {
		t.Parallel()
		h := newHarness(t)
		opts := h.baseOptions()
		opts.Retain = true
		opts.Initializer = failNodeAt(2)
		if _, err := rollback.NewCreator(h.rt, nil).Create(context.Background(), h.spec, opts); err == nil {
			t.Fatal("expected failure")
		}
		if _, err := os.Stat(h.statePath()); err != nil {
			t.Fatalf("retain mode must keep the recovery trail: %v", err)
		}
		// and that trail is usable for a later cleanup
		recovered, err := rollback.RecoverOrphans(context.Background(), h.rt, h.statePath())
		if err != nil {
			t.Fatalf("retained state must be recoverable: %v", err)
		}
		if len(recovered.Entries()) == 0 {
			t.Fatal("recovered ledger is empty")
		}
	})

	t.Run("kept after partial rollback failure", func(t *testing.T) {
		t.Parallel()
		h := newHarness(t)
		h.rt.ContainerDeleteFailures[h.spec.Nodes[0].Name] = errors.New("stuck")
		opts := h.baseOptions()
		opts.Rollback.MaxAttempts = 1
		opts.Initializer = failNodeAt(2)
		_, err := rollback.NewCreator(h.rt, nil).Create(context.Background(), h.spec, opts)
		asCreateError(t, err)
		if _, err := os.Stat(h.statePath()); err != nil {
			t.Fatalf("state file should remain for a second recovery pass: %v", err)
		}
	})
}

// TestLoadLedgerMissingFile: recovery reports os.ErrNotExist when no
// unfinished creation exists, so callers can distinguish "nothing to do".
func TestLoadLedgerMissingFile(t *testing.T) {
	t.Parallel()
	_, err := rollback.LoadLedger(filepath.Join(t.TempDir(), "absent.json"))
	if !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("expected os.ErrNotExist, got %v", err)
	}
}
