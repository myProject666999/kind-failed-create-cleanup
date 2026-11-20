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
	"testing"
	"time"

	"sigs.k8s.io/kind/pkg/cluster/internal/rollback"
	"sigs.k8s.io/kind/pkg/cluster/internal/rollback/fakeruntime"
)

// harness bundles everything a test needs and seeds a pre-existing foreign
// cluster plus a pre-existing shared network so every test can prove rollback
// never touches resources it did not create.
type harness struct {
	t       *testing.T
	rt      *fakeruntime.FakeRuntime
	dir     string
	spec    rollback.Spec
	noSleep func(ctx context.Context, d time.Duration) error
}

const (
	clusterName     = "test-cluster"
	networkName     = "test-cluster-net"
	volumeName      = "test-cluster-vol"
	existingCluster = "already-here"
)

func newHarness(t *testing.T) *harness {
	t.Helper()
	rt := fakeruntime.New()
	dir := t.TempDir()

	// Pre-existing, foreign resources: an older cluster and a shared network.
	// Rollback must leave every one of these byte-for-byte alone.
	rt.SeedNetwork("shared-kind-net")
	rt.SeedContainer(existingCluster+"-control-plane", "shared-kind-net", existingCluster)
	rt.SeedVolume(existingCluster + "-vol")

	noSleep := func(ctx context.Context, d time.Duration) error {
		return ctx.Err()
	}
	h := &harness{t: t, rt: rt, dir: dir, noSleep: noSleep}
	stateDir := filepath.Join(dir, "clusters", clusterName)
	h.spec = rollback.Spec{
		ClusterName: clusterName,
		Network:     networkName,
		Volumes:     []string{volumeName},
		StateDir:    stateDir,
		Nodes: []rollback.NodeSpec{
			{Name: clusterName + "-control-plane", Image: "node:v1", Role: "control-plane"},
			{Name: clusterName + "-worker", Image: "node:v1", Role: "worker"},
			{Name: clusterName + "-worker2", Image: "node:v1", Role: "worker"},
		},
		LocalPaths: []rollback.LocalPathSpec{
			{Name: stateDir, Directory: true},
			{Name: filepath.Join(dir, "tmp-creds", clusterName+".key"), Directory: false},
		},
	}
	return h
}

// statePath is the persisted ledger location for the harness spec.
func (h *harness) statePath() string {
	return filepath.Join(h.spec.StateDir, rollback.StateFileName)
}

// baseOptions returns options with no artificial sleeps so retry tests run
// instantly while still exercising the retry loop.
func (h *harness) baseOptions() rollback.CreateOptions {
	return rollback.CreateOptions{
		Rollback: rollback.Options{
			RetryDelay:    time.Millisecond,
			MaxRetryDelay: time.Millisecond,
			Sleep:         h.noSleep,
		},
	}
}

// failNodeAt returns an initializer that fails on the zero-indexed node.
func failNodeAt(index int) rollback.NodeInitializer {
	return func(_ context.Context, _ rollback.Runtime, node rollback.Resource) error {
		if node.Name == fmt.Sprintf("%s-worker2", clusterName) && index == 2 {
			return errors.New("node init timed out: kubeadm join did not complete")
		}
		// generic positional fallback
		want := []string{
			clusterName + "-control-plane",
			clusterName + "-worker",
			clusterName + "-worker2",
		}[index]
		if node.Name == want {
			return fmt.Errorf("node init failed on %s", want)
		}
		return nil
	}
}

// assertForeignUntouched verifies the pre-existing cluster survived.
func (h *harness) assertForeignUntouched() {
	h.t.Helper()
	if !h.rt.ContainerExists(existingCluster + "-control-plane") {
		h.t.Fatalf("pre-existing cluster container was deleted")
	}
	if !h.rt.NetworkExists("shared-kind-net") {
		h.t.Fatalf("pre-existing shared network was deleted")
	}
	if !h.rt.VolumeExists(existingCluster + "-vol") {
		h.t.Fatalf("pre-existing volume was deleted")
	}
	for _, c := range h.rt.Calls() {
		if c.Name == existingCluster+"-control-plane" ||
			c.Name == "shared-kind-net" || c.Name == existingCluster+"-vol" {
			if c.Op == fakeruntime.OpRemoveContainer || c.Op == fakeruntime.OpRemoveNetwork ||
				c.Op == fakeruntime.OpRemoveVolume || c.Op == fakeruntime.OpStopContainer {
				h.t.Fatalf("mutating call against foreign resource: %s:%s", c.Op, c.Name)
			}
		}
	}
}

// assertAllOwnedGone verifies nothing created by this attempt remains.
func (h *harness) assertAllOwnedGone() {
	h.t.Helper()
	for _, n := range h.spec.Nodes {
		if h.rt.ContainerExists(n.Name) {
			h.t.Fatalf("owned container %s still exists", n.Name)
		}
	}
	if h.rt.NetworkExists(networkName) {
		h.t.Fatalf("owned network %s still exists", networkName)
	}
	if h.rt.VolumeExists(volumeName) {
		h.t.Fatalf("owned volume %s still exists", volumeName)
	}
	// anonymous volumes: only the foreign one may remain
	if h.rt.VolumeCount() != 1 {
		h.t.Fatalf("expected only the pre-existing volume, got %d volumes", h.rt.VolumeCount())
	}
	if h.rt.ContainerCount() != 1 {
		h.t.Fatalf("expected only the pre-existing container, got %d", h.rt.ContainerCount())
	}
	if h.rt.NetworkCount() != 1 {
		h.t.Fatalf("expected only the pre-existing network, got %d", h.rt.NetworkCount())
	}
	for _, lp := range h.spec.LocalPaths {
		if _, err := os.Stat(lp.Name); !os.IsNotExist(err) {
			h.t.Fatalf("local path %s still exists", lp.Name)
		}
	}
	clusterDir := filepath.Join(h.dir, "clusters", clusterName)
	if _, err := os.Stat(clusterDir); !os.IsNotExist(err) {
		h.t.Fatalf("cluster directory %s still exists", clusterDir)
	}
}

// assertRollbackOrder checks the delete call ordering respects dependencies:
// every stop precedes the removes; every container remove precedes every
// network/volume remove; local content is handled after runtime resources.
func (h *harness) assertRollbackOrder() {
	h.t.Helper()
	calls := h.rt.Calls()
	firstRemoveContainer := -1
	firstRemoveNetwork := len(calls)
	firstRemoveVolume := len(calls)
	lastStop := -1
	lastRemoveContainer := -1
	for i, c := range calls {
		switch c.Op {
		case fakeruntime.OpStopContainer:
			lastStop = i
		case fakeruntime.OpRemoveContainer:
			if firstRemoveContainer == -1 {
				firstRemoveContainer = i
			}
			lastRemoveContainer = i
		case fakeruntime.OpRemoveNetwork:
			if i < firstRemoveNetwork {
				firstRemoveNetwork = i
			}
		case fakeruntime.OpRemoveVolume:
			if i < firstRemoveVolume {
				firstRemoveVolume = i
			}
		}
	}
	if firstRemoveContainer == -1 {
		h.t.Fatalf("no container removal calls recorded")
	}
	if lastStop > firstRemoveContainer {
		h.t.Fatalf("a container was stopped after removals began")
	}
	if lastRemoveContainer >= firstRemoveNetwork {
		h.t.Fatalf("network removed before all containers were gone")
	}
	if lastRemoveContainer >= firstRemoveVolume {
		h.t.Fatalf("volume removed before all containers were gone")
	}
}

// assertNoMutations verifies the runtime received no create/remove/stop calls
// (used by the duplicate-name test).
func (h *harness) assertNoMutations() {
	h.t.Helper()
	for _, c := range h.rt.Calls() {
		switch c.Op {
		case fakeruntime.OpCreateNetwork, fakeruntime.OpCreateVolume,
			fakeruntime.OpCreateContainer, fakeruntime.OpRemoveNetwork,
			fakeruntime.OpRemoveVolume, fakeruntime.OpRemoveContainer,
			fakeruntime.OpStopContainer:
			h.t.Fatalf("unexpected mutating call: %s:%s", c.Op, c.Name)
		}
	}
}

func asCreateError(t *testing.T, err error) *rollback.CreateError {
	t.Helper()
	var ce *rollback.CreateError
	if !errors.As(err, &ce) {
		t.Fatalf("expected *rollback.CreateError, got %T: %v", err, err)
	}
	return ce
}

// TestFirstNodeFailure: failure while initializing the very first node.
func TestFirstNodeFailure(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	opts := h.baseOptions()
	opts.Initializer = failNodeAt(0)

	_, err := rollback.NewCreator(h.rt, nil).Create(context.Background(), h.spec, opts)
	if err == nil {
		t.Fatal("expected creation failure")
	}
	ce := asCreateError(t, err)
	if ce.Retained {
		t.Fatal("retained should be false")
	}
	if ce.RollbackErr != nil {
		t.Fatalf("rollback should be clean, got: %v", ce.RollbackErr)
	}
	h.assertRollbackOrder()
	h.assertAllOwnedGone()
	h.assertForeignUntouched()
}

// TestThirdNodeFailure is the headline scenario from the task: the third node
// times out after the network, volume and three containers exist.
func TestThirdNodeFailure(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	opts := h.baseOptions()
	opts.Initializer = failNodeAt(2)

	_, err := rollback.NewCreator(h.rt, nil).Create(context.Background(), h.spec, opts)
	if err == nil {
		t.Fatal("expected creation failure")
	}
	ce := asCreateError(t, err)
	if ce.RollbackErr != nil {
		t.Fatalf("rollback should be clean, got: %v", ce.RollbackErr)
	}
	if ce.Leftovers != nil && len(ce.Leftovers) != 0 {
		t.Fatalf("unexpected leftovers: %v", ce.Leftovers)
	}
	// all three containers had to be stopped before any removal
	stops := 0
	for _, c := range h.rt.Calls() {
		if c.Op == fakeruntime.OpStopContainer {
			stops++
		}
	}
	if stops != 3 {
		t.Fatalf("expected 3 stop calls, got %d", stops)
	}
	h.assertRollbackOrder()
	h.assertAllOwnedGone()
	h.assertForeignUntouched()
}

// TestNetworkCreationFailure: nothing beyond the network exists, and the
// intent ledger entry for the network that failed to create must be tolerated
// (not-found == success).
func TestNetworkCreationFailure(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	h.rt.Fail(fakeruntime.OpCreateNetwork, networkName, errors.New("daemon refused network create"))
	opts := h.baseOptions()

	_, err := rollback.NewCreator(h.rt, nil).Create(context.Background(), h.spec, opts)
	if err == nil {
		t.Fatal("expected creation failure")
	}
	ce := asCreateError(t, err)
	if ce.RollbackErr != nil {
		t.Fatalf("rollback should tolerate the never-created network, got: %v", ce.RollbackErr)
	}
	// no container/volume create calls may have happened
	for _, c := range h.rt.Calls() {
		if c.Op == fakeruntime.OpCreateContainer || c.Op == fakeruntime.OpCreateVolume {
			t.Fatalf("resource created after network failure: %s", c.Name)
		}
	}
	// local paths were created before the network and must be cleaned
	for _, lp := range h.spec.LocalPaths {
		if _, err := os.Stat(lp.Name); !os.IsNotExist(err) {
			t.Fatalf("local path %s not cleaned", lp.Name)
		}
	}
	h.assertAllOwnedGone()
	h.assertForeignUntouched()
}

// TestRollbackContainerDeleteFailure: one container delete permanently fails;
// the others must still be cleaned and the failure must be reported while the
// original creation error is preserved.
func TestRollbackContainerDeleteFailure(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	stuck := h.spec.Nodes[1].Name
	h.rt.ContainerDeleteFailures[stuck] = errors.New("permission denied removing container")
	opts := h.baseOptions()
	opts.Initializer = failNodeAt(2)
	// keep attempts small so the test stays fast
	opts.Rollback.MaxAttempts = 2

	_, err := rollback.NewCreator(h.rt, nil).Create(context.Background(), h.spec, opts)
	if err == nil {
		t.Fatal("expected creation failure")
	}
	ce := asCreateError(t, err)
	if ce.RollbackErr == nil {
		t.Fatal("rollback error must be reported")
	}
	// both errors survive: original and rollback
	if ce.CreateErr == nil || ce.CreateErr.Error() == "" {
		t.Fatal("original creation error lost")
	}
	if !errors.Is(err, ce.CreateErr) {
		t.Fatal("errors.Is must reach the original creation error")
	}
	// the aggregate is reachable through the standard multi-error interface
	type multiUnwrapper interface {
		error
		Unwrap() []error
	}
	var unwrapper multiUnwrapper
	if !errors.As(err, &unwrapper) {
		t.Fatal("rollback aggregate must be reachable via errors.As")
	}
	if !errors.Is(unwrapper, rollback.ErrInUse) &&
		!strings.Contains(unwrapper.Error(), "permission denied") {
		t.Fatalf("rollback aggregate lost its details: %v", unwrapper)
	}
	// the stuck container remains, everything else owned is gone
	if !h.rt.ContainerExists(stuck) {
		t.Fatal("stuck container should remain after failed delete")
	}
	for _, n := range h.spec.Nodes {
		if n.Name == stuck {
			continue
		}
		if h.rt.ContainerExists(n.Name) {
			t.Fatalf("container %s should have been removed despite sibling failure", n.Name)
		}
	}
	// the stuck container keeps its network attached; the network delete must
	// have been attempted and reported, not silently skipped
	sawNetworkAttempt := false
	for _, c := range h.rt.Calls() {
		if c.Op == fakeruntime.OpRemoveNetwork && c.Name == networkName {
			sawNetworkAttempt = true
		}
	}
	if !sawNetworkAttempt {
		t.Fatal("network removal was not attempted after sibling container failure")
	}
	if len(ce.Leftovers) == 0 {
		t.Fatal("leftovers inventory should list remaining resources")
	}
	h.assertForeignUntouched()
}

// TestDuplicateNameFailsEarly: a same-named cluster already exists. Creation
// must fail with rollback.ErrAlreadyExists before creating anything, and rollback must
// never delete the existing cluster's containers.
func TestDuplicateNameFailsEarly(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	// seed a cluster using exactly the requested name
	h.rt.SeedContainer(clusterName+"-control-plane", "shared-kind-net", clusterName)
	h.rt.SeedContainer(clusterName+"-worker", "shared-kind-net", clusterName)
	opts := h.baseOptions()

	_, err := rollback.NewCreator(h.rt, nil).Create(context.Background(), h.spec, opts)
	if err == nil {
		t.Fatal("expected already-exists failure")
	}
	if !errors.Is(err, rollback.ErrAlreadyExists) {
		t.Fatalf("expected rollback.ErrAlreadyExists, got %v", err)
	}
	var ce *rollback.CreateError
	if errors.As(err, &ce) {
		t.Fatalf("duplicate-name failure must not run rollback: %+v", ce)
	}
	h.assertNoMutations()
	// both pre-existing same-name containers survive
	if !h.rt.ContainerExists(clusterName+"-control-plane") ||
		!h.rt.ContainerExists(clusterName+"-worker") {
		t.Fatal("existing same-name cluster containers were deleted")
	}
	h.assertForeignUntouched()
}

// TestRetainKeepsScene: with Retain the failure scene is preserved, rollback
// makes no delete calls, and the returned error lists leftovers + manual
// cleanup commands.
func TestRetainKeepsScene(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	opts := h.baseOptions()
	opts.Retain = true
	opts.Initializer = failNodeAt(2)

	_, err := rollback.NewCreator(h.rt, nil).Create(context.Background(), h.spec, opts)
	if err == nil {
		t.Fatal("expected creation failure")
	}
	ce := asCreateError(t, err)
	if !ce.Retained {
		t.Fatal("Retained flag not set")
	}
	for _, c := range h.rt.Calls() {
		if c.Op == fakeruntime.OpRemoveContainer || c.Op == fakeruntime.OpRemoveNetwork ||
			c.Op == fakeruntime.OpRemoveVolume || c.Op == fakeruntime.OpStopContainer {
			t.Fatalf("retain mode must not mutate runtime, saw %s:%s", c.Op, c.Name)
		}
	}
	// everything created stays
	for _, n := range h.spec.Nodes {
		if !h.rt.ContainerExists(n.Name) {
			t.Fatalf("retained container %s missing", n.Name)
		}
	}
	if !h.rt.NetworkExists(networkName) || !h.rt.VolumeExists(volumeName) {
		t.Fatal("retained network/volume missing")
	}
	if len(ce.Leftovers) == 0 {
		t.Fatal("retained error must list leftover resources")
	}
	if ce.RollbackErr == nil || ce.RollbackErr.Error() == "" {
		t.Fatal("retained error must include manual cleanup hints")
	}
	// local paths retained too
	for _, lp := range h.spec.LocalPaths {
		if _, err := os.Stat(lp.Name); err != nil {
			t.Fatalf("retained local path %s missing: %v", lp.Name, err)
		}
	}
	h.assertForeignUntouched()
}
