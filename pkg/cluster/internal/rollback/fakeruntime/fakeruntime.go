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

// Package fakeruntime is an in-memory implementation of the rollback.Runtime
// interface. It simulates containers, networks and volumes without touching a
// real container engine and supports deterministic failure injection at
// named steps, so the full create/rollback lifecycle can be unit-tested.
package fakeruntime

import (
	"context"
	"fmt"
	"sync"

	"sigs.k8s.io/kind/pkg/cluster/internal/rollback"
)

// Op is a recorded runtime operation.
type Op string

const (
	OpSnapshot        Op = "Snapshot"
	OpCreateNetwork   Op = "CreateNetwork"
	OpRemoveNetwork   Op = "RemoveNetwork"
	OpCreateVolume    Op = "CreateVolume"
	OpRemoveVolume    Op = "RemoveVolume"
	OpCreateContainer Op = "CreateContainer"
	OpStopContainer   Op = "StopContainer"
	OpRemoveContainer Op = "RemoveContainer"
	OpContainerLogs   Op = "ContainerLogs"
)

// Call records one runtime invocation in order.
type Call struct {
	Op   Op
	Name string
	Err  error
}

// fakeContainer is the in-memory state of a container.
type fakeContainer struct {
	res     rollback.Resource
	image   string
	network string
	volumes []string
	running bool
	logs    []byte
}

type fakeNetwork struct {
	res      rollback.Resource
	attached map[string]struct{} // container names
}

type fakeVolume struct {
	res       rollback.Resource
	anonymous bool
}

// FakeRuntime implements rollback.Runtime entirely in memory.
type FakeRuntime struct {
	mu         sync.Mutex
	containers map[string]*fakeContainer
	networks   map[string]*fakeNetwork
	volumes    map[string]*fakeVolume
	calls      []Call
	volCounter int

	// Fail maps an operation (optionally qualified by resource name) to the
	// error it should return. Use FailOnce for one-shot injection.
	fail     map[string]error
	failOnce map[string]error

	// NetworkDeleteTransient makes the first `network rm` return ErrInUse to
	// simulate a network whose containers have not fully detached; subsequent
	// calls succeed once no container remains attached.
	NetworkDeleteTransient bool

	// ContainerDeleteFailures maps a container name to the error its
	// RemoveContainer call always returns (used to prove one failed resource
	// does not block the rest of the cleanup).
	ContainerDeleteFailures map[string]error

	// pendingLogs holds log bytes registered before the matching container
	// exists; CreateContainer attaches them automatically.
	pendingLogs map[string][]byte
}

// New returns an empty FakeRuntime.
func New() *FakeRuntime {
	return &FakeRuntime{
		containers:              map[string]*fakeContainer{},
		networks:                map[string]*fakeNetwork{},
		volumes:                 map[string]*fakeVolume{},
		fail:                    map[string]error{},
		failOnce:                map[string]error{},
		ContainerDeleteFailures: map[string]error{},
		pendingLogs:             map[string][]byte{},
	}
}

// Fail injects a permanent failure for op. An empty name matches the op
// regardless of resource.
func (f *FakeRuntime) Fail(op Op, name string, err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.fail[key(op, name)] = err
}

// FailOnce injects a one-shot failure (consumed on the first matching call).
func (f *FakeRuntime) FailOnce(op Op, name string, err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.failOnce[key(op, name)] = err
}

func key(op Op, name string) string { return string(op) + "\x00" + name }

func (f *FakeRuntime) injected(op Op, name string) error {
	if err, ok := f.failOnce[key(op, name)]; ok {
		delete(f.failOnce, key(op, name))
		return err
	}
	if err, ok := f.failOnce[key(op, "")]; ok {
		delete(f.failOnce, key(op, ""))
		return err
	}
	if err, ok := f.fail[key(op, name)]; ok {
		return err
	}
	if err, ok := f.fail[key(op, "")]; ok {
		return err
	}
	return nil
}

func (f *FakeRuntime) record(op Op, name string, err error) {
	f.calls = append(f.calls, Call{Op: op, Name: name, Err: err})
}

// Calls returns a copy of the recorded operations.
func (f *FakeRuntime) Calls() []Call {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]Call, len(f.calls))
	copy(out, f.calls)
	return out
}

// OpNames returns just the ordered (op, name) pairs for easy assertions.
func (f *FakeRuntime) OpNames() []string {
	calls := f.Calls()
	out := make([]string, 0, len(calls))
	for _, c := range calls {
		out = append(out, string(c.Op)+":"+c.Name)
	}
	return out
}

// SeedContainer / SeedNetwork / SeedVolume place pre-existing resources into
// the runtime. Resources seeded this way simulate another, older cluster and
// must survive every create/rollback cycle in the tests.
func (f *FakeRuntime) SeedContainer(name, network, clusterLabel string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	labels := map[string]string{rollback.ClusterLabel: clusterLabel}
	f.containers[name] = &fakeContainer{
		res:     rollback.Resource{Kind: rollback.KindContainer, Name: name, Labels: labels},
		network: network,
		running: true,
	}
}

// SeedNetwork places a pre-existing network (e.g. the shared "kind" network).
func (f *FakeRuntime) SeedNetwork(name string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.networks[name] = &fakeNetwork{
		res:      rollback.Resource{Kind: rollback.KindNetwork, Name: name, Labels: map[string]string{}},
		attached: map[string]struct{}{},
	}
}

// SeedVolume places a pre-existing named volume.
func (f *FakeRuntime) SeedVolume(name string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.volumes[name] = &fakeVolume{
		res: rollback.Resource{Kind: rollback.KindVolume, Name: name, Labels: map[string]string{}},
	}
}

// SetLogs attaches log bytes returned by ContainerLogs for a node. Logs may be
// registered before the container exists; they are held and applied when the
// container is created.
func (f *FakeRuntime) SetLogs(name string, logs []byte) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if c, ok := f.containers[name]; ok {
		c.logs = logs
		return
	}
	f.pendingLogs[name] = logs
}

// Snapshot implements rollback.Runtime.
func (f *FakeRuntime) Snapshot(_ context.Context) (rollback.Snapshot, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.record(OpSnapshot, "", nil)
	snap := rollback.Snapshot{}
	for _, c := range f.containers {
		snap.Containers = append(snap.Containers, c.res)
	}
	for _, n := range f.networks {
		snap.Networks = append(snap.Networks, n.res)
	}
	for _, v := range f.volumes {
		snap.Volumes = append(snap.Volumes, v.res)
	}
	return snap, nil
}

// CreateNetwork implements rollback.Runtime.
func (f *FakeRuntime) CreateNetwork(_ context.Context, name string, labels map[string]string) (rollback.CreateResult, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := f.injected(OpCreateNetwork, name); err != nil {
		f.record(OpCreateNetwork, name, err)
		return rollback.CreateResult{}, err
	}
	if _, ok := f.networks[name]; ok {
		err := fmt.Errorf("%w: network %s", rollback.ErrAlreadyExists, name)
		f.record(OpCreateNetwork, name, err)
		return rollback.CreateResult{}, err
	}
	f.networks[name] = &fakeNetwork{
		res:      rollback.Resource{Kind: rollback.KindNetwork, Name: name, Labels: copyLabels(labels)},
		attached: map[string]struct{}{},
	}
	f.record(OpCreateNetwork, name, nil)
	return rollback.CreateResult{Created: []rollback.Resource{f.networks[name].res}}, nil
}

// RemoveNetwork implements rollback.Runtime.
func (f *FakeRuntime) RemoveNetwork(_ context.Context, name string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := f.injected(OpRemoveNetwork, name); err != nil {
		f.record(OpRemoveNetwork, name, err)
		return err
	}
	n, ok := f.networks[name]
	if !ok {
		err := fmt.Errorf("%w: network %s", rollback.ErrNotFound, name)
		f.record(OpRemoveNetwork, name, err)
		return err
	}
	if f.NetworkDeleteTransient && len(n.attached) > 0 {
		err := fmt.Errorf("%w: network %s still has %d attached containers", rollback.ErrInUse, name, len(n.attached))
		f.record(OpRemoveNetwork, name, err)
		return err
	}
	delete(f.networks, name)
	f.record(OpRemoveNetwork, name, nil)
	return nil
}

// CreateVolume implements rollback.Runtime.
func (f *FakeRuntime) CreateVolume(_ context.Context, name string, labels map[string]string) (rollback.CreateResult, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := f.injected(OpCreateVolume, name); err != nil {
		f.record(OpCreateVolume, name, err)
		return rollback.CreateResult{}, err
	}
	if _, ok := f.volumes[name]; ok {
		err := fmt.Errorf("%w: volume %s", rollback.ErrAlreadyExists, name)
		f.record(OpCreateVolume, name, err)
		return rollback.CreateResult{}, err
	}
	f.volumes[name] = &fakeVolume{res: rollback.Resource{Kind: rollback.KindVolume, Name: name, Labels: copyLabels(labels)}}
	f.record(OpCreateVolume, name, nil)
	return rollback.CreateResult{Created: []rollback.Resource{f.volumes[name].res}}, nil
}

// RemoveVolume implements rollback.Runtime.
func (f *FakeRuntime) RemoveVolume(_ context.Context, name string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := f.injected(OpRemoveVolume, name); err != nil {
		f.record(OpRemoveVolume, name, err)
		return err
	}
	if _, ok := f.volumes[name]; !ok {
		err := fmt.Errorf("%w: volume %s", rollback.ErrNotFound, name)
		f.record(OpRemoveVolume, name, err)
		return err
	}
	for _, c := range f.containers {
		for _, v := range c.volumes {
			if v == name {
				err := fmt.Errorf("%w: volume %s mounted by %s", rollback.ErrInUse, name, c.res.Name)
				f.record(OpRemoveVolume, name, err)
				return err
			}
		}
	}
	delete(f.volumes, name)
	f.record(OpRemoveVolume, name, nil)
	return nil
}

// CreateContainer implements rollback.Runtime. Every container gets an
// implicit anonymous volume, mirroring docker's unnamed --volume mounts, so
// the tests can prove rollback tracks and removes them.
func (f *FakeRuntime) CreateContainer(_ context.Context, spec rollback.ContainerSpec) (rollback.CreateResult, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := f.injected(OpCreateContainer, spec.Name); err != nil {
		f.record(OpCreateContainer, spec.Name, err)
		return rollback.CreateResult{}, err
	}
	if _, ok := f.containers[spec.Name]; ok {
		err := fmt.Errorf("%w: container %s", rollback.ErrAlreadyExists, spec.Name)
		f.record(OpCreateContainer, spec.Name, err)
		return rollback.CreateResult{}, err
	}
	net, ok := f.networks[spec.Network]
	if !ok {
		err := fmt.Errorf("%w: network %s for container %s", rollback.ErrNotFound, spec.Network, spec.Name)
		f.record(OpCreateContainer, spec.Name, err)
		return rollback.CreateResult{}, err
	}
	labels := copyLabels(spec.Labels)
	f.volCounter++
	anonName := fmt.Sprintf("anon-vol-%d", f.volCounter)
	anon := &fakeVolume{
		res:       rollback.Resource{Kind: rollback.KindVolume, Name: anonName, Labels: copyLabels(spec.Labels)},
		anonymous: true,
	}
	f.volumes[anonName] = anon
	vols := append([]string{anonName}, spec.Volumes...)
	c := &fakeContainer{
		res:     rollback.Resource{Kind: rollback.KindContainer, Name: spec.Name, Labels: labels},
		image:   spec.Image,
		network: spec.Network,
		volumes: vols,
		running: true,
		logs:    f.pendingLogs[spec.Name],
	}
	delete(f.pendingLogs, spec.Name)
	f.containers[spec.Name] = c
	net.attached[spec.Name] = struct{}{}
	f.record(OpCreateContainer, spec.Name, nil)
	return rollback.CreateResult{Created: []rollback.Resource{c.res, anon.res}}, nil
}

// StopContainer implements rollback.Runtime.
func (f *FakeRuntime) StopContainer(_ context.Context, name string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := f.injected(OpStopContainer, name); err != nil {
		f.record(OpStopContainer, name, err)
		return err
	}
	c, ok := f.containers[name]
	if !ok {
		err := fmt.Errorf("%w: container %s", rollback.ErrNotFound, name)
		f.record(OpStopContainer, name, err)
		return err
	}
	c.running = false
	f.record(OpStopContainer, name, nil)
	return nil
}

// RemoveContainer implements rollback.Runtime. It detaches the container from
// its network and deletes the implicit anonymous volume (as `docker rm -v`
// would); explicit named volumes survive and are rollback's responsibility.
func (f *FakeRuntime) RemoveContainer(_ context.Context, name string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	c, ok := f.containers[name]
	if !ok {
		err := fmt.Errorf("%w: container %s", rollback.ErrNotFound, name)
		f.record(OpRemoveContainer, name, err)
		return err
	}
	if err, fail := f.ContainerDeleteFailures[name]; fail {
		f.record(OpRemoveContainer, name, err)
		return err
	}
	if err := f.injected(OpRemoveContainer, name); err != nil {
		f.record(OpRemoveContainer, name, err)
		return err
	}
	if net, ok := f.networks[c.network]; ok {
		delete(net.attached, name)
	}
	// drop anonymous volumes created with this container
	for _, v := range c.volumes {
		if vol, ok := f.volumes[v]; ok && vol.anonymous {
			delete(f.volumes, v)
		}
	}
	delete(f.containers, name)
	f.record(OpRemoveContainer, name, nil)
	return nil
}

// ContainerLogs implements rollback.Runtime.
func (f *FakeRuntime) ContainerLogs(_ context.Context, name string) ([]byte, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	c, ok := f.containers[name]
	if !ok {
		err := fmt.Errorf("%w: container %s", rollback.ErrNotFound, name)
		f.record(OpContainerLogs, name, err)
		return nil, err
	}
	f.record(OpContainerLogs, name, nil)
	return c.logs, nil
}

// ---- test-only inspectors ----

// ContainerExists reports whether a container remains.
func (f *FakeRuntime) ContainerExists(name string) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	_, ok := f.containers[name]
	return ok
}

// NetworkExists reports whether a network remains.
func (f *FakeRuntime) NetworkExists(name string) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	_, ok := f.networks[name]
	return ok
}

// VolumeExists reports whether a volume remains.
func (f *FakeRuntime) VolumeExists(name string) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	_, ok := f.volumes[name]
	return ok
}

// ContainerCount / NetworkCount / VolumeCount size the runtime.
func (f *FakeRuntime) ContainerCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.containers)
}

func (f *FakeRuntime) NetworkCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.networks)
}

func (f *FakeRuntime) VolumeCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.volumes)
}

// ContainerRunning reports the simulated running state.
func (f *FakeRuntime) ContainerRunning(name string) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	c, ok := f.containers[name]
	return ok && c.running
}

func copyLabels(in map[string]string) map[string]string {
	out := make(map[string]string, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}
