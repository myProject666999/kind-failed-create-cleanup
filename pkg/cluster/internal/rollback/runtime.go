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
	stderrors "errors"
)

// ResourceKind enumerates the runtime resource kinds managed by rollback.
type ResourceKind string

const (
	KindContainer ResourceKind = "container"
	KindNetwork   ResourceKind = "network"
	KindVolume    ResourceKind = "volume"
	// KindLocalPath covers host-side content that is not part of the
	// container runtime: the cluster state directory and temporary credential
	// files. These are tracked in the same ledger so rollback has one complete
	// inventory.
	KindLocalPath ResourceKind = "localpath"
)

// Resource identifies one runtime resource by kind and name.
type Resource struct {
	Kind   ResourceKind
	Name   string
	Labels map[string]string
}

// CreateResult is returned by the create calls. Anonymous volumes generated
// by the runtime while creating a container (e.g. docker's unnamed --volume
// mounts) are reported here so the caller can register them in the ledger
// before they can leak.
type CreateResult struct {
	// Created lists resources that now exist as a side effect of the call,
	// including the primary resource itself and any implicit volumes.
	Created []Resource
}

// Snapshot is a point-in-time view of every resource the runtime knows
// about. It is taken twice:
//   - before creation starts, to remember which resources pre-existed;
//   - during rollback, to verify ownership before deleting.
type Snapshot struct {
	Containers []Resource
	Networks   []Resource
	Volumes    []Resource
}

// NamesByKind indexes a snapshot by resource kind.
func (s Snapshot) NamesByKind(kind ResourceKind) map[string]Resource {
	out := map[string]Resource{}
	var list []Resource
	switch kind {
	case KindContainer:
		list = s.Containers
	case KindNetwork:
		list = s.Networks
	case KindVolume:
		list = s.Volumes
	}
	for _, r := range list {
		out[r.Name] = r
	}
	return out
}

// Runtime is the minimal container-runtime surface required for create +
// rollback. The real docker/podman providers implement it on top of their CLI
// calls; tests use an in-memory implementation.
//
// Error contract: every method MUST return (or wrap) one of the package
// sentinels ErrNotFound / ErrAlreadyExists / ErrInUse where applicable, so
// rollback can tell "harmless, already gone" apart from "must be reported".
type Runtime interface {
	// Snapshot returns all resources currently visible to the runtime.
	Snapshot(ctx context.Context) (Snapshot, error)

	// CreateNetwork creates a network. Returns ErrAlreadyExists if a network
	// with the name already exists.
	CreateNetwork(ctx context.Context, name string, labels map[string]string) (CreateResult, error)
	// RemoveNetwork deletes a network. Returns ErrNotFound when the network
	// is gone, ErrInUse when containers are still attached.
	RemoveNetwork(ctx context.Context, name string) error

	// CreateVolume creates an explicit, named volume.
	CreateVolume(ctx context.Context, name string, labels map[string]string) (CreateResult, error)
	// RemoveVolume deletes a volume. Returns ErrNotFound when the volume is
	// gone, ErrInUse when a container still references it.
	RemoveVolume(ctx context.Context, name string) error

	// CreateContainer creates and starts a container attached to the named
	// network, optionally mounting the named volumes. Implicit anonymous
	// volumes created by the runtime are returned in CreateResult.
	CreateContainer(ctx context.Context, spec ContainerSpec) (CreateResult, error)
	// StopContainer stops a running container. Stopping an already stopped or
	// missing container returns ErrNotFound for the missing case.
	StopContainer(ctx context.Context, name string) error
	// RemoveContainer deletes a container. Returns ErrNotFound when absent.
	RemoveContainer(ctx context.Context, name string) error
	// ContainerLogs returns the logs of a node container, used by the
	// best-effort log export before rollback.
	ContainerLogs(ctx context.Context, name string) ([]byte, error)
}

// ContainerSpec describes a container to be created.
type ContainerSpec struct {
	Name     string
	Image    string
	Network  string
	Volumes  []string // explicit volume names
	NodeRole string
	Labels   map[string]string
}

// Sentinel errors. Runtime implementations wrap these with %w so callers can
// use errors.Is.
var (
	// ErrNotFound means the resource is already absent. Rollback treats it as
	// success.
	ErrNotFound = stderrors.New("resource not found")
	// ErrAlreadyExists means creation collided with an existing resource.
	ErrAlreadyExists = stderrors.New("resource already exists")
	// ErrInUse means deletion failed because the resource is still referenced
	// (typically a network with attached containers). Rollback retries it
	// within the time budget, then reports it.
	ErrInUse = stderrors.New("resource in use")
)

// IsNotFound reports whether err is (or wraps) ErrNotFound.
func IsNotFound(err error) bool { return stderrors.Is(err, ErrNotFound) }

// IsInUse reports whether err is (or wraps) ErrInUse.
func IsInUse(err error) bool { return stderrors.Is(err, ErrInUse) }

// IsAlreadyExists reports whether err is (or wraps) ErrAlreadyExists.
func IsAlreadyExists(err error) bool { return stderrors.Is(err, ErrAlreadyExists) }
