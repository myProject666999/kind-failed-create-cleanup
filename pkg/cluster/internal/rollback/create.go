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
	"path/filepath"

	"sigs.k8s.io/kind/pkg/log"
)

// NodeSpec describes one node container the creator should provision.
type NodeSpec struct {
	Name  string
	Image string
	Role  string
	// Volumes lists explicit named volumes mounted into this node.
	Volumes []string
}

// LocalPathSpec is host-side content created for the cluster: the cluster
// state directory or a temporary credential file.
type LocalPathSpec struct {
	// Name is the absolute path.
	Name string
	// Directory makes the creator MkdirAll instead of writing an empty file.
	Directory bool
}

// Spec is the declarative description of a cluster creation.
type Spec struct {
	ClusterName string
	// Network is the per-creation network name. A network present in the
	// preflight snapshot is treated as a pre-existing shared resource: reused,
	// not recorded, never deleted.
	Network string
	// Volumes are explicit named volumes, created before the nodes.
	Volumes []string
	// Nodes are provisioned in order; node N's initializer runs after nodes
	// 0..N-1 have initialized.
	Nodes []NodeSpec
	// LocalPaths are host files/directories tracked alongside runtime
	// resources so rollback removes them too.
	LocalPaths []LocalPathSpec
	// StateDir is the cluster state directory holding the persisted ledger
	// (creation-state.json). When non-empty, the ledger is written before any
	// runtime resource is created and refreshed after every step, so a killed
	// process can be recovered via RecoverOrphans. It should also appear in
	// LocalPaths as a directory so rollback removes it last. An empty value
	// disables persistence (in-memory ledger only).
	StateDir string
}

// NodeInitializer is the per-node bootstrap step (kubeadm init/join in the
// real provider). It runs after all containers exist. Returning an error
// fails creation and triggers rollback.
type NodeInitializer func(ctx context.Context, rt Runtime, node Resource) error

// CreateOptions controls a creation.
type CreateOptions struct {
	// Retain keeps every resource in place on failure. Rollback is skipped and
	// the returned CreateError lists leftovers plus manual cleanup hints.
	Retain bool
	// Initializer runs per node in order. nil means no bootstrap step.
	Initializer NodeInitializer
	// Rollback tunes cleanup. Zero values take the defaults.
	Rollback Options
	// Signals, when non-nil, installs the interrupt watcher. nil means the
	// creator is driven purely by ctx.
	Signals *SignalPolicy
}

// CreatedCluster is returned on success.
type CreatedCluster struct {
	Name      string
	AttemptID string
	Ledger    *Ledger
}

// Creator runs a full create-with-rollback lifecycle against a Runtime.
type Creator struct {
	rt     Runtime
	logger log.Logger
}

// NewCreator builds a Creator.
func NewCreator(rt Runtime, logger log.Logger) *Creator {
	if logger == nil {
		logger = log.NoopLogger{}
	}
	return &Creator{rt: rt, logger: logger}
}

// Create provisions the spec and, on any failure, rolls back everything this
// attempt owned — never resources that existed before it started.
func (c *Creator) Create(parentCtx context.Context, spec Spec, opts CreateOptions) (*CreatedCluster, error) {
	ctx, cancel := context.WithCancel(parentCtx)
	defer cancel()

	var watcher *signalWatcher
	if opts.Signals != nil {
		watcher = watchSignals(cancel, *opts.Signals)
		defer watcher.Stop()
	}

	// ---- preflight: snapshot + early "already exists" failure --------------
	preflight, err := c.rt.Snapshot(ctx)
	if err != nil {
		return nil, err
	}
	if err := c.checkNameAvailable(spec, preflight); err != nil {
		// nothing has been created; no ledger, no rollback, nothing to clean.
		return nil, err
	}

	ledger := NewLedger(spec.ClusterName, preflight)
	c.logger.V(1).Infof("create attempt %s for cluster %q", ledger.AttemptID, spec.ClusterName)

	// the state directory is itself a tracked local resource so clean rollback
	// removes it; on partial failure rollback keeps it for recovery
	if spec.StateDir != "" {
		alreadyTracked := false
		for _, lp := range spec.LocalPaths {
			if lp.Directory && lp.Name == spec.StateDir {
				alreadyTracked = true
				break
			}
		}
		if !alreadyTracked {
			spec.LocalPaths = append(spec.LocalPaths, LocalPathSpec{Name: spec.StateDir, Directory: true})
		}
		opts.Rollback.StateDir = spec.StateDir
	}

	// The state directory and ledger snapshot exist before any runtime
	// resource, so a kill at any later point leaves a recoverable trail.
	if spec.StateDir != "" {
		if err := os.MkdirAll(spec.StateDir, 0o755); err != nil {
			return nil, err
		}
		if err := ledger.Persist(spec.StateDir); err != nil {
			c.logger.Warnf("cannot persist creation state: %v", err)
		}
	}
	persist := func() {
		if spec.StateDir == "" {
			return
		}
		if err := ledger.Persist(spec.StateDir); err != nil {
			c.logger.Warnf("cannot refresh creation state: %v", err)
		}
	}

	// creationErr captures the first failure; the deferred-style tail decides
	// between rollback and retain.
	creationErr := c.provision(ctx, spec, opts, ledger, persist)
	if creationErr == nil {
		// a healthy, finished creation must not look like an unfinished one to
		// a later process
		if spec.StateDir != "" {
			if err := RemoveState(spec.StateDir); err != nil {
				c.logger.Warnf("cannot remove creation state: %v", err)
			}
		}
		return &CreatedCluster{
			Name:      spec.ClusterName,
			AttemptID: ledger.AttemptID,
			Ledger:    ledger,
		}, nil
	}

	if opts.Retain {
		// keep the state file on purpose: it is the manual recovery trail
		return nil, c.retainError(creationErr, ledger)
	}

	rb := NewRollbacker(c.rt, opts.Rollback)
	result := rb.Rollback(context.Background(), ledger)
	// rollback finished: drop the persisted ledger. If cleanup left failures,
	// the state file would help a second recovery pass — keep it in that case
	// instead of deleting.
	if spec.StateDir != "" && len(result.Failures) == 0 {
		if err := RemoveState(spec.StateDir); err != nil {
			c.logger.Warnf("cannot remove creation state: %v", err)
		}
	}
	ce := &CreateError{
		CreateErr:   creationErr,
		LogsDir:     result.LogsDir,
		Leftovers:   result.Leftovers,
		RollbackErr: Aggregate("rollback failures", result.Failures),
	}
	return nil, ce
}

// checkNameAvailable fails early when a cluster with the same name already
// exists. Detection follows the real provider rule: a container carrying the
// cluster label means the cluster exists. Such a cluster predates us and must
// never be deleted by our rollback.
func (c *Creator) checkNameAvailable(spec Spec, snap Snapshot) error {
	for _, n := range snap.Containers {
		if n.Labels[ClusterLabel] == spec.ClusterName {
			// wrap the sentinel so callers can errors.Is(err, ErrAlreadyExists)
			return &alreadyExistsError{cluster: spec.ClusterName}
		}
	}
	return nil
}

type alreadyExistsError struct{ cluster string }

func (e *alreadyExistsError) Error() string {
	return "node(s) already exist for a cluster with the name " + quote(e.cluster)
}

func (e *alreadyExistsError) Unwrap() error { return ErrAlreadyExists }

func quote(s string) string { return "\"" + s + "\"" }

// provision runs every creation step in dependency order. Each step records
// its intent before touching the runtime and confirms afterwards. persist is
// invoked after every successful step so the on-disk ledger never lags far
// behind the actual runtime state.
func (c *Creator) provision(ctx context.Context, spec Spec, opts CreateOptions, ledger *Ledger, persist func()) error {
	// 1. host-side content first: cluster directory and credential files.
	for _, lp := range spec.LocalPaths {
		r := Resource{Kind: KindLocalPath, Name: lp.Name, Labels: ledger.Labels()}
		ledger.RecordIntent(r)
		persist() // intent must survive a kill even if the write below is interrupted
		if lp.Directory {
			if err := os.MkdirAll(lp.Name, 0o755); err != nil {
				return err
			}
		} else {
			if err := os.MkdirAll(filepath.Dir(lp.Name), 0o755); err != nil {
				return err
			}
			if err := os.WriteFile(lp.Name, []byte{}, 0o600); err != nil {
				return err
			}
		}
		ledger.MarkConfirmed(r)
		persist()
	}

	// 2. network — skipped (and unowned) when it pre-exists.
	if spec.Network != "" {
		if _, preExisting := preflightNames(ledger, KindNetwork)[spec.Network]; !preExisting {
			if err := c.createNetwork(ctx, spec.Network, ledger, persist); err != nil {
				return err
			}
		} else {
			c.logger.V(1).Infof("network %q pre-exists; reusing without taking ownership", spec.Network)
		}
	}

	// 3. explicit named volumes.
	for _, v := range spec.Volumes {
		if _, preExisting := preflightNames(ledger, KindVolume)[v]; preExisting {
			continue
		}
		if err := c.createVolume(ctx, v, ledger, persist); err != nil {
			return err
		}
	}

	// 4. node containers, in order.
	for _, n := range spec.Nodes {
		if err := c.createContainer(ctx, spec, n, ledger, persist); err != nil {
			return err
		}
	}

	// 5. per-node bootstrap (the step where "the third node times out").
	if opts.Initializer != nil {
		for _, n := range spec.Nodes {
			node := Resource{
				Kind:   KindContainer,
				Name:   n.Name,
				Labels: ledger.Labels(),
			}
			if err := opts.Initializer(ctx, c.rt, node); err != nil {
				return err
			}
		}
	}
	return nil
}

func preflightNames(l *Ledger, kind ResourceKind) map[string]Resource {
	return l.preflight.NamesByKind(kind)
}

func (c *Creator) createNetwork(ctx context.Context, name string, ledger *Ledger, persist func()) error {
	r := Resource{Kind: KindNetwork, Name: name, Labels: ledger.Labels()}
	ledger.RecordIntent(r) // intent first: a kill here still leaves a deletable record
	persist()
	res, err := c.rt.CreateNetwork(ctx, name, ledger.Labels())
	if err != nil {
		return err
	}
	ledger.MarkConfirmed(r)
	c.adoptCreated(res, ledger)
	persist()
	return nil
}

func (c *Creator) createVolume(ctx context.Context, name string, ledger *Ledger, persist func()) error {
	r := Resource{Kind: KindVolume, Name: name, Labels: ledger.Labels()}
	ledger.RecordIntent(r)
	persist()
	res, err := c.rt.CreateVolume(ctx, name, ledger.Labels())
	if err != nil {
		return err
	}
	ledger.MarkConfirmed(r)
	c.adoptCreated(res, ledger)
	persist()
	return nil
}

func (c *Creator) createContainer(ctx context.Context, spec Spec, n NodeSpec, ledger *Ledger, persist func()) error {
	r := Resource{Kind: KindContainer, Name: n.Name, Labels: ledger.Labels()}
	ledger.RecordIntent(r)
	persist()
	res, err := c.rt.CreateContainer(ctx, ContainerSpec{
		Name:     n.Name,
		Image:    n.Image,
		Network:  spec.Network,
		Volumes:  n.Volumes,
		NodeRole: n.Role,
		Labels:   ledger.Labels(),
	})
	if err != nil {
		// intent stays in the ledger; rollback tolerates the resource missing.
		return err
	}
	ledger.MarkConfirmed(r)
	c.adoptCreated(res, ledger) // implicit anonymous volumes land here
	persist()
	return nil
}

// adoptCreated registers side-effect resources (e.g. anonymous volumes) that
// the runtime generated while creating the primary resource.
func (c *Creator) adoptCreated(res CreateResult, ledger *Ledger) {
	for _, extra := range res.Created {
		if extra.Labels == nil {
			extra.Labels = ledger.Labels()
		}
		ledger.MarkConfirmed(extra)
	}
}

// retainError builds the failure returned when the user asked to keep the
// scene: no rollback, but a complete inventory and concrete cleanup commands.
func (c *Creator) retainError(createErr error, ledger *Ledger) error {
	snap, snapErr := c.rt.Snapshot(context.Background())
	leftovers := ledger.Entries()
	resources := make([]Resource, 0, len(leftovers))
	for _, e := range leftovers {
		if e.Resource.Kind == KindLocalPath {
			if _, err := os.Stat(e.Resource.Name); err == nil {
				resources = append(resources, e.Resource)
			}
			continue
		}
		if _, ok := snap.NamesByKind(e.Resource.Kind)[e.Resource.Name]; ok {
			resources = append(resources, e.Resource)
		}
	}
	hints := manualCleanupHints(resources)
	if snapErr != nil {
		hints = append(hints, "snapshot failed while listing leftovers: "+snapErr.Error())
	}
	return &CreateError{
		CreateErr: createErr,
		Retained:  true,
		Leftovers: resources,
		RollbackErr: Aggregate("manual cleanup required", []error{
			&retainHintError{hints: hints},
		}),
	}
}

type retainHintError struct{ hints []string }

func (e *retainHintError) Error() string {
	out := "resources retained; clean up manually with:\n"
	for _, h := range e.hints {
		out += "  " + h + "\n"
	}
	return out
}

func manualCleanupHints(resources []Resource) []string {
	var hints []string
	// dependency order in reverse: containers, volumes, networks, paths
	for _, r := range resources {
		switch r.Kind {
		case KindContainer:
			hints = append(hints, "docker rm -f "+r.Name)
		case KindVolume:
			hints = append(hints, "docker volume rm "+r.Name)
		case KindNetwork:
			hints = append(hints, "docker network rm "+r.Name)
		case KindLocalPath:
			hints = append(hints, "rm -rf "+r.Name)
		}
	}
	return hints
}
