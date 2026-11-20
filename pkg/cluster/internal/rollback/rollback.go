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
	"time"

	"sigs.k8s.io/kind/pkg/log"
)

// Default rollback tuning. Every bound is deliberately small: a failed create
// must never leave the terminal blocked for long.
const (
	// DefaultTimeout caps the entire rollback including log export.
	DefaultTimeout = 30 * time.Second
	// DefaultMaxAttempts caps retries of a single resource delete.
	DefaultMaxAttempts = 4
	// DefaultRetryDelay is the first backoff; it doubles up to MaxRetryDelay.
	DefaultRetryDelay = 200 * time.Millisecond
	// DefaultMaxRetryDelay bounds backoff growth.
	DefaultMaxRetryDelay = time.Second
	// DefaultLogExportTimeout bounds the best-effort pre-rollback log export.
	DefaultLogExportTimeout = 10 * time.Second
)

// LogExporter is the optional hook for preserving a failure scene before
// resources are destroyed. Keeping it as a hook lets providers plug in the
// existing `kind export logs` implementation; the fake runtime ships a simple
// file-based one. A failed export never blocks rollback.
type LogExporter interface {
	// ExportLogs writes logs for the given node containers into dir.
	ExportLogs(ctx context.Context, dir string, containers []Resource) error
}

// noopExporter is used when no hook is configured.
type noopExporter struct{}

func (noopExporter) ExportLogs(context.Context, string, []Resource) error { return nil }

// Options configures a Rollbacker. Zero values are replaced by defaults.
type Options struct {
	Logger           log.Logger
	Timeout          time.Duration
	MaxAttempts      int
	RetryDelay       time.Duration
	MaxRetryDelay    time.Duration
	LogExportTimeout time.Duration
	Exporter         LogExporter
	// LogsBaseDir is where timestamped log export directories are created.
	// Defaults to os.TempDir().
	LogsBaseDir string
	// StateDir is the cluster state directory holding the persisted ledger.
	// When rollback leaves failures, this directory is deliberately preserved
	// so creation-state.json remains available for a second recovery pass;
	// on a fully clean rollback it is removed like any other local path.
	StateDir string
	// Sleep is overridable in tests to avoid real waiting.
	Sleep func(ctx context.Context, d time.Duration) error
}

func (o *Options) withDefaults() {
	if o.Logger == nil {
		o.Logger = log.NoopLogger{}
	}
	if o.Timeout <= 0 {
		o.Timeout = DefaultTimeout
	}
	if o.MaxAttempts <= 0 {
		o.MaxAttempts = DefaultMaxAttempts
	}
	if o.RetryDelay <= 0 {
		o.RetryDelay = DefaultRetryDelay
	}
	if o.MaxRetryDelay <= 0 {
		o.MaxRetryDelay = DefaultMaxRetryDelay
	}
	if o.LogExportTimeout <= 0 {
		o.LogExportTimeout = DefaultLogExportTimeout
	}
	if o.Exporter == nil {
		o.Exporter = noopExporter{}
	}
	if o.LogsBaseDir == "" {
		o.LogsBaseDir = os.TempDir()
	}
	if o.Sleep == nil {
		o.Sleep = sleepCtx
	}
}

func sleepCtx(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}

// Result reports what rollback did.
type Result struct {
	// LogsDir is non-empty when logs were exported.
	LogsDir string
	// Failures are the per-resource errors that could not be resolved.
	Failures []error
	// Leftovers are the owned resources still visible afterwards.
	Leftovers []Resource
}

// Rollbacker performs bounded, ownership-aware cleanup.
type Rollbacker struct {
	rt Runtime
	o  Options
}

// NewRollbacker builds a Rollbacker for runtime rt.
func NewRollbacker(rt Runtime, opts Options) *Rollbacker {
	opts.withDefaults()
	return &Rollbacker{rt: rt, o: opts}
}

// Rollback deletes every resource owned by the ledger.
//
// Ordering (dependency order, see DESIGN.md §5):
//  1. best-effort node log export (bounded, never blocking);
//  2. stop all owned containers, then remove them (reverse creation order);
//  3. remove owned volumes (explicit and implicit);
//  4. remove owned networks;
//  5. remove local paths (files first, then directories).
//
// A failure on one resource never stops the others; all failures are
// collected and returned together. "Not found" is treated as success at every
// step; "in use" and other transient errors get bounded retries.
func (rb *Rollbacker) Rollback(parentCtx context.Context, l *Ledger) Result {
	res := Result{}

	// Rollback deliberately uses its own budgeted context instead of the
	// creation context: a creation cancelled by a signal must still be cleaned
	// up. The hard Timeout is the guarantee that cleanup cannot hang forever.
	ctx, cancel := context.WithTimeout(context.Background(), rb.o.Timeout)
	defer cancel()
	// But if the *parent* is still alive and gets cancelled, respect it too.
	go func() {
		select {
		case <-parentCtx.Done():
			cancel()
		case <-ctx.Done():
		}
	}()

	// adopt resources from a lost ledger before touching anything
	if err := l.ReconcileOrphans(ctx, rb.rt); err != nil {
		rb.o.Logger.Warnf("rollback: orphan reconciliation failed, continuing with in-memory ledger: %v", err)
	}

	current, err := rb.rt.Snapshot(ctx)
	if err != nil {
		// Without a snapshot we cannot run the ownership gate safely. Keep
		// going only for ledger resources whose delete calls themselves
		// return not-found; report the snapshot problem loudly.
		res.Failures = append(res.Failures, err)
	}

	// 1. best-effort log export
	res.LogsDir = rb.exportLogs(ctx, l)

	// 2. containers: stop first, then remove
	containers := l.ByKind(KindContainer)
	for _, e := range containers {
		if !l.OwnedByThisAttempt(e.Resource, current) {
			continue
		}
		if err := rb.retry(ctx, "stop container "+e.Resource.Name, func(ctx context.Context) error {
			err := rb.rt.StopContainer(ctx, e.Resource.Name)
			if IsNotFound(err) {
				return nil
			}
			return err
		}); err != nil {
			res.Failures = append(res.Failures, err)
		}
	}
	// removal in reverse creation order (dependents before their dependencies)
	for i := len(containers) - 1; i >= 0; i-- {
		e := containers[i]
		if !l.OwnedByThisAttempt(e.Resource, current) {
			continue
		}
		if err := rb.retry(ctx, "remove container "+e.Resource.Name, func(ctx context.Context) error {
			err := rb.rt.RemoveContainer(ctx, e.Resource.Name)
			if IsNotFound(err) {
				return nil
			}
			return err
		}); err != nil {
			res.Failures = append(res.Failures, err)
		}
	}

	// refresh snapshot so network/volume deletes see the containers gone
	if refreshed, err := rb.rt.Snapshot(ctx); err == nil {
		current = refreshed
	}

	// 3. volumes (reverse order as well — anonymous volumes after named ones)
	volumes := l.ByKind(KindVolume)
	for i := len(volumes) - 1; i >= 0; i-- {
		e := volumes[i]
		if !l.OwnedByThisAttempt(e.Resource, current) {
			continue
		}
		if err := rb.retry(ctx, "remove volume "+e.Resource.Name, func(ctx context.Context) error {
			err := rb.rt.RemoveVolume(ctx, e.Resource.Name)
			if IsNotFound(err) {
				return nil
			}
			return err
		}); err != nil {
			res.Failures = append(res.Failures, err)
		}
	}

	// 4. networks
	for _, e := range l.ByKind(KindNetwork) {
		if !l.OwnedByThisAttempt(e.Resource, current) {
			continue
		}
		if err := rb.retry(ctx, "remove network "+e.Resource.Name, func(ctx context.Context) error {
			err := rb.rt.RemoveNetwork(ctx, e.Resource.Name)
			if IsNotFound(err) {
				return nil
			}
			return err
		}); err != nil {
			res.Failures = append(res.Failures, err)
		}
	}

	// 5. local paths: files before directories, directories after everything.
	// The state directory is special: when something earlier failed, keep it
	// (and the persisted ledger inside) for a later recovery pass.
	paths := l.ByKind(KindLocalPath)
	var dirs []LedgerEntry
	for _, e := range paths {
		info, statErr := os.Stat(e.Resource.Name)
		if os.IsNotExist(statErr) {
			continue // recorded intent that never materialized: success
		}
		if statErr != nil {
			res.Failures = append(res.Failures, statErr)
			continue
		}
		if info.IsDir() {
			dirs = append(dirs, e)
			continue
		}
		if rb.o.StateDir != "" && e.Resource.Name == filepath.Join(rb.o.StateDir, StateFileName) &&
			len(res.Failures) > 0 {
			continue // preserve the recovery trail
		}
		if err := os.Remove(e.Resource.Name); err != nil && !os.IsNotExist(err) {
			res.Failures = append(res.Failures, err)
		}
	}
	for i := len(dirs) - 1; i >= 0; i-- {
		name := dirs[i].Resource.Name
		if rb.o.StateDir != "" && name == rb.o.StateDir && len(res.Failures) > 0 {
			rb.o.Logger.V(1).Infof("rollback: keeping state directory %s for recovery after partial failure", name)
			continue
		}
		if err := os.RemoveAll(name); err != nil {
			res.Failures = append(res.Failures, err)
		}
	}

	// final inventory of what remains
	if finalSnap, err := rb.rt.Snapshot(ctx); err == nil {
		res.Leftovers = rb.leftovers(l, finalSnap)
	} else {
		res.Leftovers = rb.leftovers(l, current)
	}
	return res
}

// retry runs op until it succeeds, returns a non-transient error, the attempt
// budget is exhausted, or the context deadline fires. ErrInUse and other
// transient failures are retried with capped exponential backoff; ErrNotFound
// is converted to success by the caller wrappers.
func (rb *Rollbacker) retry(ctx context.Context, what string, op func(context.Context) error) error {
	delay := rb.o.RetryDelay
	var lastErr error
	for attempt := 1; attempt <= rb.o.MaxAttempts; attempt++ {
		if err := ctx.Err(); err != nil {
			if lastErr != nil {
				return lastErr
			}
			return err
		}
		err := op(ctx)
		if err == nil {
			return nil
		}
		lastErr = err
		// not-found was already normalized by the wrappers; anything that
		// reaches here is either transient or terminal. We retry regardless
		// until the budget runs out — runtime errors such as a momentarily
		// unavailable daemon look identical to ErrInUse from here, and the
		// small budget bounds the cost.
		rb.o.Logger.V(1).Infof("rollback: %s attempt %d/%d failed: %v", what, attempt, rb.o.MaxAttempts, err)
		if attempt == rb.o.MaxAttempts {
			break
		}
		if sleepErr := rb.o.Sleep(ctx, delay); sleepErr != nil {
			return lastErr
		}
		delay *= 2
		if delay > rb.o.MaxRetryDelay {
			delay = rb.o.MaxRetryDelay
		}
	}
	return lastErr
}

// exportLogs runs the configured hook under its own deadline. Failures are
// logged and swallowed so they never block or cancel rollback.
func (rb *Rollbacker) exportLogs(ctx context.Context, l *Ledger) string {
	containers := l.ByKind(KindContainer)
	if len(containers) == 0 {
		return ""
	}
	dir, err := os.MkdirTemp(rb.o.LogsBaseDir, "kind-failed-logs-")
	if err != nil {
		rb.o.Logger.Warnf("rollback: cannot create log export directory: %v", err)
		return ""
	}
	logCtx, cancel := context.WithTimeout(ctx, rb.o.LogExportTimeout)
	defer cancel()
	if err := rb.o.Exporter.ExportLogs(logCtx, dir, resourceList(containers)); err != nil {
		rb.o.Logger.Warnf("rollback: log export failed (continuing with cleanup): %v", err)
		// remove the empty/partial directory so we do not leave junk behind
		if rmErr := os.RemoveAll(dir); rmErr != nil {
			rb.o.Logger.Warnf("rollback: failed to remove partial log dir %s: %v", dir, rmErr)
			return dir
		}
		return ""
	}
	rb.o.Logger.V(0).Infof("node logs exported to %s", dir)
	return dir
}

func (rb *Rollbacker) leftovers(l *Ledger, snap Snapshot) []Resource {
	var out []Resource
	for _, e := range l.Entries() {
		if e.Resource.Kind == KindLocalPath {
			if _, err := os.Stat(e.Resource.Name); err == nil {
				out = append(out, e.Resource)
			}
			continue
		}
		if _, ok := snap.NamesByKind(e.Resource.Kind)[e.Resource.Name]; ok {
			out = append(out, e.Resource)
		}
	}
	return out
}

func resourceList(entries []LedgerEntry) []Resource {
	out := make([]Resource, 0, len(entries))
	for _, e := range entries {
		out = append(out, e.Resource)
	}
	return out
}
