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

package fakeruntime

import (
	"context"
	"os"
	"path/filepath"

	"sigs.k8s.io/kind/pkg/cluster/internal/rollback"
)

// LogExporter is a file-based rollback.LogExporter for tests: it writes each
// node container's simulated logs to dir/<node>/logs.txt. Fail can be set to
// prove a failed export never blocks rollback.
type LogExporter struct {
	rt   *FakeRuntime
	Fail error
	// Calls counts successful and failed export invocations.
	Calls int
}

// NewLogExporter builds one.
func NewLogExporter(rt *FakeRuntime) *LogExporter {
	return &LogExporter{rt: rt}
}

// ExportLogs implements rollback.LogExporter.
func (e *LogExporter) ExportLogs(ctx context.Context, dir string, containers []rollback.Resource) error {
	e.Calls++
	if e.Fail != nil {
		return e.Fail
	}
	for _, c := range containers {
		if err := ctx.Err(); err != nil {
			return err
		}
		logs, err := e.rt.ContainerLogs(ctx, c.Name)
		if err != nil {
			return err
		}
		nodeDir := filepath.Join(dir, c.Name)
		if err := os.MkdirAll(nodeDir, 0o755); err != nil {
			return err
		}
		if err := os.WriteFile(filepath.Join(nodeDir, "logs.txt"), logs, 0o600); err != nil {
			return err
		}
	}
	return nil
}
