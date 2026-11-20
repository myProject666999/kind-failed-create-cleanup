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
	stderrors "errors"
	"strings"
)

// multiError keeps several failures together without hiding any of them.
// Unlike sigs.k8s.io/kind/pkg/errors.Aggregate it implements the standard
// library Unwrap() []error contract so errors.Is works through the aggregate.
type multiError struct {
	msg  string
	errs []error
}

func (m *multiError) Error() string {
	var b strings.Builder
	b.WriteString(m.msg)
	for _, e := range m.errs {
		b.WriteString("\n  - ")
		b.WriteString(e.Error())
	}
	return b.String()
}

// Unwrap supports errors.Is/errors.As (Go 1.20+ multi-error contract).
func (m *multiError) Unwrap() []error { return m.errs }

// Is keeps errors.Is working on older toolchains that only inspect Is.
func (m *multiError) Is(target error) bool {
	for _, e := range m.errs {
		if stderrors.Is(e, target) {
			return true
		}
	}
	return false
}

// Aggregate keeps several failures together without hiding any of them.
// Unlike sigs.k8s.io/kind/pkg/errors.Aggregate it implements the standard
// library Unwrap() []error contract so errors.Is works through the aggregate.
func Aggregate(msg string, errs []error) error {
	filtered := make([]error, 0, len(errs))
	for _, e := range errs {
		if e != nil {
			filtered = append(filtered, e)
		}
	}
	if len(filtered) == 0 {
		return nil
	}
	return &multiError{msg: msg, errs: filtered}
}

// CreateError preserves BOTH failure reasons: the original creation failure
// and any failures encountered while rolling back. Neither may overwrite the
// other — the user needs the creation error to fix their config/node image,
// and the rollback errors to know what still needs manual cleanup.
type CreateError struct {
	// CreateErr is why creation failed.
	CreateErr error
	// RollbackErr is aggregate cleanup failure (nil on full cleanup or when
	// Retain skipped rollback).
	RollbackErr error
	// Retained is true when rollback was intentionally skipped.
	Retained bool
	// Leftovers lists resources still present when rollback gave up, or all
	// owned resources when Retained is set.
	Leftovers []Resource
	// LogsDir is set when node logs were exported before rollback.
	LogsDir string
}

func (e *CreateError) Error() string {
	var b strings.Builder
	b.WriteString("cluster creation failed: ")
	b.WriteString(e.CreateErr.Error())
	if e.Retained {
		b.WriteString("\nrollback skipped (retain requested); resources were left in place")
	} else if e.RollbackErr != nil {
		b.WriteString("\nautomatic rollback encountered errors: ")
		b.WriteString(e.RollbackErr.Error())
	}
	if e.LogsDir != "" {
		b.WriteString("\nnode logs exported to: ")
		b.WriteString(e.LogsDir)
	}
	if len(e.Leftovers) > 0 {
		b.WriteString("\nleftover resources:")
		for _, r := range e.Leftovers {
			b.WriteString("\n  - ")
			b.WriteString(string(r.Kind))
			b.WriteString("/")
			b.WriteString(r.Name)
		}
	}
	return b.String()
}

// Unwrap implements the Go 1.20+ multi-error contract: both the original
// creation error and the rollback aggregate remain reachable via errors.Is /
// errors.As, so neither failure can mask the other.
func (e *CreateError) Unwrap() []error {
	out := make([]error, 0, 2)
	if e.CreateErr != nil {
		out = append(out, e.CreateErr)
	}
	if e.RollbackErr != nil {
		out = append(out, e.RollbackErr)
	}
	return out
}
