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

// Package rollback contains the design and implementation of bounded,
// ownership-aware cleanup of resources left behind by a failed cluster
// creation.
//
// The package is intentionally split from the concrete docker / podman
// providers: everything here depends only on the small Runtime interface
// declared in runtime.go, which can be driven by the real container runtime
// or by an in-memory fake used in tests (see the fakeruntime sub-package).
//
// See DESIGN.md for the full rationale, in particular:
//   - why ownership is recorded as an *intent ledger* written before each
//     resource is created, cross-checked with attempt-scoped labels
//   - why a pre-existing cluster with the same name must fail creation early
//     and is never touched by rollback
//   - how forced process termination is handled
//   - how rollback is bounded in time and how it preserves the original
//     creation error
package rollback

const (
	// AttemptIDLabel is written onto every runtime resource (container,
	// network, volume) created by one create invocation. Its value is the
	// unique attempt ID, so two concurrent or retried creations never share a
	// label value even when they use the same resource names.
	AttemptIDLabel = "io.x-k8s.kind.create-attempt"
	// ClusterLabel records which cluster name a resource was created for.
	// It mirrors the existing provider label ("io.x-k8s.kind.cluster") and is
	// used by the orphan adoption path.
	ClusterLabel = "io.x-k8s.kind.rollback-cluster"
)
