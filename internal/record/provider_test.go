package record

import (
	"runtime/debug"
	"testing"
)

// TestEvaluatorCommitFrom covers how the evaluator's own commit is read out of the
// build information of the running binary (units/U01.md: evaluator_commit and
// reference_commit must be distinguishable). The branch that matters in production is
// the one where the evaluator is only a dependency of the reference repository
// (decisions.md), which a "go test" binary of this module can never take, so the
// decision is made from a *debug.BuildInfo value instead of the ambient one.
func TestEvaluatorCommitFrom(t *testing.T) {
	const forkPseudoVersion = "v0.0.0-20261121153000-0123456789ab"

	t.Run("the evaluator itself is the main module", func(t *testing.T) {
		commit, found, err := evaluatorCommitFrom(&debug.BuildInfo{
			Main:     debug.Module{Path: evaluatorModulePath},
			Settings: []debug.BuildSetting{{Key: "vcs.revision", Value: "abc123"}},
		})
		if err != nil {
			t.Fatalf("evaluatorCommitFrom: %v", err)
		}
		if !found || commit != "abc123" {
			t.Errorf("got (%q, %v), want (\"abc123\", true)", commit, found)
		}
	})

	t.Run("the main module has no vcs.revision", func(t *testing.T) {
		commit, found, err := evaluatorCommitFrom(&debug.BuildInfo{
			Main: debug.Module{Path: evaluatorModulePath},
		})
		if err != nil {
			t.Fatalf("evaluatorCommitFrom: %v", err)
		}
		if found {
			t.Errorf("got (%q, true), want found = false so that the caller falls back to git", commit)
		}
	})

	t.Run("the reference pins the evaluator's fork through a replace directive", func(t *testing.T) {
		commit, found, err := evaluatorCommitFrom(&debug.BuildInfo{
			Main: debug.Module{Path: "github.com/rihib/kube-scheduler-evaluator-reference"},
			Deps: []*debug.Module{{
				Path:    evaluatorModulePath,
				Version: "v0.0.0-00010101000000-000000000000",
				Replace: &debug.Module{
					Path:    "github.com/rihib/kube-scheduler-evaluator",
					Version: forkPseudoVersion,
				},
			}},
		})
		if err != nil {
			t.Fatalf("evaluatorCommitFrom: %v", err)
		}
		if !found || commit != forkPseudoVersion {
			t.Errorf("got (%q, %v), want (%q, true): the commit is the replacement's version, not the required one",
				commit, found, forkPseudoVersion)
		}
	})

	t.Run("the reference pins the evaluator at a version that identifies no commit", func(t *testing.T) {
		commit, _, err := evaluatorCommitFrom(&debug.BuildInfo{
			Main: debug.Module{Path: "github.com/rihib/kube-scheduler-evaluator-reference"},
			Deps: []*debug.Module{{
				Path:    evaluatorModulePath,
				Version: "v0.0.0-00010101000000-000000000000",
			}},
		})
		if err == nil {
			t.Errorf("evaluatorCommitFrom returned %q, want an error: the placeholder version is not a commit", commit)
		}
	})

	t.Run("the evaluator is not among the main module's dependencies", func(t *testing.T) {
		commit, _, err := evaluatorCommitFrom(&debug.BuildInfo{
			Main: debug.Module{Path: "github.com/rihib/kube-scheduler-evaluator-reference"},
		})
		if err == nil {
			t.Errorf("evaluatorCommitFrom returned %q, want an error", commit)
		}
	})
}
