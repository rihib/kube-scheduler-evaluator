package record

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"runtime"
	"runtime/debug"
	"strings"
	"time"
)

type fileWriter struct {
	f   *os.File
	enc *json.Encoder
}

func (w *fileWriter) WriteLine(v any) error {
	if err := w.enc.Encode(v); err != nil {
		return fmt.Errorf("record.fileWriter.WriteLine: %w", err)
	}
	return nil
}

func (w *fileWriter) Close() error {
	return w.f.Close()
}

// NewFileWriterFactory returns a WriterFactory that creates one JSONL file per run
// under dir. The file name identifies the run and carries a nanosecond timestamp, and
// the file is opened exclusively, so that two runs of the same scenario and iteration
// (a repeated run, or two processes that happen to start within the same evaluationID's
// second) fail loudly with a file-exists error instead of one silently overwriting the
// other's record.
func NewFileWriterFactory(dir string) WriterFactory {
	return func(info RunInfo) (Writer, error) {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return nil, fmt.Errorf("record.NewFileWriterFactory: failed to create directory %q: %w", dir, err)
		}
		name := fmt.Sprintf("%s-%s-iteration%d-%d.jsonl", info.EvaluationID, info.ScenarioID, info.Iteration, time.Now().UnixNano())
		f, err := os.OpenFile(filepath.Join(dir, name), os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o644)
		if err != nil {
			return nil, fmt.Errorf("record.NewFileWriterFactory: failed to create file %q: %w", name, err)
		}
		return &fileWriter{f: f, enc: json.NewEncoder(f)}, nil
	}
}

// evaluatorModulePath is this module's own import path, used to tell whether a binary
// IS the evaluator (built from this repository) or merely imports it as a dependency
// (e.g. the reference repository that runs evaluations, see decisions.md's 「全体の形」).
const evaluatorModulePath = "github.com/pfnet/kube-scheduler-evaluator"

// EvaluatorCommit returns the evaluator's own commit, regardless of whether the
// running binary is this module's own build or imports this module as a dependency.
//
// When the evaluator is only a dependency (the reference repository's case,
// decisions.md), debug.ReadBuildInfo's vcs.revision is the CALLER's commit, not the
// evaluator's: the evaluator's own commit is instead the pseudo-version this module is
// pinned to in the caller's build (info.Deps, see evaluatorCommitFrom), per
// units/U01.md's requirement that evaluator_commit and reference_commit be
// distinguishable.
//
// When the evaluator itself is the main module, vcs.revision is usable, except in a
// "go test" binary, which does not carry one; that case falls back to asking git
// directly, which gives the right answer only because the test runs inside the
// evaluator's own repository.
func EvaluatorCommit() (string, error) {
	if info, ok := debug.ReadBuildInfo(); ok {
		commit, found, err := evaluatorCommitFrom(info)
		if err != nil {
			return "", err
		}
		if found {
			return commit, nil
		}
	}
	out, err := exec.Command("git", "rev-parse", "HEAD").Output()
	if err != nil {
		return "", fmt.Errorf("record.EvaluatorCommit: build info has no usable vcs.revision, and git rev-parse HEAD failed: %w", err)
	}
	return strings.TrimSpace(string(out)), nil
}

// evaluatorCommitFrom reads the evaluator's own commit out of one build information
// value. found is false when the build information does not carry the commit even
// though nothing is wrong with it, which happens in a "go test" binary of this module.
func evaluatorCommitFrom(info *debug.BuildInfo) (commit string, found bool, err error) {
	if info.Main.Path == evaluatorModulePath {
		rev, ok := vcsRevision(info.Settings)
		return rev, ok, nil
	}
	for _, dep := range info.Deps {
		if dep.Path != evaluatorModulePath {
			continue
		}
		// The reference pins the fork of this module, whose repository URL differs from
		// this module's path, so it needs a replace directive (decisions.md). The
		// version that identifies the commit is then the replacement's: the required
		// version is whatever satisfied the module graph, or the placeholder the go
		// command writes when only the replace directive brings the module in.
		version := dep.Version
		if dep.Replace != nil {
			version = dep.Replace.Version
		}
		if !isPseudoVersion(version) {
			return "", false, fmt.Errorf(
				"record.EvaluatorCommit: the main module %s pins %s at %q, which identifies no commit; decisions.md requires a pseudo-version of the evaluator fork's commit",
				info.Main.Path, evaluatorModulePath, version)
		}
		return version, true, nil
	}
	return "", false, fmt.Errorf("record.EvaluatorCommit: %s is not among the main module %s's dependencies", evaluatorModulePath, info.Main.Path)
}

// pseudoVersionPattern matches a Go pseudo-version, whose last two fields are the
// commit's timestamp and the 12-character prefix of its hash (golang.org/ref/mod).
var pseudoVersionPattern = regexp.MustCompile(`^v[0-9]+\.[0-9]+\.[0-9]+(-[0-9A-Za-z.-]+)?-[0-9]{14}-[0-9a-f]{12}$`)

// placeholderPseudoVersion is the pseudo-version shape the go command writes for a
// module that only a replace directive brings into the build. It parses as a
// pseudo-version but points at no commit.
const placeholderPseudoVersion = "v0.0.0-00010101000000-000000000000"

// isPseudoVersion reports whether version identifies a commit of this module.
func isPseudoVersion(version string) bool {
	return version != placeholderPseudoVersion && pseudoVersionPattern.MatchString(version)
}

func vcsRevision(settings []debug.BuildSetting) (string, bool) {
	for _, s := range settings {
		if s.Key == "vcs.revision" {
			return s.Value, true
		}
	}
	return "", false
}

// HardwareInfo returns a short description of the machine running the evaluator.
// The hostname is informational only, so a failure to read it does not fail the whole
// run; it is logged instead of being silently replaced, so the reason is not lost.
func HardwareInfo() (string, error) {
	hostname, err := os.Hostname()
	if err != nil {
		slog.Warn("record.HardwareInfo: failed to get hostname", "error", err)
		hostname = "unknown"
	}
	return fmt.Sprintf("os=%s arch=%s cpus=%d hostname=%s", runtime.GOOS, runtime.GOARCH, runtime.NumCPU(), hostname), nil
}
