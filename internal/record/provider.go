package record

import (
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"runtime/debug"
	"strings"
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
// under dir. The file name identifies the run so that concurrent or repeated runs do
// not overwrite each other's records.
func NewFileWriterFactory(dir string) WriterFactory {
	return func(info RunInfo) (Writer, error) {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return nil, fmt.Errorf("record.NewFileWriterFactory: failed to create directory %q: %w", dir, err)
		}
		name := fmt.Sprintf("%s-%s-iteration%d.jsonl", info.EvaluationID, info.ScenarioID, info.Iteration)
		f, err := os.Create(filepath.Join(dir, name))
		if err != nil {
			return nil, fmt.Errorf("record.NewFileWriterFactory: failed to create file %q: %w", name, err)
		}
		return &fileWriter{f: f, enc: json.NewEncoder(f)}, nil
	}
}

// EvaluatorCommit returns the evaluator's own commit.
//
// A binary built with "go build" embeds its VCS revision in its build info.
// A "go test" binary does not, so this falls back to asking git directly;
// that fallback also covers test runs, where no VCS-stamped binary exists.
func EvaluatorCommit() (string, error) {
	if info, ok := debug.ReadBuildInfo(); ok {
		for _, s := range info.Settings {
			if s.Key == "vcs.revision" {
				return s.Value, nil
			}
		}
	}
	out, err := exec.Command("git", "rev-parse", "HEAD").Output()
	if err != nil {
		return "", fmt.Errorf("record.EvaluatorCommit: build info has no vcs.revision, and git rev-parse HEAD failed: %w", err)
	}
	return strings.TrimSpace(string(out)), nil
}

// HardwareInfo returns a short description of the machine running the evaluator.
func HardwareInfo() (string, error) {
	hostname, err := os.Hostname()
	if err != nil {
		hostname = "unknown"
	}
	return fmt.Sprintf("os=%s arch=%s cpus=%d hostname=%s", runtime.GOOS, runtime.GOARCH, runtime.NumCPU(), hostname), nil
}
