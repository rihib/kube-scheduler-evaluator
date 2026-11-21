//go:build integration

package evaluator

import (
	"bufio"
	"context"
	"encoding/json"
	"log/slog"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/pfnet/kube-scheduler-evaluator/pkg/definition"
)

// TestRun_WritesOneRecordFilePerRun covers one real run of the evaluator (decisions.md:
// the record's output directory is real, and the computer information is real; only
// the evaluator commit is irrelevant here and left to its real implementation too,
// since it does not depend on any external resource). It checks that exactly one JSONL
// file is created, and that it has a header line followed by transition lines.
func TestRun_WritesOneRecordFilePerRun(t *testing.T) {
	dir := t.TempDir()
	cfg := WithConfig(
		testUserID, "", "", "", false, slog.LevelError,
		WithRecordOutputDir(dir),
	)
	eval, err := New(
		cfg,
		WithMetricStores(),
		WithScenario("integration", 1, func(ch chan<- definition.Event) {
			ch <- definition.NewEvent(definition.EventTypeCreate, testNode("4", "8Gi"), 0)
			ch <- definition.NewEvent(definition.EventTypeCreate, testReplicaSet("job", "1", 0, time.Second), 0)
		}, nil, testIdentity()),
	)
	if err != nil {
		t.Fatalf("evaluator.New: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if code := eval.Run(ctx); code != 0 {
		t.Fatalf("evaluator.Run: exit code %d", code)
	}

	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("failed to read output dir: %v", err)
	}
	if len(entries) != 1 {
		t.Fatalf("expected exactly one record file, got %d: %v", len(entries), entries)
	}

	f, err := os.Open(filepath.Join(dir, entries[0].Name()))
	if err != nil {
		t.Fatalf("failed to open record file: %v", err)
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)
	if !scanner.Scan() {
		t.Fatalf("record file has no header line")
	}
	var header map[string]any
	if err := json.Unmarshal(scanner.Bytes(), &header); err != nil {
		t.Fatalf("header line is not valid JSON: %v", err)
	}
	if _, ok := header["input"]; !ok {
		t.Errorf("header line has no input section: %v", header)
	}

	if !scanner.Scan() {
		t.Fatalf("record file has no transition lines after the header")
	}
	var transition map[string]any
	if err := json.Unmarshal(scanner.Bytes(), &transition); err != nil {
		t.Fatalf("transition line is not valid JSON: %v", err)
	}
	if _, ok := transition["operation"]; !ok {
		t.Errorf("transition line has no operation: %v", transition)
	}
}

// TestRun_FailsWhenRecordOutputDirIsUnwritable covers the evaluator reporting failure
// when it cannot write the record, instead of silently dropping it.
func TestRun_FailsWhenRecordOutputDirIsUnwritable(t *testing.T) {
	// A file, not a directory: os.MkdirAll inside the writer factory will fail on it.
	unwritable := filepath.Join(t.TempDir(), "not-a-directory")
	if err := os.WriteFile(unwritable, []byte("x"), 0o644); err != nil {
		t.Fatalf("failed to set up the test file: %v", err)
	}

	cfg := WithConfig(
		testUserID, "", "", "", false, slog.LevelError,
		WithRecordOutputDir(filepath.Join(unwritable, "child")),
	)
	eval, err := New(
		cfg,
		WithMetricStores(),
		WithScenario("integration", 1, func(ch chan<- definition.Event) {
			ch <- definition.NewEvent(definition.EventTypeCreate, testNode("4", "8Gi"), 0)
		}, nil, testIdentity()),
	)
	if err != nil {
		t.Fatalf("evaluator.New: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if code := eval.Run(ctx); code == 0 {
		t.Fatalf("evaluator.Run: expected a non-zero exit code when the record output dir is unwritable, got 0")
	}
}
