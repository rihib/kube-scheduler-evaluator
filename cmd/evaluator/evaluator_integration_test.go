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

// TestRun_WritesOneRecordFilePerRun covers one real run of the evaluator (units/U01.md:
// the record's output directory is real, and the computer information is real; only
// the evaluator commit is irrelevant here and left to its real implementation too,
// since it does not depend on any external resource). It checks that exactly one JSONL
// file is created, and that the real file.Writer preserves the same operation sequence
// the unit tests check through the Fake writer (create_task -> assign_node -> start ->
// complete -> release), so a divergence between the Fake and the real Writer (a dropped
// line, a reordering, a Close that truncates the file) would show up here instead of
// only in production.
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

	var lines []map[string]any
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		var line map[string]any
		if err := json.Unmarshal(scanner.Bytes(), &line); err != nil {
			t.Fatalf("line %d is not valid JSON: %v", len(lines)+1, err)
		}
		lines = append(lines, line)
	}
	if err := scanner.Err(); err != nil {
		t.Fatalf("failed to scan record file: %v", err)
	}
	if len(lines) == 0 {
		t.Fatalf("record file has no header line")
	}
	header := lines[0]
	if _, ok := header["input"]; !ok {
		t.Errorf("header line has no input section: %v", header)
	}

	// findOps returns, in order, the seq of every transition line with the given operation.
	findOps := func(op string) []float64 {
		var seqs []float64
		for _, l := range lines[1:] {
			if l["operation"] == op {
				seqs = append(seqs, mustFloat(t, l["seq"], "seq"))
			}
		}
		return seqs
	}
	create, assign, start, complete, release := findOps("create_task"), findOps("assign_node"), findOps("start"), findOps("complete"), findOps("release")
	for name, seqs := range map[string][]float64{
		"create_task": create, "assign_node": assign, "start": start, "complete": complete, "release": release,
	} {
		if len(seqs) == 0 {
			t.Fatalf("no %s record found in the written file: %v", name, lines)
		}
	}
	if !(create[0] < assign[0] && assign[0] < start[0] && start[0] < complete[0] && complete[0] <= release[0]) {
		t.Errorf("records are not in the expected order: create=%v assign=%v start=%v complete=%v release=%v",
			create[0], assign[0], start[0], complete[0], release[0])
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
