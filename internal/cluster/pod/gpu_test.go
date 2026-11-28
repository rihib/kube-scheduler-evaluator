package pod

import "testing"

// TestNodeGPUCapacityOf covers NodeGPUCapacityOf's error branches and its boundary
// cases: an explicit gpu-count of "0", and no CountAnnotationKey annotation at all.
func TestNodeGPUCapacityOf(t *testing.T) {
	tests := []struct {
		name        string
		annotations map[string]string
		want        map[string]string
		wantErr     bool
	}{
		{
			name:        "no CountAnnotationKey annotation at all: not a GPU node",
			annotations: map[string]string{},
			want:        nil,
		},
		{
			name:        "CountAnnotationKey is 0: a GPU node registered with no GPUs",
			annotations: map[string]string{CountAnnotationKey: "0"},
			want:        map[string]string{},
		},
		{
			name:        "CountAnnotationKey is 2: one entry per GPU",
			annotations: map[string]string{CountAnnotationKey: "2"},
			want:        map[string]string{"gpu-0": "1000", "gpu-1": "1000"},
		},
		{
			name:        "CountAnnotationKey is not a number",
			annotations: map[string]string{CountAnnotationKey: "abc"},
			wantErr:     true,
		},
		{
			name:        "CountAnnotationKey is negative",
			annotations: map[string]string{CountAnnotationKey: "-1"},
			wantErr:     true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := NodeGPUCapacityOf(tt.annotations)
			if tt.wantErr {
				if err == nil {
					t.Fatalf("NodeGPUCapacityOf(%v) = %v, <nil>, want an error", tt.annotations, got)
				}
				return
			}
			if err != nil {
				t.Fatalf("NodeGPUCapacityOf(%v) returned an unexpected error: %v", tt.annotations, err)
			}
			if len(got) != len(tt.want) {
				t.Fatalf("NodeGPUCapacityOf(%v) = %v, want %v", tt.annotations, got, tt.want)
			}
			for k, v := range tt.want {
				if got[k] != v {
					t.Errorf("NodeGPUCapacityOf(%v)[%q] = %q, want %q", tt.annotations, k, got[k], v)
				}
			}
		})
	}
}

// TestGPUAllocatedResourceOf covers gpuAllocatedResourceOf's error branches (a GPU
// request with no index, a malformed milli share, a malformed or inconsistent index)
// and its boundary cases (an explicit gpu-index of "" and an explicit gpu-count of "0").
func TestGPUAllocatedResourceOf(t *testing.T) {
	tests := []struct {
		name        string
		annotations map[string]string
		want        map[string]string
		wantErr     bool
	}{
		{
			name:        "no GPU annotation at all: not a GPU Pod",
			annotations: map[string]string{},
			want:        nil,
		},
		{
			name:        "IndexAnnotationKey is explicitly empty, with no milli or count: not placed on a GPU",
			annotations: map[string]string{IndexAnnotationKey: ""},
			want:        nil,
		},
		{
			name:        "one GPU, less than a whole one",
			annotations: map[string]string{IndexAnnotationKey: "0", MilliAnnotationKey: "500"},
			want:        map[string]string{"gpu-0": "500"},
		},
		{
			name: "two whole GPUs, their count matches the index count",
			annotations: map[string]string{
				IndexAnnotationKey: "0-1", MilliAnnotationKey: "1000", CountAnnotationKey: "2",
			},
			want: map[string]string{"gpu-0": "1000", "gpu-1": "1000"},
		},
		{
			name:        "MilliAnnotationKey is 0: a GPU Pod asking for no share of it",
			annotations: map[string]string{IndexAnnotationKey: "0", MilliAnnotationKey: "0"},
			want:        map[string]string{"gpu-0": "0"},
		},
		{
			name:        "MilliAnnotationKey carries a leading plus: recorded as the plain number",
			annotations: map[string]string{IndexAnnotationKey: "0", MilliAnnotationKey: "+500"},
			want:        map[string]string{"gpu-0": "500"},
		},
		{
			name:        "MilliAnnotationKey is set but IndexAnnotationKey is missing",
			annotations: map[string]string{MilliAnnotationKey: "500"},
			wantErr:     true,
		},
		{
			name:        "CountAnnotationKey is set but IndexAnnotationKey is missing",
			annotations: map[string]string{CountAnnotationKey: "1"},
			wantErr:     true,
		},
		{
			name:        "IndexAnnotationKey is set but MilliAnnotationKey is missing",
			annotations: map[string]string{IndexAnnotationKey: "0"},
			wantErr:     true,
		},
		{
			name:        "MilliAnnotationKey is not a number",
			annotations: map[string]string{IndexAnnotationKey: "0", MilliAnnotationKey: "500m"},
			wantErr:     true,
		},
		{
			name:        "MilliAnnotationKey is negative",
			annotations: map[string]string{IndexAnnotationKey: "0", MilliAnnotationKey: "-500"},
			wantErr:     true,
		},
		{
			name:        "an index part is not a number",
			annotations: map[string]string{IndexAnnotationKey: "0-abc", MilliAnnotationKey: "1000"},
			wantErr:     true,
		},
		{
			name: "the index count does not match CountAnnotationKey",
			annotations: map[string]string{
				IndexAnnotationKey: "0-1", MilliAnnotationKey: "1000", CountAnnotationKey: "1",
			},
			wantErr: true,
		},
		{
			name: "the same GPU number appears twice in IndexAnnotationKey",
			annotations: map[string]string{
				IndexAnnotationKey: "0-0", MilliAnnotationKey: "1000", CountAnnotationKey: "2",
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := gpuAllocatedResourceOf(tt.annotations)
			if tt.wantErr {
				if err == nil {
					t.Fatalf("gpuAllocatedResourceOf(%v) = %v, <nil>, want an error", tt.annotations, got)
				}
				return
			}
			if err != nil {
				t.Fatalf("gpuAllocatedResourceOf(%v) returned an unexpected error: %v", tt.annotations, err)
			}
			if len(got) != len(tt.want) {
				t.Fatalf("gpuAllocatedResourceOf(%v) = %v, want %v", tt.annotations, got, tt.want)
			}
			for k, v := range tt.want {
				if got[k] != v {
					t.Errorf("gpuAllocatedResourceOf(%v)[%q] = %q, want %q", tt.annotations, k, got[k], v)
				}
			}
		})
	}
}
