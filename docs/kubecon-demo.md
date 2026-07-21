# KubeCon Demo: Improving a GPU Bin-Packing Scheduler with kube-scheduler-evaluator

This demo shows how kube-scheduler-evaluator lets you iterate quickly on the
develop → evaluate → improve loop of Kubernetes scheduler development, using
GPU bin packing as the subject.

## Background: why utilization-percentage bin packing is not always enough

A common bin-packing strategy scores nodes by their **GPU utilization
percentage** and packs GPU pods onto the most-utilized node. This minimizes
the number of nodes in use, but it is not always the best way to prevent
**GPU fragmentation** — in particular when nodes have *different numbers of
GPUs* (for example, when GPU failures take some GPUs on a node out of
service).

Example: a node with 1 GPU (unused) and a node with 8 GPUs (4 in use) have
utilizations of 0% and 50%. Utilization-percentage bin packing sends the next
1-GPU pod to the 8-GPU node, eating into the large contiguous block of free
GPUs. To prevent fragmentation, the pod should go to the 1-GPU node instead —
i.e. pack by the **absolute number of free GPUs** (best fit), preferring the
node with the *fewest* free GPUs that still fits the pod.

The demo compares these two strategies as scheduler score plugins:

| Plugin | Strategy |
| --- | --- |
| `GPUUtilizationBinPacking` | Highest current GPU utilization % wins |
| `GPUFreeCountBinPacking` | Fewest free GPUs wins (best fit) |

Both are implemented in
[kube-scheduler-evaluator-reference](https://github.com/rihib/kube-scheduler-evaluator-reference)
under `plugins/gpubinpacking/`, and are enabled through the
`KubeSchedulerConfiguration` profiles in
`config/gpu-binpacking-scheduler/scheduler-config.yaml`.

## The scenario

Both scenarios replay the *same* workload (`scenarios/gpubinpacking/`); only
the scheduler profile differs.

1. **Nodes**: 4 nodes with 8 GPUs and 4 nodes with 1 GPU (36 GPUs total) —
   a cluster with heterogeneous GPU counts, as happens after GPU failures.
2. **Preload**: 4 jobs × 5 GPUs. They only fit on the 8-GPU nodes and two of
   them cannot share a node, so every 8-GPU node ends up 5/8 used (62.5%)
   under any scheduler.
3. **Small jobs**: 4 jobs × 1 GPU.
   - *Utilization*: the 8-GPU nodes (62.5%) outscore the empty 1-GPU nodes
     (0%), so the small jobs eat into the 8-GPU nodes.
   - *Free count*: the 1-GPU nodes have the fewest free GPUs, so the small
     jobs land there and each 8-GPU node keeps 3 free GPUs.
4. **Medium jobs**: 4 jobs × 3 GPUs.
   - *Utilization*: only 2 still fit; 2 stay **pending** while the 1-GPU
     nodes sit idle. Peak cluster GPU utilization ≈ **83%**.
   - *Free count*: all 4 fit, one per 8-GPU node. Peak cluster GPU
     utilization = **100%**, nothing pending.

The evaluator records the new `gpu_*` metrics after every scenario event:

| Metric | Meaning |
| --- | --- |
| `gpu_capacity_gpus` | Total allocatable GPUs on all nodes |
| `gpu_allocated_gpus` | GPUs requested by running (scheduled) pods |
| `gpu_pending_gpus` | GPUs requested by pending (unschedulable) pods |
| `gpu_utilization_percent` | allocated / capacity × 100 |

They are visualized by the provisioned **"GPU Bin-Packing Demo"** Grafana
dashboard (`config/grafana/dashboards/json/gpu-binpacking-demo.json`).

## Prerequisites

- Docker (docker compose)
- Go 1.24+
- The reference implementation checked out as a git submodule:

```bash
git clone --recurse-submodules https://github.com/rihib/kube-scheduler-evaluator.git
cd kube-scheduler-evaluator/kube-scheduler-evaluator-reference
# or, in an existing clone:
#   git submodule update --init
```

The submodule's `go.mod` replaces the
`github.com/pfnet/kube-scheduler-evaluator` dependency with the parent
directory, so scheduler/metric changes made in this repository are picked up
immediately by the demo — that is the point of the demo: edit, run, see the
result in seconds.

## Running the demo

All commands run inside `kube-scheduler-evaluator-reference/`.

### One-shot (both scenarios back to back)

```bash
make demo   # docker compose up (VictoriaMetrics + Grafana) + run both scenarios
make open   # Grafana: http://localhost:3000 (admin / password)
```

Open the **GPU Bin-Packing Demo** dashboard (time range: last 15 minutes).

### Presentation flow (run the scenarios one at a time)

```bash
make up                 # start VictoriaMetrics + Grafana once
make demo-utilization   # 1) baseline: utilization-percentage bin packing
# → show the dashboard: utilization plateaus at ~83%, 6 GPUs stay pending
make demo-freecount     # 2) improved: free-GPU-count bin packing
# → show the dashboard: utilization reaches 100%, nothing pending
make clean              # stop and clean up when done
```

Talking points for step 3 of the demo:

- The only difference between the two runs is ~50 lines of score-plugin code
  (`plugins/gpubinpacking/utilization.go` vs `freecount.go`) and the scheduler
  profile — the workload and cluster are identical.
- Each run takes ~15 seconds even though the simulated workload spans hours
  of virtual time. This is the develop → evaluate → improve loop running at
  interactive speed, with no real cluster needed.

### Pacing

Events are emitted every 500 ms of real time so the dashboard visibly draws
while you talk. Control this with `DEMO_PACE` (a Go duration):

```bash
DEMO_PACE=0s make demo-freecount   # as fast as possible
DEMO_PACE=1s make demo-freecount   # slower, for narration
```

## What to look at on the dashboard

- **Overall GPU Utilization (%)** — the utilization scenario ramps to ~83%
  and stalls; the free-count scenario ramps to 100%.
- **Peak GPU Utilization** — ~83% vs 100% side by side.
- **Pending GPU Requests** — 6 GPUs' worth of jobs stuck pending under
  utilization bin packing; zero under free-count bin packing.
- **Allocated GPUs vs Cluster GPU Capacity** — the allocated line touches the
  capacity line only in the free-count scenario.

## Troubleshooting

- **No data on the dashboard**: check the time range (the run is short — use
  "Last 15 minutes"), and that the `evaluation` variable includes the latest
  `evaluation-...` ID (default is All).
- **Rerunning**: every run gets a fresh `evaluation_id`; old runs remain in
  VictoriaMetrics until `docker volume rm` (or filter them out with the
  `evaluation` dashboard variable).
- **Ports in use**: the compose file uses 3000 (Grafana) and 8428
  (VictoriaMetrics).
