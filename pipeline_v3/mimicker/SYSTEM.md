# System & Environment Reference

Technical specification for the compute environment used by this project.

---

## Cluster: Midway3

| Detail | Value |
|--------|-------|
| **Cluster** | Midway3 |
| **Institution** | Research Computing Center (RCC), The University of Chicago |
| **Scheduler** | SLURM |
| **Login Nodes** | `midway3-login{1-4}.rcc.local` |
| **OS** | Red Hat Enterprise Linux 8.4 (Ootpa) |
| **Kernel** | 4.18.0-305.x86_64 |

### Documentation

- [RCC User Guide](https://rcc.uchicago.edu/docs/)
- [Midway3 Hardware](https://rcc.uchicago.edu/resources/midway3)
- [SLURM at RCC](https://rcc.uchicago.edu/docs/slurm/index.html)

---

## GPU Node

Our training and inference runs on a dedicated GPU node in the `hcn1-gpu` partition.

| Spec | Value |
|------|-------|
| **Node** | `midway3-0427` |
| **Partition** | `hcn1-gpu` |
| **Account** | `pi-hcn1` |
| **QOS** | `hcn1` (max walltime: 2 days) |

### GPUs

| Spec | Value |
|------|-------|
| **Model** | NVIDIA L40S |
| **Count** | 4 per node |
| **VRAM** | 48 GB GDDR6 per card (192 GB total) |
| **Architecture** | Ada Lovelace (AD102) |
| **CUDA Cores** | 18,176 per card |
| **Tensor Cores** | 568 per card (4th-gen) |
| **RT Cores** | 142 per card (3rd-gen) |
| **FP32** | 91.6 TFLOPS per card |
| **TF32 Tensor** | 183.2 TFLOPS per card |
| **FP16 Tensor** | 366.4 TFLOPS per card |
| **INT8 Tensor** | 733 TOPS per card |
| **Memory Bandwidth** | 864 GB/s per card |
| **TDP** | 350W per card |
| **PCIe** | Gen 4 x16 |
| **NVLink** | Not available (PCIe topology) |
| **Multi-GPU** | NCCL over PCIe |

> **Note:** The L40S is distinct from the L40 (no "S"). The L40S has higher TDP (350W vs 300W), higher clocks, and is optimized for AI/HPC workloads rather than professional visualization. It uses the full AD102 die.

### CPUs

| Spec | Value |
|------|-------|
| **Model** | Intel Xeon Gold 6346 |
| **Sockets** | 2 |
| **Cores per Socket** | 16 |
| **Total Cores** | 32 physical (HT disabled on compute) |
| **Clock** | 3.10 GHz base / 3.60 GHz turbo |
| **Architecture** | Ice Lake-SP (x86_64) |
| **L3 Cache** | 36 MB per socket |
| **TDP** | 205W per socket |

### Memory & Storage

| Spec | Value |
|------|-------|
| **System RAM** | 1 TB (1,031,735 MB) |
| **Storage Mount** | `/project` (Lustre parallel filesystem) |
| **Total Capacity** | 23 TB |
| **Available** | ~18 TB |

---

## SLURM Job Configuration

The `submit_job.sh` script requests:

```bash
#SBATCH --job-name={your-job-name}
#SBATCH --partition=hcn1-gpu
#SBATCH --account=pi-hcn1
#SBATCH --qos=hcn1
#SBATCH --gres=gpu:{up_to_4_gpus}
#SBATCH --cpus-per-task={up_to_32_cpus}
#SBATCH --mem={requested_RAM_up_to_1000G}
#SBATCH --time={wall_time_up_to_48_hours}
```

### Useful SLURM Commands

```bash
# Submit a job
sbatch submit_job.sh

# Check job status
squeue -u $USER

# Cancel a job
scancel <JOBID>

# View job details
scontrol show job <JOBID>

# View node details
scontrol show node midway3-0427

# View partition info
sinfo -p hcn1-gpu

# Check past job stats (after completion)
sacct -j <JOBID> --format=JobID,Elapsed,MaxRSS,MaxVMSize,TotalCPU,AllocGRES
```

### Job Monitoring Dashboard

The `monitor/` module provides a web-based dashboard:

```bash
# Start the monitor
cd monitor && python app.py

# Access via VS Code port forwarding at http://localhost:5001
```

Features:

- Live log file tailing
- SLURM job status parsing

---

## Software Stack

### Module Environment

```bash
module load cuda/12.6
module load python/miniforge-25.3.0
```

### Reproducing the Environment

### Environment Variables

Set in `submit_job.sh`:

```bash
export HF_HOME=/project/smbowdre/dtfalk/models/cache
export TORCH_HOME=/project/smbowdre/dtfalk/models/cache/torch
export PYTHONUNBUFFERED=1
export PYTORCH_CUDA_ALLOC_CONF=expandable_segments:True
```

| Variable | Purpose |
| ---------- | --------- |
| `HF_HOME` | HuggingFace model cache (shared storage) |
| `TORCH_HOME` | PyTorch hub cache |
| `PYTHONUNBUFFERED` | Real-time log output to SLURM |
| `PYTORCH_CUDA_ALLOC_CONF=expandable_segments:True` | Reduce CUDA memory fragmentation |

---

## Multi-GPU Inference Setup

| Detail | Value |
| -------- | ------- |
| **Framework** | PyTorch + HuggingFace Accelerate |
| **Parallelism** | Tensor Parallelism (model sharding) |
| **Backend** | NCCL (GPU-to-GPU communication) |
| **Topology** | Single-node, 4 GPUs over PCIe |
| **Precision** | FP16 / BF16 mixed precision |

### How Tensor Parallelism Works

```
                    ┌──────────────────────────────────────┐
                    │         midway3-0427 (1 node)        │
                    │                                      │
                    │  ┌────────┐  ┌────────┐              │
                    │  │ GPU 0  │  │ GPU 1  │              │
                    │  │ L40S   │  │ L40S   │              │
                    │  │ 48GB   │  │ 48GB   │              │
                    │  │ Layer  │  │ Layer  │              │
                    │  │ 0-19   │  │ 20-39  │              │
                    │  └───┬────┘  └───┬────┘              │
                    │      │  PCIe     │                   │
                    │      │  ◄────►   │                   │
                    │      │  NCCL     │                   │
                    │      │  ◄────►   │                   │
                    │  ┌───┴────┐  ┌───┴────┐              │
                    │  │ GPU 2  │  │ GPU 3  │              │
                    │  │ L40S   │  │ L40S   │              │
                    │  │ 48GB   │  │ 48GB   │              │
                    │  │ Layer  │  │ Layer  │              │
                    │  │ 40-59  │  │ 60-79  │              │
                    │  └────────┘  └────────┘              │
                    │                                      │
                    │  CPU: 2× Xeon Gold 6346 (32 cores)   │
                    │  RAM: 1 TB                           │
                    └──────────────────────────────────────┘
```

The 70B parameter model is sharded across 4 GPUs (~17.5B parameters each). Each GPU holds a contiguous block of transformer layers. Activations are passed between GPUs via NCCL over PCIe during the forward pass.

---

## Performance Notes

### L40S for LLM Embedding Extraction

The NVIDIA L40S is well-suited for this workload:

- **48 GB VRAM** per card handles 70B parameter models with 4-way tensor parallelism
- **4th-gen Tensor Cores** accelerate FP16 inference (366 TFLOPS)
- **864 GB/s memory bandwidth** enables fast KV-cache updates
- **PCIe Gen 4** interconnect supports tensor parallel communication
