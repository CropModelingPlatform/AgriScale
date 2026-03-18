# AgriScale

> **A distributed framework for large-scale, spatially explicit ensemble crop model simulations**

AgriScale orchestrates the end-to-end execution of gridded soil-crop simulations over large spatial domains using adaptive domain partitioning, hierarchical parallelism, and Singularity/Apptainer containers. It enables multi-model ensemble runs across heterogeneous computing environments — HPC clusters, cloud platforms, or local servers — without modifying the original model implementations.

---

## Table of Contents

- [Key Features](#key-features)
- [Architecture](#architecture)
- [Integrated Models](#integrated-models)
- [Getting Started](#getting-started)
  - [Prerequisites](#prerequisites)
  - [Get the Container](#get-the-container)
  - [Data Setup](#data-setup)
- [Configuration](#configuration)
- [Running Simulations](#running-simulations)
  - [SLURM Job Array](#slurm-job-array)
  - [Singularity Bind Mounts](#singularity-bind-mounts)
- [Input Database Schema](#input-database-schema)
- [Performance Notes](#performance-notes)
- [Citation](#citation)
- [License](#license)

---

## Key Features

- **Multi-model support** — run CELSIUS, DSSAT, and STICS in a unified workflow without modifying their source code
- **Adaptive spatial partitioning** — automatic domain decomposition into balanced subdomains for dynamic load balancing
- **Hierarchical parallelism** — combines inter-task distributed execution with intra-task multiprocessing via SLURM job arrays
- **I/O optimization** — application-level caching and two storage strategies (shared storage and node-local) to reduce file-system contention
- **Container-based portability** — all models and dependencies are packaged in a single Singularity SIF image (`datamill.sif`)
- **SQLite-backed data management** — lightweight relational database (`MasterInput.db`) structures simulation units, soil, climate, cultivar, and management inputs
- **Flexible crop management** — supports spatialized sowing dates, fertilization scenarios, varieties, and irrigation from configuration or gridded files

---

## Architecture

AgriScale divides a spatial domain into a regular grid and partitions grid cells into subdomains using row-major ordering with near-equal load balancing (see Appendix A in the paper). Each SLURM array task processes one subdomain:

```
User specifies domain (bounding box + resolution)
        │
        ▼
Spatial domain partitioning  ──►  N subdomains  (SLURM --array=0-N)
        │
        ▼  (per subdomain task)
┌─────────────────────────────────────────────┐
│  1. Build simulation list from MasterInput  │
│  2. Generate model-specific input files     │  ◄── local cache (climate, cultivars)
│  3. Execute model(s) with multiprocessing   │
│  4. Parse and aggregate outputs             │
│  5. Write results to /outputData            │
└─────────────────────────────────────────────┘
```

Two I/O strategies are supported:
- **Shared Storage Strategy (SSS)** — model I/O written directly to the shared file system (suitable for WekaFS or fast parallel FS)
- **Node Locality Strategy (NLS)** — I/O staged to a fast local temporary directory (`$TMPDIR`), then results are copied back (recommended for ZFS/EXT4 file systems)

---

## Integrated Models

| Model   | Language     | Reference |
|---------|-------------|-----------|
| CELSIUS | Visual Basic | Ricome et al., 2017 |
| DSSAT   | Fortran      | Hoogenboom et al., 2019 |
| STICS   | Fortran      | Brisson et al., 2009 |

Models are invoked via model adapters that handle input generation, executable invocation, and output parsing, keeping the core simulation pipeline model-agnostic.

---

## Getting Started

### Prerequisites

- **Singularity** (≥ 3.x) or **Apptainer** (≥ 1.x) installed on your compute environment
- **SLURM** workload manager (for HPC job submission)
- Input datasets: gridded climate (NetCDF/Zarr), soil, and optionally sowing date rasters at consistent spatial resolution
- A populated `MasterInput.db` SQLite database (see [Input Database Schema](#input-database-schema))

### Get the Container

Download the pre-built `datamill.sif` container from the latest release of [AgriscaleContainer](https://github.com/CropModelingPlatform/AgriscaleContainer/releases/latest):

```bash
bash scripts/download_container.sh
```

This downloads `datamill.sif` directly from the GitHub Release asset — no authentication required for a public repository. To pin a specific version:

```bash
AGRISCALE_VERSION=v1.2.0 bash scripts/download_container.sh
```

For private repositories, set `GITHUB_TOKEN` beforehand:

```bash
export GITHUB_TOKEN=ghp_your_token_here
bash scripts/download_container.sh
```

### Data Setup

Organize your data as follows (paths are mapped via Singularity bind mounts):

```
/inputData/          # gridded input data (climate NetCDF/Zarr, soil, sowing dates)
/package/            # AgriScale repository root (this directory)
  config.ini
  scripts/
  db/
    MasterInput.db
  data/
  datamill.sif
/inter/              # intermediate files and simulation outputs
/outputData/         # final aggregated results
$TMPDIR/             # fast node-local scratch (for NLS strategy)
```

---

## Configuration

All simulation options are controlled by `config.ini` in the repository root. Key sections:

| Section | Key Parameters | Description |
|---------|---------------|-------------|
| `[spatialized_features]` | `s_variety`, `s_fert`, `s_sowing`, `s_irr`, `s_density` | Toggle spatially explicit management inputs |
| `[variety_settings]` | `variety_dict`, `variety` | Map raster variety codes to cultivar IDs |
| `[model_options]` | `models` | Models to run: `("celsius")`, `("stics" "celsius" "dssat")` |
| `[sowing_options]` | `SD`, `SW` | Sowing date mode: `0`=fixed, `1`=from CELSIUS output, `3`=list of DOYs, `4`=gridded file |
| `[simulation_options]` | `simoption` | `1`=WS+NS, `2`=WS only, `3`=NS only, `4`=Potential |
| `[simulation_dates]` | `startd`, `endd`, `start_sowing`, `end_sowing` | Simulation window (day-of-year or days relative to sowing) |
| `[soil_settings]` | `textclass`, `soilTextureType` | Use gridded soil texture file or a fixed global texture class |
| `[extraction_options]` | `doExtract`, `bound` | Optionally extract a sub-region bounding box `(lon_min lat_min lon_max lat_max)` |
| `[crop_mask_options]` | `cropmask` | Apply a crop mask raster |
| `[fertilization_options]` | `fertioption` | Fertilization scenario codes from `CropManagement` table |

Example: run CELSIUS and STICS with gridded sowing dates for all varieties:

```ini
[model_options]
models=("celsius" "stics")

[sowing_options]
SD=1

[simulation_options]
simoption=(2)
```

---

## Running Simulations

### SLURM Job Array

Submit a job array where each task processes one spatial subdomain:

```bash
sbatch --array=0-<N-1> datamill.sh
```

Replace `<N-1>` with the number of subdomains minus one (determined by the `parts` parameter in `config.ini` or set automatically). For example, for 15 subdomains:

```bash
sbatch --array=0-14 datamill.sh
```

Key SLURM parameters in `datamill.sh` (edit to match your cluster):

```bash
#SBATCH --cpus-per-task=8     # cores per task (intra-task multiprocessing)
#SBATCH --mem=64G
#SBATCH --time=23:58:00
#SBATCH --partition cpu-dedicated
```

### Singularity Bind Mounts

The container is launched with the following bind mounts (adapt paths to your site):

| Mount inside container | Example host path | Purpose |
|------------------------|------------------|---------|
| `/package` | `/scratch/$USER/AgriScale_Ind` | AgriScale working directory |
| `/inputData` | `/storage/…/data_zarr` | Input climate/soil data |
| `/inter` | `/scratch/$USER/AgriScale_Ind/results` | Intermediate files |
| `/outputData` | `/scratch/$USER/AgriScale_Ind/results` | Final outputs |
| `$TMPDIR` | `/scratch/$USER/tmp` | Fast local scratch (NLS strategy) |

Edit the `singularity exec` command in `datamill.sh` to match your site's paths.

---

## Input Database Schema

Simulations are driven by a **MasterInput SQLite database** (`db/MasterInput.db`). The key tables are:

| Table | Description |
|-------|-------------|
| `SimulationUnit` | One row per grid cell; links climate, soil, and management records |
| `CropManagement` | Crop management practices (sowing date, density, fertilization, irrigation) |
| `Cultivar` | Cultivar parameter sets per model |
| `Soil` | Soil profile data per grid cell |
| `Climate` | References to gridded climate files or records |

When spatially explicit management is enabled (`s_sowing=1`, etc.), AgriScale automatically regenerates the `CropManagement` table with unique identifiers encoding the grid cell and scenario.

A template schema is provided in `db/ori_MasterInput.db`. A detailed field-level description is available in the paper (Appendix B, Table B.1).

---

## Performance Notes

Benchmark results from the paper (3,830,650 simulations per model over Sub-Saharan Africa at 0.05° resolution):

- Speedup is near-linear up to ~16 cores per task; distributed task partitioning maintains scaling beyond 32–64 cores where single-task multiprocessing plateaus
- Job efficiency ranges from **70–92%** across HPC environments and I/O strategies
- The **Node Locality Strategy** (staging I/O to `$TMPDIR`) is strongly recommended for ZFS and EXT4 file systems to avoid shared file-system contention

---

## Citation

If you use AgriScale in your work, please cite:

> Midingoyi, C. A., Falconnier, G. N., Blitz-Frayret, C., Pradal, C., Giner, M., Adam, M., Corbeels, M., Couëdel, A., Heuclin, B., Lavarenne, J., Gerardeaux, E., Loison, R., Agbohessou, Y., et al. (2025). *AgriScale: A Distributed Framework for Gridded Crop Models Ensemble applications*. Manuscript submitted for publication.

---

## License

This project is licensed under the terms of the [LICENSE](LICENSE) file included in this repository.

