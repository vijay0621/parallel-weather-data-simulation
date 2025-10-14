# Parallel Weather Simulation (Flask + MPI)

A parallel weather data simulation for Tamil Nadu using Flask on the backend, mpi4py for distributed execution, and a simple frontend.

## Features

- Parallel data fetch for 38 Tamil Nadu districts using MPI (mpi4py)
- Real-time per-rank progress updates with non-blocking MPI sends/receives
- Performance metrics: execution time, estimated sequential time, speedup, per-rank times
- Statistical analysis via MPI collectives (Reduce): hottest/coldest, variance, criteria counts
- Alert system via Scatter: per-rank thresholds, gathered alerts and card color-coding
- Allgather demonstration: every rank computes an independent global average
- Frontend UI with rank selector, per-rank progress bars, processor summary, and metrics panel

## Architecture Overview

- `app.py`: Flask server exposing endpoints and orchestrating MPI runs
- `mpi_fetch.py`: MPI job implementing Broadcast, Scatter, Gather, Reduce, Allgather, Send/Recv, Isend/Irecv, and Barriers
- `tn_districts.py`: List of 38 TN districts with lat/lon
- `static/index.html`, `static/styles.css`, `static/script.js`: Frontend
- `static/visualization.html`: Chart view (uses Chart.js)
- Data outputs: `data/weather.json`, `data/progress.json`, `data/metrics.json`

## Prerequisites

- Python 3.9+
- MPI runtime (OpenMPI or MPICH)
- `mpi4py` Python package
- OpenWeather API key (set environment variable `OPENWEATHER_API_KEY`)

## Installation

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

Ensure an MPI runtime is installed (example with Ubuntu):

```bash
sudo apt-get update && sudo apt-get install -y mpich
# or: sudo apt-get install -y openmpi-bin openmpi-common libopenmpi-dev
```

Set your API key (replace with your key):

```bash
export OPENWEATHER_API_KEY=YOUR_KEY_HERE
```

## Running the App

Start Flask:

```bash
python app.py
```

Open the app at `http://127.0.0.1:5000/`.

### Refreshing Data

There are two refresh modes:

- Blocking refresh (existing endpoint):

```bash
curl -X POST http://127.0.0.1:5000/api/refresh -H 'Content-Type: application/json' -d '{"num_processors": 4}'
```

- Non-blocking refresh (recommended for demos):

```bash
curl -X POST http://127.0.0.1:5000/api/refresh/start -H 'Content-Type: application/json' -d '{"num_processors": 4}'
```

This starts the MPI job via `mpirun -n <ranks> python run_mpi_fetch.py ...`.

### Live Progress and Metrics

- Progress (polled by the frontend): `GET /api/progress`
- Metrics after completion: `GET /api/metrics`
- Latest data: `GET /api/data`

## Frontend UI Guide

- "Refresh Now" button: starts a non-blocking refresh and begins polling progress.
- Ranks dropdown: selects how many MPI ranks (`-n`) to use on the next refresh. Choose 2, 4, 6, or 8.
- Progress bars: one per rank, showing items processed over that rank's assigned districts.
- District cards: show district data, the `processor_rank` badge, and alert color coding:
  - low: amber border
  - medium: red border
- Processor summary: which rank processed which districts.
- MPI Performance Metrics panel:
  - Execution Time, Speedup
  - Hottest/Coldest district (via MPI.MAX/MPI.MIN reduced values)
  - Temperature Variance (via reduce of sum and squared sum)
  - Alerts count
  - Per-rank execution times
  - Allgather independent averages per rank

### Visualization Page

A separate visualization view is available at `/visualization.html` with Chart.js bar charts.

- Temperature, Humidity, Rainfall, Wind buttons update the chart
- Uses the same `/api/data` dataset

## MPI Concepts Mapping

- Broadcast: Root broadcasts config (API key, timeout) via `comm.bcast`
- Scatter: Root scatters district chunks and per-rank thresholds via `comm.scatter`
- Inter-process communication: Control messages and progress via `send/recv` and `isend/irecv`
- Blocking operations: Start (`send` from root, `recv` at workers) and done notifications
- Non-blocking operations: Workers `isend` progress, root posts `irecv` and `test()` to update progress file
- Synchronization: `comm.Barrier()` before timing and after computation fences
- Gather: Results, alerts, and per-rank timing arrays gathered via `comm.gather`
- Reduce: SUM (mean and variance components), MAX/MIN (hottest/coldest)
- Allgather: `comm.allgather` shares full dataset to all ranks; each rank computes independent global average; root gathers these for display

## Tuning and Limits

- Execution target: under 30 seconds for full refresh (timeout per request kept small for demo)
- Choose ranks carefully based on available CPU cores and MPI installation
- If `mpirun` isn’t found, the backend falls back to `mpiexec`

## Troubleshooting

- Ensure `OPENWEATHER_API_KEY` is set and valid
- Verify MPI installed and callable (`mpirun --version`)
- Install `mpi4py` in the same Python environment as Flask
- Check server logs for errors from `app.py`
- Inspect `data/progress.json` and `data/metrics.json` during/after runs

## License

MIT
