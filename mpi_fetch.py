import os
import json
import time
from datetime import datetime
from typing import Dict, List, Tuple, Any, Optional

import requests
from mpi4py import MPI


# MPI message tags
TAG_START = 11
TAG_PROGRESS = 12
TAG_DONE = 13
TAG_HALO = 21


def _ensure_parent_dir(path: str) -> None:
    parent = os.path.dirname(path)
    if parent:
        os.makedirs(parent, exist_ok=True)


def _write_json_atomic(path: str, payload: Dict[str, Any]) -> None:
    _ensure_parent_dir(path)
    tmp_path = f"{path}.tmp"
    with open(tmp_path, 'w') as f:
        json.dump(payload, f, indent=2)
    os.replace(tmp_path, path)


def _chunk_indices(num_items: int, num_bins: int, bin_index: int) -> Tuple[int, int]:
    base = num_items // num_bins
    rem = num_items % num_bins
    start = bin_index * base + min(bin_index, rem)
    end = start + base + (1 if bin_index < rem else 0)
    return start, end


def _fetch_one(district: Dict[str, Any], api_key: str, timeout: float) -> Dict[str, Any]:
    lat = district.get('lat')
    lon = district.get('lon')
    url = (
        "https://api.openweathermap.org/data/2.5/weather"
        f"?lat={lat}&lon={lon}&appid={api_key}&units=metric"
    )
    try:
        response = requests.get(url, timeout=timeout)
        response.raise_for_status()
        data = response.json()
        return {
            'district': district.get('name'),
            'temperature_c': float(data['main']['temp']),
            'humidity_pct': float(data['main']['humidity']),
            'wind_speed_ms': float(data['wind']['speed']),
            'rainfall_mm': float(data.get('rain', {}).get('1h', 0) or 0.0),
        }
    except Exception as e:
        return {
            'district': district.get('name'),
            'error': str(e),
        }


def _haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    from math import radians, sin, cos, asin, sqrt
    R = 6371.0
    dlat = radians(lat2 - lat1)
    dlon = radians(lon2 - lon1)
    a = sin(dlat / 2) ** 2 + cos(radians(lat1)) * cos(radians(lat2)) * sin(dlon / 2) ** 2
    c = 2 * asin(sqrt(a))
    return R * c


def _pearson(x: List[float], y: List[float]) -> Optional[float]:
    n = min(len(x), len(y))
    if n < 3:
        return None
    x = x[-n:]
    y = y[-n:]
    mx = sum(x) / n
    my = sum(y) / n
    num = sum((xi - mx) * (yi - my) for xi, yi in zip(x, y))
    denx = sum((xi - mx) ** 2 for xi in x)
    deny = sum((yi - my) ** 2 for yi in y)
    if denx <= 0 or deny <= 0:
        return None
    return num / (denx ** 0.5 * deny ** 0.5)


def _z_score(latest: float, series: List[float]) -> Optional[float]:
    n = len(series)
    if n < 5:
        return None
    mean = sum(series) / n
    var = sum((v - mean) ** 2 for v in series) / n
    std = var ** 0.5
    if std == 0:
        return 0.0
    return (latest - mean) / std


def fetch_weather_data(
    districts: List[Dict[str, Any]],
    output_file: str,
    num_processors: int = 4,
    progress_file: str = 'data/progress.json',
    metrics_file: str = 'data/metrics.json',
    anomaly_file: str = 'mpi_output/anomaly_report.json',
) -> bool:
    """
    Parallel weather fetch using mpi4py demonstrating multiple MPI concepts:
    - Inter-process communication (Send/Recv)
    - Blocking ops (send/recv) and synchronization (Barrier)
    - Non-blocking progress using isend/irecv
    - Broadcast, Scatter, Gather, Reduce, Allgather
    """

    comm = MPI.COMM_WORLD
    rank = comm.Get_rank()
    size = comm.Get_size()

    # Config broadcast (Broadcast)
    if rank == 0:
        config = {
            'api_key': os.environ.get('OPENWEATHER_API_KEY', ''),
            'timeout': 4.0,  # keep requests snappy for demo
        }
    else:
        config = None
    config = comm.bcast(config, root=0)

    # Build work distribution (Scatter)
    # Keep root (rank 0) as coordinator for progress/UI; workers are 1..size-1
    worker_count = max(size - 1, 1)
    if size == 1:
        # Degenerate non-MPI fallback (single process)
        worker_chunks: List[List[Dict[str, Any]]] = [districts]
    else:
        worker_chunks = [[]]  # placeholder for root
        for i in range(worker_count):
            s, e = _chunk_indices(len(districts), worker_count, i)
            worker_chunks.append(districts[s:e])

    # Per-rank alert thresholds to demonstrate Scatter
    if rank == 0:
        thresholds = [None]
        for r in range(1, size):
            # simple varying thresholds per rank for demo
            thresholds.append({
                'temp_gt': 28 + (r % 5),
                'humidity_gt': 70 + (r % 5) * 3,
            })
    else:
        thresholds = None

    # Scatter chunks and thresholds
    my_chunk: List[Dict[str, Any]] = comm.scatter(worker_chunks, root=0) if size > 1 else worker_chunks[0]
    my_thresholds: Dict[str, float] = comm.scatter(thresholds, root=0) if size > 1 else {'temp_gt': 30, 'humidity_gt': 80}

    # Precompute neighbor adjacency on root and broadcast
    if rank == 0:
        name_to_idx = {d['name']: i for i, d in enumerate(districts)}
        adjacency: Dict[str, List[str]] = {}
        for i, a in enumerate(districts):
            adj = []
            for j, b in enumerate(districts):
                if i == j:
                    continue
                if _haversine_km(a['lat'], a['lon'], b['lat'], b['lon']) <= 100.0:
                    adj.append(b['name'])
            adjacency[a['name']] = adj
    else:
        adjacency = None
    adjacency = comm.bcast(adjacency, root=0)

    # Synchronize start (Barrier)
    comm.Barrier()
    t0 = MPI.Wtime()

    # Blocking start signal from root to workers (Send/Recv)
    if size > 1:
        if rank == 0:
            for r in range(1, size):
                comm.send(True, dest=r, tag=TAG_START)
        else:
            _ = comm.recv(source=0, tag=TAG_START)

    # Initialize progress file from root
    if rank == 0:
        # Root knows how many items per worker from worker_chunks
        ranks_progress = {}
        total_map = {}
        if size > 1:
            # include coordinator rank 0 as a visible row (no work)
            total_map['0'] = 0
            ranks_progress['0'] = {'done': 0, 'total': 0}
            for r in range(1, size):
                total_map[str(r)] = len(worker_chunks[r])
                ranks_progress[str(r)] = {'done': 0, 'total': len(worker_chunks[r])}
        else:
            total_map['0'] = len(my_chunk)
            ranks_progress['0'] = {'done': 0, 'total': len(my_chunk)}

        _write_json_atomic(progress_file, {
            'status': 'running',
            'started_at': datetime.now().isoformat(),
            'updated_at': datetime.now().isoformat(),
            'completed': False,
            'size': size,
            'ranks': ranks_progress,
        })

    # Each worker performs its fetch and non-blocking progress (Isend)
    local_results: List[Dict[str, Any]] = []
    local_alerts: List[Dict[str, Any]] = []
    local_request_durations: List[float] = []
    # History and anomalies
    history_dir = 'mpi_output'
    if rank == 0 and size == 1:
        _ensure_parent_dir(os.path.join(history_dir, 'dummy'))
    elif rank != 0:
        _ensure_parent_dir(os.path.join(history_dir, 'dummy'))
    history_path = os.path.join(history_dir, f'history_rank_{rank}.json')
    try:
        if os.path.exists(history_path):
            with open(history_path, 'r') as f:
                history_map: Dict[str, List[Dict[str, Any]]] = json.load(f)
        else:
            history_map = {}
    except Exception:
        history_map = {}
    anomaly_reports_local: List[Dict[str, Any]] = []
    per_rank_anomaly_counts = {'low': 0, 'medium': 0, 'high': 0, 'critical': 0}
    local_temp_sum = 0.0
    local_temp_sumsq = 0.0
    local_temp_count = 0
    local_max_temp = float('-inf')
    local_min_temp = float('inf')
    local_max_name = ''
    local_min_name = ''

    # Root will act as progress collector; it does no fetching when multiple ranks
    if size == 1 or rank != 0:
        total_items = len(my_chunk)
        done_items = 0
        pending_reqs: List[MPI.Request] = []
        for d in my_chunk:
            t_req0 = time.perf_counter()
            item = _fetch_one(d, api_key=config['api_key'], timeout=config['timeout'])
            duration = time.perf_counter() - t_req0
            local_request_durations.append(duration)

            # enrich with rank metadata
            item['processor_rank'] = rank
            item['total_processors'] = size
            # maintain history circular buffer (last 10)
            now_iso = datetime.now().isoformat()
            rec = {
                'ts': now_iso,
                'temperature_c': item.get('temperature_c'),
                'humidity_pct': item.get('humidity_pct'),
                'rainfall_mm': item.get('rainfall_mm'),
                'wind_speed_ms': item.get('wind_speed_ms'),
            }
            hlist = history_map.get(item['district'], [])
            hlist.append(rec)
            if len(hlist) > 10:
                hlist = hlist[-10:]
            history_map[item['district']] = hlist

            # anomaly detection using z-score threshold
            z_threshold = 2.0
            # temperature spike/drop
            temps = [r['temperature_c'] for r in hlist if r.get('temperature_c') is not None]
            hums = [r['humidity_pct'] for r in hlist if r.get('humidity_pct') is not None]
            rains = [r['rainfall_mm'] for r in hlist if r.get('rainfall_mm') is not None]
            winds = [r['wind_speed_ms'] for r in hlist if r.get('wind_speed_ms') is not None]
            # helper to classify severity by abs z
            def sev_for(z: float) -> str:
                az = abs(z)
                if az >= 3.0:
                    return 'critical'
                if az >= 2.5:
                    return 'high'
                if az >= 2.0:
                    return 'medium'
                return 'low'

            latest_temp = temps[-1] if temps else None
            latest_hum = hums[-1] if hums else None
            latest_rain = rains[-1] if rains else None
            latest_wind = winds[-1] if winds else None
            district_anomaly_sev = None
            district_anomaly_types: List[str] = []

            if latest_temp is not None:
                zt = _z_score(latest_temp, temps)
                if zt is not None and abs(zt) >= z_threshold:
                    atype = 'temp_spike' if zt > 0 else 'temp_drop'
                    sev = sev_for(zt)
                    anomaly_reports_local.append({'district': item['district'], 'type': atype, 'severity': sev, 'z_score': round(float(zt), 3)})
                    per_rank_anomaly_counts[sev] += 1
                    district_anomaly_types.append(atype)
                    district_anomaly_sev = sev if district_anomaly_sev is None else max(district_anomaly_sev, sev, key=lambda s: ['low','medium','high','critical'].index(s))

            if latest_hum is not None:
                zh = _z_score(latest_hum, hums)
                if zh is not None and abs(zh) >= z_threshold:
                    sev = sev_for(zh)
                    anomaly_reports_local.append({'district': item['district'], 'type': 'humidity_extreme', 'severity': sev, 'z_score': round(float(zh), 3)})
                    per_rank_anomaly_counts[sev] += 1
                    district_anomaly_types.append('humidity_extreme')
                    district_anomaly_sev = sev if district_anomaly_sev is None else max(district_anomaly_sev, sev, key=lambda s: ['low','medium','high','critical'].index(s))

            # rainfall onset: last was 0 and now > 0
            if len(rains) >= 2 and rains[-2] == 0 and (latest_rain or 0) > 0:
                sev = 'medium'
                anomaly_reports_local.append({'district': item['district'], 'type': 'rain_onset', 'severity': sev, 'z_score': None})
                per_rank_anomaly_counts[sev] += 1
                district_anomaly_types.append('rain_onset')
                district_anomaly_sev = district_anomaly_sev or sev

            if latest_wind is not None:
                zw = _z_score(latest_wind, winds)
                if zw is not None and abs(zw) >= z_threshold:
                    sev = sev_for(zw)
                    anomaly_reports_local.append({'district': item['district'], 'type': 'wind_anomaly', 'severity': sev, 'z_score': round(float(zw), 3)})
                    per_rank_anomaly_counts[sev] += 1
                    district_anomaly_types.append('wind_anomaly')
                    district_anomaly_sev = sev if district_anomaly_sev is None else max(district_anomaly_sev, sev, key=lambda s: ['low','medium','high','critical'].index(s))

            item['anomaly_severity'] = district_anomaly_sev
            item['anomaly_types'] = district_anomaly_types

            local_results.append(item)

            # stats accumulation only if we have temperature
            if 'temperature_c' in item and isinstance(item['temperature_c'], (int, float)):
                temp = float(item['temperature_c'])
                local_temp_sum += temp
                local_temp_sumsq += temp * temp
                local_temp_count += 1
                if temp > local_max_temp:
                    local_max_temp = temp
                    local_max_name = item['district']
                if temp < local_min_temp:
                    local_min_temp = temp
                    local_min_name = item['district']

            # alerts based on scattered thresholds
            exceed = 0
            if 'temperature_c' in item and isinstance(item['temperature_c'], (int, float)) and item['temperature_c'] > my_thresholds['temp_gt']:
                exceed += 1
            if 'humidity_pct' in item and isinstance(item['humidity_pct'], (int, float)) and item['humidity_pct'] > my_thresholds['humidity_gt']:
                exceed += 1
            severity = 'none'
            if exceed == 1:
                severity = 'low'
            elif exceed >= 2:
                severity = 'medium'
            if severity != 'none':
                local_alerts.append({
                    'district': item['district'],
                    'rank': rank,
                    'exceed_count': exceed,
                    'severity': severity,
                    'thresholds': my_thresholds,
                })
                item['alert_severity'] = severity

            # non-blocking progress update to root
            done_items += 1
            if size > 1:
                req = comm.isend({'rank': rank, 'done': done_items, 'total': total_items}, dest=0, tag=TAG_PROGRESS)
                pending_reqs.append(req)
                # throttle outstanding requests
                if len(pending_reqs) > 8:
                    pending_reqs[0].wait()
                    pending_reqs = pending_reqs[1:]

        # flush outstanding progress sends
        for req in pending_reqs:
            try:
                req.wait()
            except Exception:
                pass

        # persist history for this rank
        try:
            _ensure_parent_dir(history_path)
            with open(history_path, 'w') as f:
                json.dump(history_map, f)
        except Exception:
            pass

        # blocking notify done (Send)
        if size > 1:
            comm.send({'rank': rank, 'done': done_items, 'total': total_items}, dest=0, tag=TAG_DONE)

    # Root collects progress using non-blocking receives (Irecv)
    if size > 1 and rank == 0:
        finished = {r: False for r in range(1, size)}
        ranks_progress: Dict[str, Dict[str, int]] = {
            str(r): {'done': 0, 'total': len(worker_chunks[r])} for r in range(1, size)
        }
        # add rank 0 row for UI
        ranks_progress['0'] = {'done': 0, 'total': 0}
        progress_reqs: Dict[int, MPI.Request] = {
            r: comm.irecv(source=r, tag=TAG_PROGRESS) for r in range(1, size)
        }
        done_reqs: Dict[int, MPI.Request] = {
            r: comm.irecv(source=r, tag=TAG_DONE) for r in range(1, size)
        }
        while not all(finished.values()):
            for r in range(1, size):
                if finished[r]:
                    continue
                flag, msg = progress_reqs[r].test()
                if flag and msg is not None:
                    ranks_progress[str(r)]['done'] = int(msg.get('done', 0))
                    _write_json_atomic(progress_file, {
                        'status': 'running',
                        'started_at': None,
                        'updated_at': datetime.now().isoformat(),
                        'completed': False,
                        'size': size,
                        'ranks': ranks_progress,
                    })
                    progress_reqs[r] = comm.irecv(source=r, tag=TAG_PROGRESS)
                flag_done, msg_done = done_reqs[r].test()
                if flag_done and msg_done is not None:
                    finished[r] = True
                    ranks_progress[str(r)]['done'] = ranks_progress[str(r)]['total']
                    _write_json_atomic(progress_file, {
                        'status': 'running',
                        'started_at': None,
                        'updated_at': datetime.now().isoformat(),
                        'completed': False,
                        'size': size,
                        'ranks': ranks_progress,
                    })
            time.sleep(0.03)

    # Halo exchange: boundary histories for spatial correlation
    boundary_exchange_time = 0.0
    correlation_map_local: Dict[str, float] = {}
    regional_flags_local: List[Dict[str, Any]] = []
    if size == 1 or rank != 0:
        # Build owner map on all ranks (broadcast implicit via scatter knowledge)
        owner_map: Dict[str, int] = {}
        if size > 1:
            # gather owner mapping at root then bcast
            # construct local mapping for my chunk
            local_owners = {x['district']: rank for x in local_results}
            gathered = comm.gather(local_owners, root=0)
            if rank == 0:
                # not used in this branch
                pass
            else:
                owner_map = None
            if rank == 0:
                merged = {}
                for m in gathered:
                    if m:
                        merged.update(m)
                owner_map = merged
            owner_map = comm.bcast(owner_map, root=0)
        else:
            owner_map = {x['district']: 0 for x in local_results}

        # Identify boundary districts and neighbor ranks
        my_names = [x['district'] for x in local_results]
        need_from_rank: Dict[int, List[str]] = {}
        send_to_rank: Dict[int, Dict[str, List[Dict[str, Any]]]] = {}
        for dn in my_names:
            for nb in adjacency.get(dn, []):
                r = owner_map.get(nb)
                if r is None or r == rank:
                    continue
                need_from_rank.setdefault(r, []).append(nb)
        # Prepare histories to send to neighbors (my boundary)
        for other_r in set(need_from_rank.keys()):
            # find my districts that are neighbors of other_r's districts
            to_send_names = []
            for their_name in need_from_rank[other_r]:
                # any of my names that are neighbor of their_name
                for myn in my_names:
                    if myn in adjacency and their_name in adjacency and (myn in adjacency[their_name] or their_name in adjacency[myn]):
                        to_send_names.append(myn)
            uniq = sorted(set(to_send_names))
            payload = {n: history_map.get(n, []) for n in uniq}
            send_to_rank[other_r] = payload

        # Non-blocking halo exchange with neighbor ranks
        t_halo0 = time.perf_counter()
        recv_reqs: Dict[int, MPI.Request] = {}
        recv_buffers: Dict[int, Any] = {}
        send_reqs: List[MPI.Request] = []
        for other_r, payload in send_to_rank.items():
            send_reqs.append(comm.isend(payload, dest=other_r, tag=TAG_HALO))
            recv_reqs[other_r] = comm.irecv(source=other_r, tag=TAG_HALO)
        # wait for receives
        for other_r, req in recv_reqs.items():
            try:
                recv_buffers[other_r] = req.wait()
            except Exception:
                recv_buffers[other_r] = {}
        # ensure sends complete
        for sreq in send_reqs:
            try:
                sreq.wait()
            except Exception:
                pass
        boundary_exchange_time = time.perf_counter() - t_halo0

        # Build correlation map using received neighbor histories
        # Compose neighbor series by name
        neighbor_histories: Dict[str, List[Dict[str, Any]]] = {}
        for _r, payload in recv_buffers.items():
            if isinstance(payload, dict):
                neighbor_histories.update(payload)

        # For each of my districts, compute correlation with neighbor districts
        for dn in my_names:
            my_hist = history_map.get(dn, [])
            my_temps = [r.get('temperature_c') for r in my_hist if r.get('temperature_c') is not None]
            if len(my_temps) < 3:
                continue
            for nb in adjacency.get(dn, []):
                if nb not in neighbor_histories:
                    continue
                nb_hist = neighbor_histories.get(nb, [])
                nb_temps = [r.get('temperature_c') for r in nb_hist if r.get('temperature_c') is not None]
                if len(nb_temps) < 3:
                    continue
                corr = _pearson(my_temps[-10:], nb_temps[-10:])
                if corr is not None:
                    key = f"{dn}|{nb}"
                    correlation_map_local[key] = round(float(corr), 3)

        # Regional pattern classification for current anomalies
        my_anom_names = {r['district'] for r in anomaly_reports_local}
        for dn in my_anom_names:
            # count neighbor anomalies (based on simple z on neighbor latest history)
            neigh_anom_count = 0
            for nb in adjacency.get(dn, []):
                nb_hist = neighbor_histories.get(nb, [])
                temps = [r.get('temperature_c') for r in nb_hist if r.get('temperature_c') is not None]
                if len(temps) >= 5:
                    zt = _z_score(temps[-1], temps)
                    if zt is not None and abs(zt) >= 2.0:
                        neigh_anom_count += 1
            cls = 'isolated' if neigh_anom_count == 0 else ('regional' if neigh_anom_count >= 3 else 'local_cluster')
            regional_flags_local.append({'district': dn, 'pattern': cls, 'neighbor_anomaly_count': neigh_anom_count})

    # Gather results (Gather)
    if size > 1:
        all_results_lists = comm.gather(local_results, root=0)
        all_alerts_lists = comm.gather(local_alerts, root=0)
        per_rank_times = comm.gather(sum(local_request_durations), root=0)
        all_anomaly_reports = comm.gather(anomaly_reports_local, root=0)
        boundary_exchange_times = comm.gather(boundary_exchange_time, root=0)
        per_rank_anomaly_counts_list = comm.gather(per_rank_anomaly_counts, root=0)
        correlation_maps = comm.gather(correlation_map_local, root=0)
        regional_flags_lists = comm.gather(regional_flags_local, root=0)
    else:
        all_results_lists = [local_results]
        all_alerts_lists = [local_alerts]
        per_rank_times = [sum(local_request_durations)]
        all_anomaly_reports = [anomaly_reports_local]
        boundary_exchange_times = [boundary_exchange_time]
        per_rank_anomaly_counts_list = [per_rank_anomaly_counts]
        correlation_maps = [correlation_map_local]
        regional_flags_lists = [regional_flags_local]

    # Collective stats (Reduce)
    global_temp_sum = comm.reduce(local_temp_sum, op=MPI.SUM, root=0)
    global_temp_sumsq = comm.reduce(local_temp_sumsq, op=MPI.SUM, root=0)
    global_temp_count = comm.reduce(local_temp_count, op=MPI.SUM, root=0)
    global_max_temp = comm.reduce(local_max_temp, op=MPI.MAX, root=0)
    global_min_temp = comm.reduce(local_min_temp, op=MPI.MIN, root=0)

    # independent allgather for full dataset on every rank (Allgather)
    if size > 1:
        allgather_lists = comm.allgather(local_results)
    else:
        allgather_lists = [local_results]
    full_data_view = [item for sub in allgather_lists for item in sub]
    # Each rank computes independent global average from the allgather view
    if full_data_view:
        temps_view = [x['temperature_c'] for x in full_data_view if 'temperature_c' in x]
        independent_global_avg = sum(temps_view) / len(temps_view) if temps_view else None
    else:
        independent_global_avg = None
    # Gather these independent results to root to demonstrate consistency
    all_independent_avgs = comm.gather(independent_global_avg, root=0)

    # Synchronize end and timing
    comm.Barrier()
    t1 = MPI.Wtime()
    parallel_time = t1 - t0

    # Root composes outputs
    if rank == 0:
        all_results = [item for sub in all_results_lists for item in sub]
        # associate processor distribution
        processor_distribution: Dict[str, List[str]] = {}
        for r in range(size):
            processor_distribution[f'processor_{r}'] = [
                d['district'] for d in all_results if d.get('processor_rank') == r
            ]

        # build averages
        temps = [d['temperature_c'] for d in all_results if 'temperature_c' in d]
        humidity = [d['humidity_pct'] for d in all_results if 'humidity_pct' in d]
        rainfall = [d['rainfall_mm'] for d in all_results if 'rainfall_mm' in d]
        wind_speeds = [d['wind_speed_ms'] for d in all_results if 'wind_speed_ms' in d]

        averages = {
            'temperature_c': round(sum(temps) / len(temps), 2) if temps else None,
            'humidity_pct': round(sum(humidity) / len(humidity), 2) if humidity else None,
            'rainfall_mm': round(sum(rainfall) / len(rainfall), 2) if rainfall else None,
            'wind_speed_ms': round(sum(wind_speeds) / len(wind_speeds), 2) if wind_speeds else None,
        }

        # derive hottest/coldest district names via gather comparison
        hottest_name = None
        coldest_name = None
        if temps:
            hottest_temp = global_max_temp
            coldest_temp = global_min_temp
            for item in all_results:
                if 'temperature_c' in item:
                    if item['temperature_c'] == hottest_temp:
                        hottest_name = item['district']
                    if item['temperature_c'] == coldest_temp:
                        coldest_name = item['district']

        # variance from reduce sums
        variance = None
        if global_temp_count and global_temp_count > 0:
            mean = global_temp_sum / global_temp_count
            variance = (global_temp_sumsq / global_temp_count) - (mean * mean)
            if variance is not None:
                variance = round(variance, 4)

        # counts for criteria
        temp_gt_30 = len([1 for d in all_results if d.get('temperature_c') is not None and d.get('temperature_c') > 30])
        humidity_gt_80 = len([1 for d in all_results if d.get('humidity_pct') is not None and d.get('humidity_pct') > 80])

        # alerts summary
        all_alerts = [item for sub in all_alerts_lists for item in sub]
        alerts_by_district = {}
        for a in all_alerts:
            name = a['district']
            # keep worst severity per district
            prev = alerts_by_district.get(name)
            if not prev:
                alerts_by_district[name] = a
            else:
                order = {'none': 0, 'low': 1, 'medium': 2, 'high': 3}
                if order.get(a['severity'], 0) > order.get(prev['severity'], 0):
                    alerts_by_district[name] = a

        # metrics
        per_rank_exec = {f'rank_{i}': round(float(per_rank_times[i]), 4) if i < len(per_rank_times) else 0.0 for i in range(len(per_rank_times))}
        sequential_estimate = sum(per_rank_times) if per_rank_times else parallel_time
        speedup = (sequential_estimate / parallel_time) if parallel_time > 0 else None

        total_anomalies = sum(len(x) for x in all_anomaly_reports)
        # reduce-like severity distribution aggregation
        agg_sev = {'low': 0, 'medium': 0, 'high': 0, 'critical': 0}
        for dct in per_rank_anomaly_counts_list:
            for k in list(agg_sev.keys()):
                agg_sev[k] += int(dct.get(k, 0))

        metrics = {
            'execution_time_sec': round(parallel_time, 4),
            'estimated_sequential_time_sec': round(sequential_estimate, 4),
            'speedup_factor': round(speedup, 3) if speedup else None,
            'per_rank_execution_sec': per_rank_exec,
            'boundary_exchange_time_sec': {f'rank_{i}': round(float(boundary_exchange_times[i]), 4) for i in range(len(boundary_exchange_times))},
            'anomaly_detection_counts': {f'rank_{i}': per_rank_anomaly_counts_list[i] for i in range(len(per_rank_anomaly_counts_list))},
            'total_anomalies': total_anomalies,
            'severity_distribution': agg_sev,
            'hottest_district': {'name': hottest_name, 'temperature_c': global_max_temp if temps else None},
            'coldest_district': {'name': coldest_name, 'temperature_c': global_min_temp if temps else None},
            'temperature_variance': variance,
            'criteria_counts': {
                'temp_gt_30': temp_gt_30,
                'humidity_gt_80': humidity_gt_80,
            },
            'alert_summary': {
                'total_alerts': len(all_alerts),
                'by_district': alerts_by_district,
            },
            'allgather_independent_avgs': {f'rank_{i}': (round(val, 4) if val is not None else None) for i, val in enumerate(all_independent_avgs or [])},
        }

        # compose final dataset JSON
        final_data = {
            'last_updated': datetime.now().isoformat(),
            'total_processors_used': size,
            'processor_distribution': processor_distribution,
            'averages': averages,
            'districts': all_results,
        }

        # Write outputs
        _write_json_atomic(output_file, final_data)
        _write_json_atomic(metrics_file, metrics)

        # Aggregate anomalies and correlation data for visualization
        all_anoms = [item for sub in all_anomaly_reports for item in sub]
        # anomaly heatmap by district
        heatmap = {}
        for ar in all_anoms:
            name = ar['district']
            heatmap[name] = heatmap.get(name, 0) + 1
        # merge correlation maps
        corr_map: Dict[str, float] = {}
        for m in correlation_maps:
            corr_map.update(m)
        regional_flags = [item for sub in regional_flags_lists for item in sub]
        anomaly_payload = {
            'generated_at': datetime.now().isoformat(),
            'total_anomalies': total_anomalies,
            'severity_distribution': agg_sev,
            'heatmap': heatmap,
            'correlations': corr_map,
            'regional_patterns': regional_flags,
            'per_rank_counts': {f'rank_{i}': per_rank_anomaly_counts_list[i] for i in range(len(per_rank_anomaly_counts_list))},
        }
        _write_json_atomic(anomaly_file, anomaly_payload)

        # mark progress complete
        try:
            # Load latest progress (if exists) to preserve counters
            if os.path.exists(progress_file):
                with open(progress_file, 'r') as f:
                    prog = json.load(f)
            else:
                prog = {}
        except Exception:
            prog = {}
        prog.update({
            'status': 'done',
            'completed': True,
            'ended_at': datetime.now().isoformat(),
        })
        _write_json_atomic(progress_file, prog)

    return True