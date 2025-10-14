let chartInstance = null;
let progressInterval = null;

async function fetchData() {
    const res = await fetch('/api/data');
    if (!res.ok) {
        throw new Error('Failed to load data');
    }
    return res.json();
}

async function fetchProgress() {
    const res = await fetch('/api/progress');
    if (!res.ok) return { status: 'idle', completed: false, ranks: {} };
    return res.json();
}

async function fetchMetrics() {
    const res = await fetch('/api/metrics');
    if (!res.ok) return {};
    return res.json();
}

function renderAverages(averages) {
    document.getElementById('avgTemp').textContent = averages.temperature_c ?? '—';
    document.getElementById('avgHumidity').textContent = averages.humidity_pct ?? '—';
    document.getElementById('avgRain').textContent = averages.rainfall_mm ?? '—';
    document.getElementById('avgWind').textContent = averages.wind_speed_ms ?? '—';
}

function renderGrid(districts) {
    const grid = document.getElementById('grid');
    grid.innerHTML = '';
    districts.forEach(d => {
        const card = document.createElement('div');
        const sev = d.alert_severity || 'none';
        card.className = `card severity-${sev}`;

        // Adapting the data to fit the new design
        const weatherIconCode = '04d'; // Placeholder: Your backend would need to provide this.
        const weatherDescription = 'Light rain'; // Placeholder: Your backend would need to provide this.
        const precipitationPercentage = d.rainfall_mm !== null ? `${(d.rainfall_mm * 10).toFixed(0)}%` : '—'; // Placeholder: Adapting rainfall to a percentage
        const windSpeed = d.wind_speed_ms !== null ? `${(d.wind_speed_ms * 3.6).toFixed(0)} km/h` : '—'; // Convert m/s to km/h

        card.innerHTML = `
            <div class="card-header">
                <div class="district-name">${d.district}</div>
                <div class="processor-badge">Rank ${d.processor_rank}</div>
            </div>
            <div class="card-content">
                <div class="main-weather">
                    <img src="https://openweathermap.org/img/wn/${weatherIconCode}@2x.png" alt="Weather Icon" class="weather-icon">
                    <div class="temperature">
                        ${d.temperature_c ?? '—'} <span class="unit">°C</span>
                    </div>
                </div>
                <div class="details">
                    <div class="detail-item">Precipitation: ${precipitationPercentage}</div>
                    <div class="detail-item">Humidity: ${d.humidity_pct ?? '—'}%</div>
                    <div class="detail-item">Wind: ${windSpeed}</div>
                </div>
                <div class="summary">
                    <div class="weather-description">${weatherDescription}</div>
                </div>
            </div>
        `;
        grid.appendChild(card);
    });
}

function renderProcessorSummary(distribution) {
    const container = document.getElementById('processorSummary');
    if (!container) return;
    const entries = Object.entries(distribution || {});
    container.innerHTML = entries.map(([rank, list]) => {
        return `<div class="proc">${rank}: ${list.join(', ')}</div>`;
    }).join('');
}

function renderPerRankTimes(perRank) {
    const container = document.getElementById('perRankTimes');
    if (!container) return;
    const items = Object.entries(perRank || {}).map(([rank, sec]) => `<div class="rank-time"><strong>${rank}</strong>: ${sec}s</div>`);
    container.innerHTML = items.join('');
}

function renderMetrics(metrics) {
    if (!metrics) return;
    const elExec = document.getElementById('execTime');
    if (elExec) elExec.textContent = metrics.execution_time_sec ?? '—';
    const elSpeed = document.getElementById('speedup');
    if (elSpeed) elSpeed.textContent = metrics.speedup_factor ?? '—';
    const hottest = metrics.hottest_district;
    const coldest = metrics.coldest_district;
    const elHot = document.getElementById('hottest');
    const elCold = document.getElementById('coldest');
    if (elHot) elHot.textContent = hottest && hottest.name ? `${hottest.name} (${hottest.temperature_c}°C)` : '—';
    if (elCold) elCold.textContent = coldest && coldest.name ? `${coldest.name} (${coldest.temperature_c}°C)` : '—';
    const elVar = document.getElementById('variance');
    if (elVar) elVar.textContent = metrics.temperature_variance ?? '—';
    const elAlerts = document.getElementById('alertCount');
    if (elAlerts) elAlerts.textContent = metrics.alert_summary ? metrics.alert_summary.total_alerts : '—';
    renderPerRankTimes(metrics.per_rank_execution_sec || {});
    const allg = metrics.allgather_independent_avgs || {};
    const avgsEl = document.getElementById('allgatherAvgs');
    if (avgsEl) {
        avgsEl.innerHTML = `<div><strong>Allgather avg per rank</strong>: ${Object.entries(allg).map(([k,v]) => `${k}: ${v ?? '—'}`).join(' | ')}</div>`;
    }
}

function renderProgress(progress) {
    const container = document.getElementById('progressContainer');
    if (!container) return;
    container.innerHTML = '';
    const ranks = progress.ranks || {};
    Object.keys(ranks).forEach(r => {
        const { done, total } = ranks[r];
        const pct = total > 0 ? Math.round((done / total) * 100) : 0;
        const row = document.createElement('div');
        row.className = 'progress-row';
        row.innerHTML = `
            <div class="progress-label">Rank ${r}</div>
            <div class="progress-bar"><div class="progress-fill" style="width:${pct}%"></div></div>
            <div class="progress-value">${done}/${total}</div>
        `;
        container.appendChild(row);
    });
}

function updateLastUpdated(ts) {
    const el = document.getElementById('lastUpdated');
    if (el) {
      el.textContent = `Last updated: ${new Date(ts).toLocaleString()}`;
    }
}

function renderChart(districts, kind) {
    const labels = districts.map(d => d.district);
    let data = [];
    let label = '';
    switch (kind) {
        case 'temp':
            label = 'Temperature (°C)';
            data = districts.map(d => d.temperature_c ?? null);
            break;
        case 'humidity':
            label = 'Humidity (%)';
            data = districts.map(d => d.humidity_pct ?? null);
            break;
        case 'rain':
            label = 'Rainfall (mm)';
            data = districts.map(d => d.rainfall_mm ?? null);
            break;
        case 'wind':
            label = 'Wind Speed (m/s)';
            data = districts.map(d => d.wind_speed_ms ?? null);
            break;
        default:
            return;
    }

    const ctx = document.getElementById('chartCanvas').getContext('2d');
    if (chartInstance) {
        chartInstance.destroy();
    }
    chartInstance = new Chart(ctx, {
        type: 'bar',
        data: {
            labels,
            datasets: [{
                label,
                data,
                backgroundColor: 'rgba(30, 136, 229, 0.6)'
            }]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            scales: {
                y: { beginAtZero: true }
            }
        }
    });
}

async function loadAndRender() {
    try {
        const data = await fetchData();
        updateLastUpdated(data.last_updated || Date.now());
        
        // Conditional rendering based on the page
        if (document.getElementById('grid')) {
            renderAverages(data.averages || {});
            renderGrid(data.districts || []);
            renderProcessorSummary(data.processor_distribution || {});
        }
        
        if (document.getElementById('chartCanvas')) {
            renderChart(data.districts || [], 'temp');
        }

        const metrics = await fetchMetrics();
        renderMetrics(metrics);

    } catch (e) {
        console.error(e);
    }
}

async function refreshNow() {
    const btn = document.getElementById('refreshBtn');
    btn.disabled = true;
    try {
        const num = parseInt(document.getElementById('numProcs')?.value || '4', 10);
        await fetch('/api/refresh/start', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ num_processors: num }) });
        // Poll for progress
        if (progressInterval) clearInterval(progressInterval);
        progressInterval = setInterval(async () => {
            const prog = await fetchProgress();
            renderProgress(prog);
            if (prog.completed) {
                clearInterval(progressInterval);
                const freshData = await fetchData();
                updateLastUpdated(freshData.last_updated || Date.now());
                renderAverages(freshData.averages || {});
                renderGrid(freshData.districts || []);
                renderProcessorSummary(freshData.processor_distribution || {});
                const metrics = await fetchMetrics();
                renderMetrics(metrics);
            }
        }, 600);
    } catch (e) {
        console.error(e);
    } finally {
        btn.disabled = false;
    }
}

function setupEvents() {
    if (document.getElementById('refreshBtn')) {
        document.getElementById('refreshBtn').addEventListener('click', refreshNow);
    }
    const sel = document.getElementById('numProcs');
    if (sel) sel.addEventListener('change', () => {});

    if (document.getElementById('chartCanvas')) {
        document.querySelectorAll('.chart-buttons button').forEach(btn => {
            btn.addEventListener('click', () => {
                const kind = btn.getAttribute('data-chart');
                fetchData().then(data => renderChart(data.districts || [], kind));
            });
        });
    }

    // Auto-refresh every 10 minutes
    setInterval(() => {
        refreshNow();
    }, 10 * 60 * 1000);
}

window.addEventListener('DOMContentLoaded', () => {
    setupEvents();
    loadAndRender();
});