let chartInstance = null;

async function fetchData() {
    const res = await fetch('/api/data');
    if (!res.ok) {
        throw new Error('Failed to load data');
    }
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
        card.className = 'card';

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
        }
        
        if (document.getElementById('chartCanvas')) {
            renderChart(data.districts || [], 'temp');
        }

    } catch (e) {
        console.error(e);
    }
}

async function refreshNow() {
    const btn = document.getElementById('refreshBtn');
    btn.disabled = true;
    try {
        const res = await fetch('/api/refresh', { method: 'POST' });
        const json = await res.json();
        if (json && json.data) {
            updateLastUpdated(json.data.last_updated || Date.now());
            
            if (document.getElementById('grid')) {
                renderAverages(json.data.averages || {});
                renderGrid(json.data.districts || []);
            }

            if (document.getElementById('chartCanvas')) {
                // Fetch data again to re-render the chart
                const freshData = await fetchData();
                renderChart(freshData.districts || [], 'temp');
            }
        }
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