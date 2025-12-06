// Dashboard JavaScript - Real-time updates and charts

let rowsChart = null;
let tagsChart = null;

// Load dashboard data
async function loadDashboardData() {
    try {
        await Promise.all([
            loadStatus(),
            loadTimeseries(),
            loadTagsDistribution(),
            loadPipelineHealth(),
            loadRecentEvents()
        ]);
    } catch (error) {
        console.error('Error loading dashboard data:', error);
    }
}

// Load status KPIs
async function loadStatus() {
    const response = await fetch('/api/ingestion/status');
    const data = await response.json();

    document.getElementById('status').textContent = data.status;
    document.getElementById('last_run').textContent = formatTime(data.last_run);
    document.getElementById('rows_last_hour').textContent = formatNumber(data.rows_loaded_last_hour);
    document.getElementById('tags_ingested').textContent = formatNumber(data.tags_ingested);
    document.getElementById('event_frames').textContent = formatNumber(data.event_frames_ingested);
    document.getElementById('quality_score').textContent = data.data_quality_score + '%';
    document.getElementById('avg_latency').textContent = data.avg_latency_seconds + 's';
    document.getElementById('active_pipelines').textContent = data.active_pipelines + '/' + data.pipeline_groups;
}

// Load time-series data and create chart
async function loadTimeseries() {
    const response = await fetch('/api/ingestion/timeseries');
    const data = await response.json();

    const ctx = document.getElementById('rowsChart');

    if (rowsChart) {
        rowsChart.destroy();
    }

    rowsChart = new Chart(ctx, {
        type: 'line',
        data: {
            labels: data.timestamps,
            datasets: [
                {
                    label: 'Rows per Minute',
                    data: data.rows_per_minute,
                    borderColor: '#FF6B35',
                    backgroundColor: 'rgba(255, 107, 53, 0.1)',
                    borderWidth: 2,
                    fill: true,
                    tension: 0.4
                },
                {
                    label: 'Errors',
                    data: data.errors_per_minute,
                    borderColor: '#E63946',
                    backgroundColor: 'rgba(230, 57, 70, 0.1)',
                    borderWidth: 2,
                    fill: true,
                    tension: 0.4
                }
            ]
        },
        options: {
            responsive: true,
            maintainAspectRatio: true,
            plugins: {
                legend: {
                    position: 'top',
                },
                tooltip: {
                    mode: 'index',
                    intersect: false,
                }
            },
            scales: {
                y: {
                    beginAtZero: true,
                    title: {
                        display: true,
                        text: 'Records'
                    }
                },
                x: {
                    title: {
                        display: true,
                        text: 'Time'
                    }
                }
            }
        }
    });
}

// Load tags distribution and create chart
async function loadTagsDistribution() {
    const response = await fetch('/api/ingestion/tags');
    const data = await response.json();

    const ctx = document.getElementById('tagsChart');

    if (tagsChart) {
        tagsChart.destroy();
    }

    tagsChart = new Chart(ctx, {
        type: 'bar',
        data: {
            labels: data.plants,
            datasets: [{
                label: 'Tags per Plant',
                data: data.tag_counts,
                backgroundColor: data.colors,
                borderWidth: 0
            }]
        },
        options: {
            responsive: true,
            maintainAspectRatio: true,
            plugins: {
                legend: {
                    display: false
                },
                tooltip: {
                    callbacks: {
                        label: function(context) {
                            return context.parsed.y + ' tags';
                        }
                    }
                }
            },
            scales: {
                y: {
                    beginAtZero: true,
                    title: {
                        display: true,
                        text: 'Number of Tags'
                    }
                }
            }
        }
    });
}

// Load pipeline health status
async function loadPipelineHealth() {
    const response = await fetch('/api/ingestion/pipeline_health');
    const pipelines = await response.json();

    const container = document.getElementById('pipeline_health');
    container.innerHTML = '';

    pipelines.forEach(pipeline => {
        const statusClass = pipeline.status.toLowerCase();
        const item = document.createElement('div');
        item.className = `pipeline-item ${statusClass}`;
        item.innerHTML = `
            <div class="pipeline-header">
                <span class="pipeline-name">${pipeline.name}</span>
                <span class="pipeline-status ${statusClass}">${pipeline.status}</span>
            </div>
            <div class="pipeline-stats">
                <span>Tags: ${formatNumber(pipeline.tags)}</span>
                <span>Avg Duration: ${pipeline.avg_duration_seconds}s</span>
                <span>Success Rate: ${pipeline.success_rate}%</span>
                <span>Last Run: ${formatTime(pipeline.last_run)}</span>
            </div>
        `;
        container.appendChild(item);
    });
}

// Load recent events
async function loadRecentEvents() {
    const response = await fetch('/api/ingestion/recent_events');
    const events = await response.json();

    const container = document.getElementById('recent_events');
    container.innerHTML = '';

    events.forEach(event => {
        const item = document.createElement('div');
        item.className = `event-item ${event.type}`;
        item.innerHTML = `
            <span class="event-time">${event.time}</span>
            <span class="event-message">${event.message}</span>
        `;
        container.appendChild(item);
    });
}

// Utility functions
function formatNumber(num) {
    return new Intl.NumberFormat('en-US').format(num);
}

function formatTime(isoString) {
    const date = new Date(isoString);
    const now = new Date();
    const diffMs = now - date;
    const diffMins = Math.floor(diffMs / 60000);

    if (diffMins < 1) {
        return 'Just now';
    } else if (diffMins < 60) {
        return `${diffMins} min ago`;
    } else if (diffMins < 1440) {
        const hours = Math.floor(diffMins / 60);
        return `${hours} hour${hours > 1 ? 's' : ''} ago`;
    } else {
        return date.toLocaleDateString() + ' ' + date.toLocaleTimeString();
    }
}

// Auto-refresh every 30 seconds
setInterval(() => {
    loadDashboardData();
}, 30000);

// Initial load
loadDashboardData();
