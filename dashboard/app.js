const DATA = 'data/';
let navChart = null;

async function fetchJSON(file) {
    try {
        const r = await fetch(DATA + file + '?t=' + Date.now());
        if (!r.ok) throw new Error(r.status);
        return await r.json();
    } catch (e) {
        console.warn(`Failed to load ${file}:`, e);
        return null;
    }
}

function fmt$(n) {
    if (n == null) return '--';
    return '$' + Number(n).toLocaleString('en-US', { minimumFractionDigits: 0, maximumFractionDigits: 0 });
}

function fmtPct(n, decimals = 1) {
    if (n == null) return '--';
    return (n >= 0 ? '+' : '') + Number(n).toFixed(decimals) + '%';
}

function timeAgo(iso) {
    const ms = Date.now() - new Date(iso).getTime();
    const min = Math.floor(ms / 60000);
    if (min < 1) return 'just now';
    if (min < 60) return min + 'm ago';
    const hrs = Math.floor(min / 60);
    if (hrs < 24) return hrs + 'h ago';
    return Math.floor(hrs / 24) + 'd ago';
}

function el(id) { return document.getElementById(id); }

function renderMeta(meta) {
    if (!meta) return;
    el('last-updated').textContent = 'Updated ' + timeAgo(meta.exported_at);
    const dot = el('alpaca-status');
    dot.classList.toggle('ok', meta.alpaca_connected);
    dot.classList.toggle('err', !meta.alpaca_connected);
    dot.title = meta.alpaca_connected ? 'Alpaca connected' : 'Alpaca disconnected';
}

function renderFund(fund) {
    if (!fund) return;

    // Hero metrics
    const eq = fund.current?.equity;
    el('nav-value').textContent = fmt$(eq);

    const ret = fund.performance?.total_return_pct;
    const retEl = el('total-return');
    retEl.textContent = ret != null ? fmtPct(ret) : '--';
    if (ret != null) retEl.className = 'metric-value ' + (ret >= 0 ? 'positive' : 'negative');

    const dd = fund.performance?.max_drawdown_pct;
    el('max-drawdown').textContent = dd != null ? dd.toFixed(1) + '%' : '--';

    const wr = fund.performance?.win_rate_pct;
    el('win-rate').textContent = wr != null ? wr.toFixed(0) + '%' : '--';

    // Risk
    el('risk-dd').textContent = fund.risk?.drawdown_pct != null ? fund.risk.drawdown_pct + '%' : '--';
    el('risk-limit').textContent = fund.risk?.drawdown_limit_pct != null ? fund.risk.drawdown_limit_pct + '%' : '--';
    const safe = fund.risk?.safe_to_trade;
    const statusEl = el('risk-status');
    if (safe != null) {
        statusEl.textContent = safe ? 'SAFE' : 'HALTED';
        statusEl.className = 'stat-value ' + (safe ? 'positive' : 'negative');
    }
    el('risk-positions').textContent = fund.risk?.active_positions != null
        ? fund.risk.active_positions + '/' + (fund.risk.max_concurrent || 5)
        : '--';

    // Performance
    el('perf-trades').textContent = fund.performance?.total_trades ?? '--';
    el('perf-wins').textContent = fund.performance?.winning_trades ?? '--';
    el('perf-avg-win').textContent = fund.performance?.avg_win_pct != null ? fmtPct(fund.performance.avg_win_pct) : '--';
    el('perf-avg-loss').textContent = fund.performance?.avg_loss_pct != null ? fmtPct(fund.performance.avg_loss_pct) : '--';

    // NAV Chart
    renderChart(fund.nav_history || []);
}

function renderChart(history) {
    if (!history.length) return;
    const ctx = el('nav-chart').getContext('2d');

    const labels = history.map(p => p.date);
    const data = history.map(p => p.equity);

    if (navChart) navChart.destroy();

    navChart = new Chart(ctx, {
        type: 'line',
        data: {
            labels,
            datasets: [{
                data,
                borderColor: '#4fc3f7',
                backgroundColor: 'rgba(79, 195, 247, 0.08)',
                fill: true,
                tension: 0.3,
                pointRadius: 3,
                pointBackgroundColor: '#4fc3f7',
                borderWidth: 2,
            }]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            plugins: {
                legend: { display: false },
                tooltip: {
                    callbacks: {
                        label: ctx => '$' + ctx.parsed.y.toLocaleString(),
                    }
                }
            },
            scales: {
                x: {
                    grid: { color: 'rgba(255,255,255,0.04)' },
                    ticks: { color: '#6b6b80', font: { size: 11 } }
                },
                y: {
                    grid: { color: 'rgba(255,255,255,0.04)' },
                    ticks: {
                        color: '#6b6b80',
                        font: { size: 11 },
                        callback: v => '$' + (v / 1000).toFixed(0) + 'k'
                    }
                }
            }
        }
    });
}

function renderPositions(pos) {
    if (!pos) return;

    // Active
    const activeEl = el('active-positions');
    if (!pos.active?.length) {
        activeEl.innerHTML = '<p class="muted">No active positions</p>';
    } else {
        activeEl.innerHTML = pos.active.map(p => `
            <div class="position-item">
                <span class="symbol">${esc(p.symbol)}</span>
                <span class="direction ${p.direction}">${p.direction}</span>
                <span class="thesis">${esc(p.thesis)}</span>
            </div>
        `).join('');
    }

    // Recent closed
    const closedEl = el('recent-trades');
    if (!pos.recent_closed?.length) {
        closedEl.innerHTML = '<p class="muted">No completed trades</p>';
    } else {
        closedEl.innerHTML = pos.recent_closed.map(p => {
            const pct = p.result_pct;
            const cls = pct != null ? (pct >= 0 ? 'positive' : 'negative') : '';
            return `
                <div class="position-item">
                    <span class="symbol">${esc(p.symbol)}</span>
                    <span class="direction ${p.direction}">${p.direction}</span>
                    <span class="thesis">${esc(p.thesis)}</span>
                    <span class="result ${cls}">${pct != null ? fmtPct(pct) : '--'}</span>
                </div>
            `;
        }).join('');
    }
}

function renderResearch(res) {
    if (!res) return;

    const s = res.summary || {};
    el('res-hypotheses').textContent = s.total_hypotheses ?? '--';
    el('res-active').textContent = s.active ?? '--';
    el('res-completed').textContent = s.completed ?? '--';

    const k = res.knowledge || {};
    el('res-signals').textContent = k.signal_count ?? '--';
    el('res-dead-ends').textContent = k.dead_end_count ?? '--';
    el('res-sessions').textContent = res.activity?.sessions_today ?? '--';

    // Signals list
    const sigEl = el('signals-list');
    if (k.signals?.length) {
        sigEl.innerHTML = k.signals.map(s => `
            <div class="knowledge-item">
                <div class="name">${esc(s.name.replace(/_/g, ' '))}</div>
                <div class="detail">${s.magnitude_pct != null ? fmtPct(s.magnitude_pct) + ' avg' : s.status}</div>
            </div>
        `).join('');
    } else {
        sigEl.innerHTML = '<p class="muted">None yet</p>';
    }

    // Dead ends list
    const deEl = el('dead-ends-list');
    if (k.dead_ends?.length) {
        deEl.innerHTML = k.dead_ends.map(d => `
            <div class="knowledge-item">
                <div class="name">${esc(d.name.replace(/_/g, ' '))}</div>
                <div class="detail">${esc(d.reason)}</div>
            </div>
        `).join('');
    } else {
        deEl.innerHTML = '<p class="muted">None yet</p>';
    }

    // Journal
    const jEl = el('journal-list');
    if (res.journal?.length) {
        jEl.innerHTML = res.journal.map(j => `
            <div class="journal-entry">
                <div class="date">${esc(j.date)}</div>
                <div class="investigated">${esc(j.investigated)}</div>
                <div class="findings">${esc(j.findings)}</div>
            </div>
        `).join('');
    } else {
        jEl.innerHTML = '<p class="muted">No journal entries</p>';
    }
}

function esc(s) {
    if (!s) return '';
    const d = document.createElement('div');
    d.textContent = s;
    return d.innerHTML;
}

async function load() {
    const [fund, positions, research, meta] = await Promise.all([
        fetchJSON('fund.json'),
        fetchJSON('positions.json'),
        fetchJSON('research.json'),
        fetchJSON('meta.json'),
    ]);

    renderMeta(meta);
    renderFund(fund);
    renderPositions(positions);
    renderResearch(research);
}

// Initial load + auto-refresh every 5 min
load();
setInterval(load, 5 * 60 * 1000);
