const DATA = 'data/';
let navChart = null;
let journalData = [];
let journalFilter = 'all';
let journalPage = 0;
const JOURNAL_PER_PAGE = 10;

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

function esc(s) {
    if (!s) return '';
    const d = document.createElement('div');
    d.textContent = s;
    return d.innerHTML;
}

// --- Renderers ---

function renderMeta(meta) {
    if (!meta) return;
    el('last-updated').textContent = 'Updated ' + timeAgo(meta.exported_at);
    const dot = el('alpaca-status');
    dot.classList.toggle('ok', meta.alpaca_connected);
    dot.classList.toggle('err', !meta.alpaca_connected);
    dot.title = meta.alpaca_connected ? 'Alpaca connected' : 'Alpaca disconnected';
}

function renderFund(fund, research) {
    if (!fund) return;

    // Hero metrics
    el('nav-value').textContent = fmt$(fund.current?.equity);

    const ret = fund.performance?.total_return_pct;
    const retEl = el('total-return');
    retEl.textContent = ret != null ? fmtPct(ret) : '--';
    if (ret != null) retEl.className = 'metric-value ' + (ret >= 0 ? 'positive' : 'negative');

    // Hypotheses tested = signals + dead ends
    const k = research?.knowledge;
    if (k) {
        const tested = (k.signal_count || 0) + (k.dead_end_count || 0);
        el('hypotheses-tested').textContent = tested;
    }

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
                tooltip: { callbacks: { label: ctx => '$' + ctx.parsed.y.toLocaleString() } }
            },
            scales: {
                x: {
                    grid: { color: 'rgba(255,255,255,0.04)' },
                    ticks: { color: '#6b6b80', font: { size: 11 } }
                },
                y: {
                    grid: { color: 'rgba(255,255,255,0.04)' },
                    ticks: {
                        color: '#6b6b80', font: { size: 11 },
                        callback: v => '$' + (v / 1000).toFixed(0) + 'k'
                    }
                }
            }
        }
    });
}

function renderKnowledge(res) {
    if (!res) return;
    const k = res.knowledge || {};

    el('k-signals').textContent = k.signal_count ?? 0;
    el('k-dead-ends').textContent = k.dead_end_count ?? 0;
    el('k-literature').textContent = k.literature_count ?? 0;

    // Working signals
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

    // Dead ends — show last 8
    const deEl = el('dead-ends-list');
    if (k.dead_ends?.length) {
        const recent = k.dead_ends.slice(-8);
        const remaining = k.dead_ends.length - recent.length;
        let html = recent.map(d => `
            <div class="knowledge-item">
                <div class="name">${esc(d.name.replace(/_/g, ' '))}</div>
                <div class="detail">${esc(d.reason)}</div>
            </div>
        `).join('');
        if (remaining > 0) {
            html += `<p class="muted">+ ${remaining} more</p>`;
        }
        deEl.innerHTML = html;
    } else {
        deEl.innerHTML = '<p class="muted">None yet</p>';
    }
}

const STATE_STYLES = {
    EXPLORING: { color: '#4fc3f7', label: 'Exploring' },
    PROMISING: { color: '#66bb6a', label: 'Promising' },
    FAILING: { color: '#ffa726', label: 'Failing' },
    VALIDATED: { color: '#2e7d32', label: 'Validated' },
    RETIRED: { color: '#888', label: 'Retired' },
};

function renderLiveSignals(res) {
    const signals = res?.live_signals || [];
    const listEl = el('live-signals-list');

    if (!signals.length) {
        listEl.innerHTML = '<p class="muted">No signals under live test yet</p>';
    } else {
        listEl.innerHTML = signals.map(s => {
            const st = STATE_STYLES[s.state] || STATE_STYLES.EXPLORING;
            const dots = (s.experiments || []).map(e => {
                const c = e.correct ? '#66bb6a' : '#ef5350';
                return `<span title="${esc(e.symbol)}: ${fmtPct(e.return_pct)}" style="display:inline-block;width:12px;height:12px;border-radius:50%;background:${c};margin:0 2px;"></span>`;
            }).join('');

            const effN = s.effective_independent_n || 0;
            const effCorrect = s.effective_correct_n || 0;
            const raw = s.total_experiments || 0;
            let overlap = '';
            if (effN < raw) overlap = ` <span class="muted">(${raw} experiments, ${raw - effN} overlapped)</span>`;

            return `
                <div style="border:1px solid rgba(255,255,255,0.1);border-radius:8px;padding:12px 16px;margin:8px 0;">
                    <div style="display:flex;justify-content:space-between;align-items:center;">
                        <strong>${esc(s.event_type.replace(/_/g, ' '))}</strong>
                        <span style="background:${st.color};color:#fff;padding:2px 10px;border-radius:12px;font-size:12px;">${st.label}</span>
                    </div>
                    <div style="margin-top:6px;">${dots}</div>
                    <div style="margin-top:6px;font-size:13px;">${effCorrect}/${effN} independent tests correct${overlap}</div>
                </div>
            `;
        }).join('');
    }

    // Focus status
    const focus = res?.focus;
    const focusEl = el('focus-status');
    if (focus) {
        const c = focus.over_limit ? '#ef5350' : '#66bb6a';
        focusEl.innerHTML = `<span style="color:${c};font-size:13px;font-weight:bold;">${focus.active_signal_types}/${focus.max_signal_types} signal types active</span>`;
    }
}

function renderActiveNow(pos, res) {
    // Active positions
    const activeEl = el('active-positions');
    if (!pos?.active?.length) {
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

    // Today's research — most recent journal entries from today
    const todayEl = el('todays-research');
    const today = new Date().toISOString().slice(0, 10);
    const todayEntries = (res?.journal || []).filter(j => j.date === today);
    if (todayEntries.length) {
        const latest = todayEntries[todayEntries.length - 1];
        todayEl.innerHTML = `
            <div class="today-focus">
                <div class="today-investigated">${esc(latest.investigated)}</div>
                <div class="today-findings">${esc(latest.findings)}</div>
            </div>
            <div class="muted" style="margin-top:6px">${todayEntries.length} session${todayEntries.length > 1 ? 's' : ''} today</div>
        `;
    } else {
        todayEl.innerHTML = '<p class="muted">No sessions today</p>';
    }
}

function renderPipeline(res) {
    const p = res?.pipeline;
    if (!p) return;

    // Watchlist
    const wEl = el('watchlist');
    if (p.watchlist?.length) {
        wEl.innerHTML = p.watchlist.map(w => `
            <div class="pipeline-item">
                <span class="pip-date">${esc(w.date)}</span>
                <span class="pip-symbol">${esc(w.symbol)}</span>
                <span class="pip-desc">${esc(w.event.slice(0, 80))}</span>
            </div>
        `).join('');
    } else {
        wEl.innerHTML = '<p class="muted">Nothing on watch</p>';
    }

    // Pending triggers
    const tEl = el('pending-triggers');
    if (p.pending_triggers?.length) {
        tEl.innerHTML = p.pending_triggers.map(t => `
            <div class="pipeline-item">
                <span class="pip-symbol">${esc(t.symbol)}</span>
                <span class="direction ${t.direction}">${t.direction}</span>
                <span class="pip-desc">${esc(t.event_type)}</span>
            </div>
        `).join('');
    } else {
        tEl.innerHTML = '<p class="muted">No queued trades</p>';
    }

    // Research queue
    const rEl = el('research-queue');
    if (p.research_queue?.length) {
        rEl.innerHTML = p.research_queue.map(q => `
            <div class="pipeline-item">
                <span class="pip-desc">${esc(q.question.slice(0, 120))}</span>
            </div>
        `).join('');
    } else {
        rEl.innerHTML = '<p class="muted">Queue empty</p>';
    }
}

function renderRecentTrades(pos) {
    const closedEl = el('recent-trades');
    if (!pos?.recent_closed?.length) {
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

// --- Journal with pagination and filters ---

const TAG_LABELS = {
    discovery: 'Discovery',
    dead_end: 'Dead End',
    validation: 'Validation',
    exploration: 'Exploration',
    operational: 'Operational',
    infrastructure: 'Infrastructure',
};
const TAG_CLASSES = {
    discovery: 'tag-discovery',
    dead_end: 'tag-dead-end',
    validation: 'tag-validation',
    exploration: 'tag-exploration',
    operational: 'tag-operational',
    infrastructure: 'tag-infrastructure',
};

function getFilteredJournal() {
    if (journalFilter === 'all') return journalData;
    return journalData.filter(j => j.tag === journalFilter);
}

function renderJournalPage() {
    const filtered = getFilteredJournal();
    const jEl = el('journal-list');
    const total = filtered.length;
    const totalPages = Math.max(1, Math.ceil(total / JOURNAL_PER_PAGE));
    journalPage = Math.min(journalPage, totalPages - 1);
    const start = journalPage * JOURNAL_PER_PAGE;
    const page = filtered.slice(start, start + JOURNAL_PER_PAGE);

    if (!total) {
        jEl.innerHTML = '<p class="muted">No entries match this filter</p>';
    } else {
        jEl.innerHTML = page.map(j => {
            const tag = j.tag || 'exploration';
            const borderClass = TAG_CLASSES[tag] || 'tag-exploration';
            return `
                <div class="journal-entry ${borderClass}">
                    <div class="journal-header">
                        <span class="date">${esc(j.date)}</span>
                        <span class="tag-badge ${borderClass}">${TAG_LABELS[tag] || tag}</span>
                    </div>
                    <div class="investigated">${esc(j.investigated)}</div>
                    <div class="findings">${esc(j.findings)}</div>
                </div>
            `;
        }).join('');
    }

    el('journal-prev').disabled = journalPage <= 0;
    el('journal-next').disabled = journalPage >= totalPages - 1;
    el('journal-page-info').textContent = `Page ${journalPage + 1} of ${totalPages} (${total} entries)`;
}

function initJournal(res) {
    journalData = (res?.journal || []).slice().reverse();  // newest first
    journalPage = 0;
    journalFilter = 'all';
    renderJournalPage();

    // Pagination buttons
    el('journal-prev').onclick = () => { journalPage--; renderJournalPage(); };
    el('journal-next').onclick = () => { journalPage++; renderJournalPage(); };

    // Filter buttons
    document.querySelectorAll('.filter-btn').forEach(btn => {
        btn.addEventListener('click', () => {
            document.querySelectorAll('.filter-btn').forEach(b => b.classList.remove('active'));
            btn.classList.add('active');
            journalFilter = btn.dataset.filter;
            journalPage = 0;
            renderJournalPage();
        });
    });
}

// --- Investigations (hypotheses with reports) ---

let hypData = [];
let hypFilter = 'all';
let hypPage = 0;
const HYP_PER_PAGE = 10;

const STATUS_LABELS = {
    active: 'Active',
    pending: 'Pending',
    completed: 'Completed',
    invalidated: 'Invalidated',
};

function getFilteredHyps() {
    if (hypFilter === 'all') return hypData;
    return hypData.filter(h => h.status === hypFilter);
}

function renderHypPage() {
    const filtered = getFilteredHyps();
    const listEl = el('hyp-list');
    const total = filtered.length;
    const totalPages = Math.max(1, Math.ceil(total / HYP_PER_PAGE));
    hypPage = Math.min(hypPage, totalPages - 1);
    const start = hypPage * HYP_PER_PAGE;
    const page = filtered.slice(start, start + HYP_PER_PAGE);

    if (!total) {
        listEl.innerHTML = '<p class="muted">No hypotheses match this filter</p>';
    } else {
        listEl.innerHTML = page.map(h => {
            let resultHtml = '';
            if (h.status === 'completed' && h.result_pct != null) {
                const cls = h.direction_correct ? 'positive' : 'negative';
                resultHtml = `<span class="hyp-result ${cls}">${fmtPct(h.abnormal_pct || h.result_pct)}</span>`;
            } else if (h.status === 'active') {
                resultHtml = '<span class="hyp-result muted">open</span>';
            } else if (h.status === 'invalidated') {
                resultHtml = '<span class="hyp-result muted">—</span>';
            } else {
                resultHtml = '<span class="hyp-result muted">—</span>';
            }

            return `
                <div class="hyp-item status-border-${h.status}" data-hyp-id="${esc(h.id)}" onclick="openReport('${esc(h.id)}')">
                    <span class="status-badge status-${h.status}">${STATUS_LABELS[h.status] || h.status}</span>
                    <span class="hyp-symbol">${esc(h.symbol)}</span>
                    <span class="direction ${h.direction}">${h.direction}</span>
                    <span class="hyp-thesis">${esc(h.thesis)}</span>
                    ${resultHtml}
                    <span class="hyp-confidence">${h.confidence != null ? h.confidence + '/10' : ''}</span>
                </div>
            `;
        }).join('');
    }

    el('hyp-prev').disabled = hypPage <= 0;
    el('hyp-next').disabled = hypPage >= totalPages - 1;
    el('hyp-page-info').textContent = `Page ${hypPage + 1} of ${totalPages} (${total} hypotheses)`;
}

function openReport(id) {
    const h = hypData.find(h => h.id === id);
    if (!h || !h.report) return;
    el('modal-title').textContent = `Report: ${h.id} — ${h.symbol} ${h.direction}`;
    el('modal-body').textContent = h.report;
    el('report-modal').style.display = 'flex';
}

function closeModal() {
    el('report-modal').style.display = 'none';
}

function initHypotheses(data) {
    if (!data) return;
    hypData = data.hypotheses || [];
    hypPage = 0;
    hypFilter = 'all';

    // Stats bar
    const c = data.counts || {};
    el('hyp-stats').innerHTML = [
        `<span><span class="hyp-count">${c.total || 0}</span> total</span>`,
        `<span><span class="hyp-count">${c.active || 0}</span> active</span>`,
        `<span><span class="hyp-count">${c.pending || 0}</span> pending</span>`,
        `<span><span class="hyp-count">${c.completed || 0}</span> completed</span>`,
        `<span><span class="hyp-count">${c.invalidated || 0}</span> invalidated</span>`,
    ].join('');

    renderHypPage();

    // Pagination
    el('hyp-prev').onclick = () => { hypPage--; renderHypPage(); };
    el('hyp-next').onclick = () => { hypPage++; renderHypPage(); };

    // Filters
    document.querySelectorAll('[data-hyp-filter]').forEach(btn => {
        btn.addEventListener('click', () => {
            document.querySelectorAll('[data-hyp-filter]').forEach(b => b.classList.remove('active'));
            btn.classList.add('active');
            hypFilter = btn.dataset.hypFilter;
            hypPage = 0;
            renderHypPage();
        });
    });

    // Modal close
    el('modal-close').onclick = closeModal;
    el('report-modal').onclick = (e) => {
        if (e.target === el('report-modal')) closeModal();
    };
    document.addEventListener('keydown', (e) => {
        if (e.key === 'Escape') closeModal();
    });
}

// --- Load ---

async function load() {
    const [fund, positions, research, meta, hypotheses] = await Promise.all([
        fetchJSON('fund.json'),
        fetchJSON('positions.json'),
        fetchJSON('research.json'),
        fetchJSON('meta.json'),
        fetchJSON('hypotheses.json'),
    ]);

    renderMeta(meta);
    renderFund(fund, research);
    renderLiveSignals(research);
    renderKnowledge(research);
    renderActiveNow(positions, research);
    renderPipeline(research);
    initHypotheses(hypotheses);
    renderRecentTrades(positions);
    initJournal(research);
}

// Initial load + auto-refresh every 5 min
load();
setInterval(load, 5 * 60 * 1000);
