"""
Email reporting — sends daily research digests via Gmail SMTP.
"""

import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from datetime import datetime

from config import GMAIL_ADDRESS, GMAIL_APP_PASSWORD, REPORT_RECIPIENT
from research import (
    get_active_hypotheses,
    get_pending_hypotheses,
    get_completed_hypotheses,
    get_research_summary,
    load_patterns,
    load_knowledge,
)
from trader import get_account_summary

# ---------------------------------------------------------------------------
# HTML Template constants — keep layout separate from logic
# ---------------------------------------------------------------------------

_PAGE_OPEN = (
    '<html><body style="font-family: -apple-system, Arial, sans-serif; '
    'max-width: 700px; margin: 0 auto; color: #333;">'
)

_PAGE_CLOSE = (
    '<hr style="margin-top: 20px;">'
    '<p style="color: #aaa; font-size: 11px;">Stock Market Causal Research</p>'
    '</body></html>'
)

_STAT_CELL = (
    '<td style="padding: 8px 20px; text-align: center;">'
    '<div style="font-size: 24px; font-weight: bold;">{value}</div>'
    '<div style="color: #888; font-size: 12px;">{label}</div></td>'
)

_STATUS_COLORS = {
    "pending": "#1565c0", "active": "#e65100",
    "completed": "#2e7d32", "retired": "#888",
}


def build_daily_report():
    """Build a clean, scannable daily research digest."""
    import html as html_mod
    import db as _db

    summary = get_account_summary()
    research = get_research_summary()
    active = get_active_hypotheses()
    pending = get_pending_hypotheses()
    completed = get_completed_hypotheses()
    knowledge = load_knowledge()

    known_count = len(knowledge.get("known_effects", {}))
    dead_count = len(knowledge.get("dead_ends", []))
    positions = summary.get("positions", [])
    equity = summary.get("equity", 0)
    starting_equity = 100_000
    total_pl = equity - starting_equity
    total_pl_pct = (total_pl / starting_equity) * 100

    html = _PAGE_OPEN + f"""
    <h2>Daily Research Digest</h2>
    <p style="color: #888;">{datetime.now().strftime('%A, %B %d %Y')}</p>
    """

    # --- Portfolio snapshot ---
    pl_color = "#2e7d32" if total_pl >= 0 else "#c62828"
    html += f"""
    <table style="border-collapse: collapse; margin: 16px 0;"><tr>
    {_STAT_CELL.format(value=f"${equity:,.0f}", label="Equity")}
    {_STAT_CELL.format(value=f'<span style="color: {pl_color}">{total_pl:+,.0f}</span>', label="Total P&L")}
    {_STAT_CELL.format(value=len(positions), label="Open positions")}
    {_STAT_CELL.format(value=research.get('direction_accuracy', 'n/a'), label="Win rate")}
    </tr></table>
    """

    # --- Open positions ---
    if positions:
        html += '<h3>Open Positions</h3>'
        html += '<table style="border-collapse: collapse; width: 100%;">'
        html += '<tr style="border-bottom: 2px solid #ddd; font-size: 13px; color: #888;">'
        html += '<td style="padding: 6px 8px;">Symbol</td><td style="padding: 6px 8px;">Side</td>'
        html += '<td style="padding: 6px 8px;">Entry</td><td style="padding: 6px 8px;">Current</td>'
        html += '<td style="padding: 6px 8px;">P&L</td></tr>'
        for p in positions:
            pl = p.get("unrealized_pl", 0)
            pl_pct = p.get("unrealized_plpc", 0)
            pl_c = "#2e7d32" if pl >= 0 else "#c62828"
            html += f"""<tr style="border-bottom: 1px solid #eee;">
                <td style="padding: 6px 8px; font-weight: bold;">{p['symbol']}</td>
                <td style="padding: 6px 8px;">{p.get('side', '').upper()}</td>
                <td style="padding: 6px 8px;">${p.get('entry_price', 0):.2f}</td>
                <td style="padding: 6px 8px;">${p.get('current_price', 0):.2f}</td>
                <td style="padding: 6px 8px; color: {pl_c};">{pl:+.0f} ({pl_pct:+.1f}%)</td>
            </tr>"""
        html += '</table>'

    # --- Live Signal Tests (the core view) ---
    patterns = load_patterns()
    from self_review import load_methodology
    method = load_methodology()
    promo_criteria = method.get("promotion_criteria", {})
    min_tests = promo_criteria.get("min_live_tests", 3)
    retire_tests = promo_criteria.get("retirement_min_tests", 5)

    if patterns:
        html += '<h3>Live Signal Tests</h3>'
        html += '<p style="color: #888; font-size: 13px; margin-bottom: 12px;">Each signal needs repeated independent experiments. A single result proves nothing.</p>'
        for event_type, pat in sorted(patterns.items(), key=lambda x: x[1].get("total_tests", 0), reverse=True):
            raw_total = pat.get("total_tests", 0)
            eff_n = pat.get("effective_independent_n", raw_total)
            eff_correct = pat.get("effective_correct_n", pat.get("direction_correct_count", 0))
            state = pat.get("state", "EXPLORING")
            title = event_type.replace("_", " ").title()

            # Count in-flight experiments
            in_flight = sum(1 for h in active + pending if h.get("event_type") == event_type)

            # State colors and labels
            state_styles = {
                "EXPLORING": ("#1565c0", "Exploring"),
                "PROMISING": ("#2e7d32", "Promising"),
                "FAILING": ("#f9a825", "Failing"),
                "VALIDATED": ("#1b5e20", "Validated"),
                "RETIRED": ("#888", "Retired"),
            }
            badge_color, badge_label = state_styles.get(state, ("#888", state))

            # Experiment dots
            exps = pat.get("experiments", [])
            exp_dots = ""
            for e in exps[-10:]:
                dot_c = "#2e7d32" if e.get("direction_correct") else "#c62828"
                sym = e.get("symbol", "?")
                pct = e.get("actual_pct", 0)
                exp_dots += f'<span title="{sym}: {pct:+.1f}%" style="display: inline-block; width: 12px; height: 12px; border-radius: 50%; background: {dot_c}; margin: 0 2px;"></span>'

            html += f"""
            <div style="border: 1px solid #ddd; border-radius: 8px; padding: 12px 16px; margin: 10px 0;">
                <div style="display: flex; justify-content: space-between; align-items: center;">
                    <span style="font-weight: bold;">{html_mod.escape(title)}</span>
                    <span style="background: {badge_color}; color: white; padding: 2px 10px; border-radius: 12px; font-size: 12px;">{badge_label}</span>
                </div>
                <div style="margin-top: 6px;">{exp_dots}</div>
                <div style="margin-top: 6px; font-size: 13px; color: #555;">{eff_correct}/{eff_n} independent tests correct"""
            if eff_n < raw_total:
                html += f' <span style="color: #888;">({raw_total} experiments, {raw_total - eff_n} overlapped)</span>'
            html += '</div>'

            # What does this signal need next?
            if state == "EXPLORING":
                needed = max(min_tests - eff_n, 1)
                html += f'<div style="font-size: 12px; color: #1565c0; margin-top: 4px;">Needs {needed} more independent test{"s" if needed != 1 else ""} before any judgment</div>'
            elif state == "PROMISING":
                needed = max(retire_tests - eff_n, 1)
                html += f'<div style="font-size: 12px; color: #2e7d32; margin-top: 4px;">Needs {needed} more to reach validation threshold</div>'
            elif state == "FAILING":
                needed = max(retire_tests - eff_n, 1)
                html += f'<div style="font-size: 12px; color: #f9a825; margin-top: 4px;">Needs {needed} more to confirm retirement</div>'
            elif state == "VALIDATED":
                # Show revalidation timeline
                last_val = pat.get("last_updated", "")
                if last_val:
                    try:
                        months_ago = (datetime.now() - datetime.fromisoformat(last_val[:19])).days / 30
                        if months_ago < 3:
                            health_c, health_t = "#2e7d32", f"Confirmed {months_ago:.0f}mo ago"
                        elif months_ago < 6:
                            health_c, health_t = "#f9a825", f"Revalidation due in {6 - months_ago:.0f}mo"
                        else:
                            health_c, health_t = "#c62828", "Revalidation overdue"
                        html += f'<div style="font-size: 12px; color: {health_c}; margin-top: 4px;">{health_t}</div>'
                    except (ValueError, TypeError):
                        pass

            if in_flight:
                html += f'<div style="font-size: 12px; color: #1565c0; margin-top: 4px;">{in_flight} experiment{"s" if in_flight != 1 else ""} in pipeline</div>'
            html += '</div>'

    # --- Active experiments ---
    if active:
        html += '<h3>Live Experiments</h3>'
        for h in active:
            html += _build_compact_hypothesis(h, html_mod)

    # --- Recent results ---
    if completed:
        recent = sorted(completed, key=lambda h: (h.get("result") or {}).get("exit_time", ""), reverse=True)[:5]
        html += '<h3>Latest Results</h3>'
        for h in recent:
            html += _build_compact_hypothesis(h, html_mod)

    # --- Pipeline ---
    if pending:
        html += f'<h3>Pipeline ({len(pending)} queued)</h3>'
        html += '<table style="border-collapse: collapse; width: 100%;">'
        for h in pending[:10]:
            symbol = h.get("expected_symbol", "TBD")
            direction = h.get("expected_direction", "?").upper()
            event = h.get("event_type", "").replace("_", " ").title()
            conf = h.get("confidence", "?")
            trigger = h.get("trigger", "")
            trigger_note = f' — triggers {trigger}' if trigger else ""
            html += f"""<tr style="border-bottom: 1px solid #eee;">
                <td style="padding: 4px 8px; font-weight: bold;">{html_mod.escape(symbol)}</td>
                <td style="padding: 4px 8px;">{direction}</td>
                <td style="padding: 4px 8px;">{html_mod.escape(event)}</td>
                <td style="padding: 4px 8px; color: #888;">conf {conf}/10{html_mod.escape(trigger_note)}</td>
            </tr>"""
        if len(pending) > 10:
            html += f'<tr><td colspan="4" style="padding: 4px 8px; color: #888;">...and {len(pending) - 10} more</td></tr>'
        html += '</table>'

    # --- Scoreboard ---
    from config import MAX_ACTIVE_SIGNAL_TYPES
    active_types = set()
    for h in active + pending:
        active_types.add(h.get("event_type"))
    focus_label = f"{len(active_types)}/{MAX_ACTIVE_SIGNAL_TYPES}"
    focus_color = "#2e7d32" if len(active_types) <= MAX_ACTIVE_SIGNAL_TYPES else "#c62828"

    html += f"""
    <h3>Scoreboard</h3>
    <table style="border-collapse: collapse;">
        <tr><td style="padding: 4px 12px;">Signals under live test</td><td style="padding: 4px 12px;"><b>{len(patterns)}</b></td></tr>
        <tr><td style="padding: 4px 12px;">Research findings (backtest)</td><td style="padding: 4px 12px;">{known_count}</td></tr>
        <tr><td style="padding: 4px 12px;">Dead ends</td><td style="padding: 4px 12px;">{dead_count}</td></tr>
        <tr><td style="padding: 4px 12px;">Total experiments</td><td style="padding: 4px 12px;">{research['total_hypotheses']}</td></tr>
        <tr><td style="padding: 4px 12px;">Focus</td><td style="padding: 4px 12px; color: {focus_color};"><b>{focus_label} signal types active</b></td></tr>
    </table>
    """

    # --- Watchlist ---
    rq = _db.load_queue()
    watchlist = rq.get("event_watchlist", [])
    if watchlist:
        html += '<h3>Watching For</h3><ul>'
        for w in watchlist[:10]:
            html += f'<li><b>{html_mod.escape(str(w.get("event", "?")))}</b> — {html_mod.escape(str(w.get("expected_date", "?")))}</li>'
        html += '</ul>'

    # Token usage
    html += build_token_usage_section()

    html += _PAGE_CLOSE
    return html


def _build_compact_hypothesis(h, html_mod):
    """Render a single hypothesis as a compact card."""
    direction = h.get("expected_direction", "?")
    mag = h.get("expected_magnitude_pct", 0)
    timeframe = h.get("expected_timeframe_days", "?")
    symbol = h.get("expected_symbol", "TBD")
    status = h.get("status", "?")
    confidence = h.get("confidence", "?")
    color = _STATUS_COLORS.get(status, "#333")

    # Sample size — just the count, never raw data
    n = h.get("sample_size") or h.get("backtest_events")
    if isinstance(n, list):
        n = len(n)

    desc = (h.get("event_description") or "")[:150]
    mechanism = (h.get("causal_mechanism") or "")[:150]

    card = f"""
    <div style="border: 1px solid #ddd; border-radius: 8px; padding: 12px 16px; margin: 12px 0;">
        <div style="display: flex; justify-content: space-between; align-items: center;">
            <span style="font-size: 16px; font-weight: bold;">{html_mod.escape(symbol)} — {html_mod.escape(h.get('event_type', '').replace('_', ' ').title())}</span>
            <span style="background: {color}; color: white; padding: 2px 10px; border-radius: 12px; font-size: 12px;">{status}</span>
        </div>
        <div style="margin-top: 6px; color: #555; font-size: 14px;">{html_mod.escape(desc)}</div>
        <div style="margin-top: 6px; font-size: 13px;">
            {direction.upper()} <b>{mag:+.1f}%</b> over {timeframe}d
            &middot; confidence {confidence}/10
            &middot; {n if n else '?'} events tested
        </div>
    """

    # Trade info
    trade = h.get("trade")
    result = h.get("result")
    if trade and not result:
        card += f"""
        <div style="margin-top: 6px; background: #fff3e0; padding: 8px 12px; border-radius: 4px; font-size: 13px;">
            Trading {symbol} @ ${trade.get('entry_price', '?')}
            &middot; ${trade.get('position_size_usd', trade.get('position_size', '?'))} position
            &middot; deadline {str(trade.get('deadline', '?'))[:10]}
        </div>
        """
    elif result:
        ret = result.get("abnormal_return_pct", result.get("raw_return_pct", 0))
        correct = result.get("direction_correct", False)
        ret_color = "#2e7d32" if (ret or 0) > 0 else "#c62828"
        # Frame as one data point, not a verdict
        event_type = h.get("event_type", "")
        pattern_context = ""
        try:
            patterns = load_patterns()
            pat = patterns.get(event_type)
            if pat and pat.get("total_tests", 0) > 0:
                total = pat["total_tests"]
                cor = pat.get("direction_correct_count", 0)
                pattern_context = f" &middot; signal overall: {cor}/{total}"
        except Exception:
            pass
        card += f"""
        <div style="margin-top: 6px; background: #f5f5f5; padding: 8px 12px; border-radius: 4px; font-size: 13px;">
            This experiment: <span style="color: {ret_color}; font-weight: bold;">{ret:+.1f}%</span> abnormal return{pattern_context}
        </div>
        """

    card += "</div>"
    return card


def get_daily_token_usage(date_str=None):
    """Sum token usage for all sessions on a given date (default: today)."""
    import db as _db
    return _db.get_daily_token_usage(date_str)


def estimate_cost(usage):
    """Estimate API cost from token usage (Opus 4.6 pricing)."""
    # Opus 4.6: $5/MTok input, $25/MTok output, $0.50/MTok cache read, $6.25/MTok cache write
    cost = (
        usage.get("input_tokens", 0) / 1_000_000 * 5.00
        + usage.get("output_tokens", 0) / 1_000_000 * 25.00
        + usage.get("cache_read_tokens", 0) / 1_000_000 * 0.50
        + usage.get("cache_creation_tokens", 0) / 1_000_000 * 6.25
    )
    return round(cost, 2)


def build_token_usage_section():
    """Build HTML section showing today's token usage and cost."""
    usage = get_daily_token_usage()
    if usage["sessions"] == 0:
        return ""

    cost = estimate_cost(usage)

    def fmt_k(n):
        return f"{n / 1000:,.1f}k" if n >= 1000 else str(n)

    return f"""
    <h3>Token Usage</h3>
    <table style="border-collapse: collapse;">
        <tr><td style="padding: 4px 12px;">Sessions</td><td style="padding: 4px 12px;"><b>{usage['sessions']}</b></td></tr>
        <tr><td style="padding: 4px 12px;">API calls</td><td style="padding: 4px 12px;">{usage['api_calls']}</td></tr>
        <tr><td style="padding: 4px 12px;">Input tokens</td><td style="padding: 4px 12px;">{fmt_k(usage['input_tokens'])}</td></tr>
        <tr><td style="padding: 4px 12px;">Output tokens</td><td style="padding: 4px 12px;">{fmt_k(usage['output_tokens'])}</td></tr>
        <tr><td style="padding: 4px 12px;">Cache read</td><td style="padding: 4px 12px;">{fmt_k(usage['cache_read_tokens'])}</td></tr>
        <tr><td style="padding: 4px 12px;">Cache write</td><td style="padding: 4px 12px;">{fmt_k(usage['cache_creation_tokens'])}</td></tr>
        <tr><td style="padding: 4px 12px;">Total</td><td style="padding: 4px 12px;"><b>{fmt_k(usage['total_tokens'])}</b></td></tr>
        <tr><td style="padding: 4px 12px;">Estimated cost</td><td style="padding: 4px 12px;"><b>${cost:.2f}</b></td></tr>
    </table>
    """


def send_email(subject, body):
    """Send a simple HTML email. Used by shell scripts and internal notifications."""
    from config import require_gmail
    require_gmail()
    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"] = GMAIL_ADDRESS
    msg["To"] = REPORT_RECIPIENT

    msg.attach(MIMEText(body, "html"))

    with smtplib.SMTP_SSL("smtp.gmail.com", 465) as server:
        server.login(GMAIL_ADDRESS, GMAIL_APP_PASSWORD)
        server.sendmail(GMAIL_ADDRESS, REPORT_RECIPIENT, msg.as_string())

    print(f"Email sent to {REPORT_RECIPIENT}: {subject}")


def send_report(subject=None, body_html=None):
    """Send the daily report email."""
    if body_html is None:
        body_html = build_daily_report()
    if subject is None:
        subject = f"Research Report — {datetime.now().strftime('%Y-%m-%d')}"

    send_email(subject, body_html)


def parse_token_usage(log_file):
    """Parse token usage from a stream-json log file."""
    import json

    totals = {
        "input_tokens": 0,
        "output_tokens": 0,
        "cache_read_tokens": 0,
        "cache_creation_tokens": 0,
    }
    message_count = 0

    try:
        with open(log_file) as f:
            for line in f:
                line = line.strip()
                if not line or not line.startswith("{"):
                    continue
                try:
                    obj = json.loads(line)
                except json.JSONDecodeError:
                    continue
                usage = None
                if obj.get("type") == "assistant" and "message" in obj:
                    usage = obj["message"].get("usage")
                elif obj.get("type") == "message" and "usage" in obj:
                    usage = obj["usage"]
                if usage:
                    message_count += 1
                    totals["input_tokens"] += usage.get("input_tokens", 0)
                    totals["output_tokens"] += usage.get("output_tokens", 0)
                    totals["cache_read_tokens"] += usage.get("cache_read_input_tokens", 0)
                    totals["cache_creation_tokens"] += usage.get("cache_creation_input_tokens", 0)
    except Exception:
        pass

    totals["total_tokens"] = totals["input_tokens"] + totals["output_tokens"] + totals["cache_read_tokens"] + totals["cache_creation_tokens"]
    totals["api_calls"] = message_count
    return totals


def parse_session_narrative(log_file):
    """Extract the researcher's own commentary from a stream-json log."""
    import json

    texts = []
    try:
        with open(log_file) as f:
            for line in f:
                line = line.strip()
                if not line or not line.startswith("{"):
                    continue
                try:
                    obj = json.loads(line)
                except json.JSONDecodeError:
                    continue
                if obj.get("type") == "assistant":
                    for c in obj.get("message", {}).get("content", []):
                        if c.get("type") == "text":
                            text = c["text"].strip()
                            if text and len(text) > 40:
                                texts.append(text)
    except Exception:
        pass
    return texts


def get_latest_journal_entry():
    """Get the most recent research journal entry."""
    import db as _db
    entries = _db.get_recent_journal(1)
    return entries[0] if entries else None


def _to_str(val):
    """Safely convert any value to a string for HTML rendering."""
    if val is None:
        return ""
    if isinstance(val, list):
        return "; ".join(str(v) for v in val)
    if isinstance(val, dict):
        return "; ".join(f"{k}: {v}" for k, v in val.items())
    return str(val)


def build_hypothesis_story(h):
    """Tell the story of a hypothesis in plain language."""
    import html as html_mod

    direction = h.get("expected_direction", "?")
    mag = h.get("expected_magnitude_pct", 0)
    timeframe = h.get("expected_timeframe_days", "?")
    symbol = h.get("expected_symbol", "TBD")
    status = h.get("status", "?")
    confidence = h.get("confidence", "?")
    n = h.get("sample_size") or h.get("backtest_events", "?")
    if isinstance(n, list):
        n = len(n)

    # Status styling
    color = _STATUS_COLORS.get(status, "#333")

    # The idea (what and why)
    desc = _to_str(h.get("event_description", ""))
    mechanism = _to_str(h.get("causal_mechanism", ""))

    # What the evidence showed
    oos = h.get("out_of_sample_split", {})
    oos_verdict = oos.get("verdict", "")

    html = f"""
    <div style="border: 1px solid #ddd; border-radius: 8px; padding: 16px; margin: 16px 0;">
        <div style="display: flex; justify-content: space-between; align-items: center;">
            <span style="font-size: 16px; font-weight: bold;">{html_mod.escape(h.get('event_type', '').replace('_', ' ').title())}</span>
            <span style="background: {color}; color: white; padding: 2px 10px; border-radius: 12px; font-size: 12px;">{status}</span>
        </div>

        <div style="margin-top: 10px;">
            <b>The idea:</b> {html_mod.escape(desc[:200])}
        </div>

        <div style="margin-top: 8px;">
            <b>Why it should work:</b> {html_mod.escape(mechanism[:200])}
        </div>

        <div style="margin-top: 8px;">
            <b>Expected:</b> {direction.upper()} for <b>+{mag}%</b> over {timeframe} days
            (confidence: {confidence}/10, tested on {n} historical events)
        </div>
    """

    # Out-of-sample results
    if oos and oos.get("discovery_avg_abnormal_1d") is not None:
        disc_ret = oos.get("discovery_avg_abnormal_1d", 0)
        disc_n = oos.get("discovery_n", "?")
        disc_pos = oos.get("discovery_positive_rate_1d", 0)
        val_ret = oos.get("validation_avg_abnormal_1d", 0)
        val_n = oos.get("validation_n", "?")
        val_pos = oos.get("validation_positive_rate_1d", 0)

        verdict_color = "#2e7d32" if "PASS" in str(oos_verdict).upper() else "#c62828"
        html += f"""
        <div style="margin-top: 10px; background: #fafafa; padding: 10px; border-radius: 4px;">
            <b>Test results:</b><br>
            Discovery ({disc_n} events): <b>{disc_ret:+.1f}%</b> day-1 return, {disc_pos:.0%} positive<br>
            Out-of-sample ({val_n} events): <b>{val_ret:+.1f}%</b> day-1 return, {val_pos:.0%} positive<br>
            <span style="color: {verdict_color}; font-weight: bold;">{html_mod.escape(str(oos_verdict))}</span>
        </div>
        """

    # Live validation
    live = h.get("live_validation_march_2026")
    if live and isinstance(live, dict):
        additions = live.get("additions", [])
        avg_1d = live.get("avg_abnormal_1d", 0)
        html += '<div style="margin-top: 8px; background: #e8f5e9; padding: 10px; border-radius: 4px;">'
        html += f'<b>Live validation ({live.get("announcement_date", "")}):</b> '
        if additions:
            parts = [f'{a["symbol"]} {a.get("abnormal_1d", 0):+.1f}%' for a in additions[:6]]
            html += ", ".join(parts)
        html += f'<br>Average: <b>{avg_1d:+.1f}%</b> day-1 abnormal return'
        html += '</div>'

    # Trade status
    trade = h.get("trade")
    result = h.get("result")
    if trade and not result:
        html += f"""
        <div style="margin-top: 8px; background: #fff3e0; padding: 10px; border-radius: 4px;">
            <b>Active trade:</b> {html_mod.escape(str(symbol))} @ ${trade.get('entry_price', '?')}
            &middot; Size: ${trade.get('position_size_usd', trade.get('position_size', '?'))}
            &middot; Deadline: {str(trade.get('deadline', '?'))[:10]}
        </div>
        """
    elif result:
        ret = result.get("abnormal_return_pct", result.get("raw_return_pct", 0))
        ret_color = "#2e7d32" if ret > 0 else "#c62828"
        event_type = h.get("event_type", "")
        pattern_note = ""
        try:
            pat = load_patterns().get(event_type)
            if pat and pat.get("total_tests", 0) > 0:
                pattern_note = f" &middot; signal overall: {pat['direction_correct_count']}/{pat['total_tests']}"
        except Exception:
            pass
        html += f"""
        <div style="margin-top: 8px; background: #f5f5f5; padding: 10px; border-radius: 4px;">
            This experiment: <span style="color: {ret_color};">{ret:+.1f}% abnormal return</span>{pattern_note}
        </div>
        """

    html += "</div>"
    return html


def build_findings_section(knowledge):
    """Build a readable summary of all validated signals and dead ends."""
    import html as html_mod

    html = ""

    # Validated signals
    known = knowledge.get("known_effects", {})
    if known:
        html += '<h3 style="color: #2e7d32;">Validated Signals</h3>'
        for name, effect in known.items():
            # Skip deprecated entries
            if "DEPRECATED" in name.upper() or effect.get("note", "").startswith("DEPRECATED"):
                continue

            title = name.replace("_", " ").title()
            status = _to_str(effect.get("status", effect.get("verdict", "")))

            # Build description from whatever fields exist
            desc = _to_str(effect.get("description", effect.get("effect", "")))
            if not desc:
                # Fallback: use first string value that looks like a description
                for v in effect.values():
                    if isinstance(v, str) and len(v) > 30:
                        desc = v
                        break

            # Pick border color based on status
            if any(s in status.lower() for s in ["strong", "confirmed", "pass"]):
                border_color = "#2e7d32"
                bg_color = "#f0f8f0"
            elif any(s in status.lower() for s in ["pending", "partial", "promising"]):
                border_color = "#f9a825"
                bg_color = "#fffde7"
            else:
                border_color = "#1565c0"
                bg_color = "#e3f2fd"

            html += f"""
            <div style="background: {bg_color}; border-left: 4px solid {border_color}; padding: 12px 16px; margin: 12px 0;">
                <div style="font-size: 16px; font-weight: bold;">{html_mod.escape(title)}</div>
                <div style="margin-top: 6px;">{html_mod.escape(desc[:300])}</div>
            """

            # Show stats line if structured fields exist
            stats_parts = []
            mag = effect.get("avg_magnitude_pct") or effect.get("avg_abnormal_3d") or effect.get("avg_1d_abnormal")
            if mag is not None:
                stats_parts.append(f"<b>{mag:+.1f}%</b> avg abnormal" if isinstance(mag, (int, float)) else f"<b>{mag}</b>")
            timeframe = effect.get("timeframe_days")
            if timeframe:
                stats_parts.append(f"over <b>{timeframe}d</b>")
            n = effect.get("sample_size") or effect.get("n_events")
            if n:
                stats_parts.append(f"n={n}")
            rate = effect.get("reliability") or effect.get("positive_rate") or effect.get("positive_rate_1d")
            if rate is not None:
                try:
                    rate_f = float(rate)
                    pct = rate_f * 100 if rate_f <= 1 else rate_f
                    stats_parts.append(f"{pct:.0f}% positive")
                except (ValueError, TypeError):
                    stats_parts.append(f"{rate}% positive")
            if status:
                stats_parts.append(status)

            if stats_parts:
                html += f'<div style="margin-top: 8px; color: #555;">{" &middot; ".join(stats_parts)}</div>'

            # Discovery / OOS details
            if effect.get("magnitude_discovery") and effect.get("magnitude_oos"):
                html += f"""
                <div style="margin-top: 6px; font-size: 13px; color: #666;">
                    Discovery: {html_mod.escape(_to_str(effect['magnitude_discovery']))}
                    <br>Out-of-sample: {html_mod.escape(_to_str(effect['magnitude_oos']))}
                </div>
                """
            elif effect.get("discovery_2021") or effect.get("oos_2022"):
                parts = []
                if effect.get("discovery_2021"):
                    parts.append(f"Discovery: {html_mod.escape(_to_str(effect['discovery_2021']))}")
                if effect.get("oos_2022"):
                    parts.append(f"OOS 2022: {html_mod.escape(_to_str(effect['oos_2022']))}")
                if effect.get("oos_2023_2024"):
                    parts.append(f"OOS 2023-24: {html_mod.escape(_to_str(effect['oos_2023_2024']))}")
                html += f'<div style="margin-top: 6px; font-size: 13px; color: #666;">{"<br>".join(parts)}</div>'

            if effect.get("regime_dependence"):
                html += f'<div style="margin-top: 4px; font-size: 13px; color: #996600;">Regime: {html_mod.escape(_to_str(effect["regime_dependence"]))}</div>'
            if effect.get("blocking_issue"):
                html += f'<div style="margin-top: 4px; font-size: 13px; color: #c62828;">Blocking: {html_mod.escape(_to_str(effect["blocking_issue"]))}</div>'

            html += "</div>"

    # Dead ends
    dead = knowledge.get("dead_ends", [])
    if dead:
        html += f'<h3 style="color: #888;">Dead Ends ({len(dead)} ideas tested, didn\'t work)</h3><ul style="color: #666;">'
        for d in dead:
            name = d.get("event_type", "").replace("_", " ").title()
            # Extract just the first sentence of the reason
            reason = _to_str(d.get("reason", ""))
            first_sentence = reason.split(".")[0] + "." if "." in reason else reason[:120]
            html += f"<li><b>{html_mod.escape(name)}</b> — {html_mod.escape(first_sentence)}</li>"
        html += "</ul>"

    return html


def build_literature_section(knowledge):
    """Build a readable summary of what's been studied."""
    import html as html_mod

    lit = knowledge.get("literature", {})
    if not lit:
        return ""

    html = '<h3>Research Areas</h3>'
    for topic, data in lit.items():
        title = topic.replace("_", " ").title()
        key = _to_str(data.get("key_finding", data.get("summary", "")))[:200]
        html += f'<div style="margin: 8px 0;"><b>{html_mod.escape(title)}</b> — {html_mod.escape(key)}</div>'

    return html


def send_session_report(session_type, status, log_file, validation_warnings=""):
    """Send a post-session summary email. Called by run.sh after every session."""
    import json
    import os
    import html as html_mod

    # Parse token usage and narrative from log
    token_usage = parse_token_usage(log_file) if log_file else {}
    narrative = parse_session_narrative(log_file) if log_file else []

    # Get the latest journal entry (what the researcher wrote about this session)
    journal = get_latest_journal_entry() or {}

    # Load current state
    research = get_research_summary()
    knowledge = load_knowledge()

    # Read research_queue for priorities and handoff
    import db as _db
    rq = _db.load_queue()

    handoff = rq.get("session_handoff", {})
    next_priorities = rq.get("next_session_priorities", [])
    queue_pending = len([t for t in rq.get("queue", []) if t.get("status") == "pending"])
    watchlist_count = len(rq.get("event_watchlist", []))

    status_color = {"completed": "#2e7d32", "timed_out": "#e65100", "crashed": "#c62828"}.get(status, "#333")
    status_label = {"completed": "Completed", "timed_out": "Timed Out", "crashed": "Crashed"}.get(status, status.upper())

    subject = f"Research session — {datetime.now().strftime('%Y-%m-%d %H:%M')}"
    if status != "completed":
        subject = f"[{status_label.upper()}] {subject}"

    # --- Build the email ---
    html = f"""
    <html><body style="font-family: -apple-system, Arial, sans-serif; max-width: 700px; margin: 0 auto; color: #333;">
    <h2>Research Session Report</h2>
    <p style="color: #888;">{datetime.now().strftime('%A, %B %d %Y at %H:%M')} &middot; <span style="color: {status_color};">{status_label}</span></p>
    """

    if validation_warnings:
        html += f'<p style="color: #e65100; background: #fff3e0; padding: 8px 12px; border-radius: 4px;"><b>Warning:</b> {html_mod.escape(validation_warnings)}</p>'

    # --- What was investigated (from journal) ---
    if journal.get("investigated"):
        html += f"""
        <h3>What was investigated</h3>
        <p>{html_mod.escape(_to_str(journal['investigated']))}</p>
        """

    # --- Key findings (from journal) ---
    if journal.get("findings"):
        html += '<h3>Findings</h3>'
        findings = _to_str(journal["findings"])
        for paragraph in findings.split("\n\n"):
            paragraph = paragraph.strip()
            if not paragraph:
                continue
            html += f'<p style="margin: 8px 0;">{html_mod.escape(paragraph)}</p>'

    # --- Surprises (from journal) ---
    if journal.get("surprised_by"):
        html += f"""
        <h3>Surprises</h3>
        <div style="background: #fff8e1; border-left: 4px solid #ffa000; padding: 12px 16px; margin: 12px 0;">
            {html_mod.escape(_to_str(journal['surprised_by']))}
        </div>
        """

    # --- Researcher's commentary (from log, most insightful excerpts) ---
    if narrative:
        # Pick the longest/most substantive messages (likely the analysis summaries)
        best = sorted(narrative, key=len, reverse=True)[:3]
        html += '<h3>Researcher notes</h3>'
        for note in best:
            # Trim to ~500 chars for readability
            trimmed = note[:500] + ("..." if len(note) > 500 else "")
            html += f'<div style="background: #f5f5f5; padding: 10px 14px; margin: 8px 0; border-radius: 4px; font-size: 14px;">{html_mod.escape(trimmed)}</div>'

    # --- Hypotheses (the full story) ---
    all_hypotheses = get_active_hypotheses() + get_pending_hypotheses() + get_completed_hypotheses()
    if all_hypotheses:
        html += '<h3>Hypotheses</h3>'
        for h in all_hypotheses:
            html += build_hypothesis_story(h)

    # --- Validated signals & dead ends ---
    html += build_findings_section(knowledge)

    # --- What's next ---
    if handoff.get("next_step") or next_priorities:
        html += '<h3>Next up</h3>'
        if handoff.get("next_step"):
            html += f'<p>{html_mod.escape(_to_str(handoff["next_step"]))}</p>'
        if next_priorities:
            html += "<ol>"
            for p in next_priorities[:5]:
                task_text = p.get("task", p) if isinstance(p, dict) else p
                html += f"<li>{html_mod.escape(str(task_text))}</li>"
            html += "</ol>"

    if handoff.get("blockers"):
        html += f'<p style="color: #c62828;">Blocked: {html_mod.escape(_to_str(handoff["blockers"]))}</p>'

    # --- Scoreboard (compact) ---
    known_count = len(knowledge.get("known_effects", {}))
    dead_count = len(knowledge.get("dead_ends", []))
    html += f"""
    <h3>Scoreboard</h3>
    <table style="border-collapse: collapse;">
        <tr><td style="padding: 4px 12px;">Validated signals</td><td style="padding: 4px 12px;"><b>{known_count}</b></td></tr>
        <tr><td style="padding: 4px 12px;">Dead ends</td><td style="padding: 4px 12px;">{dead_count}</td></tr>
        <tr><td style="padding: 4px 12px;">Hypotheses</td><td style="padding: 4px 12px;">{research['total_hypotheses']} ({research['active']} active)</td></tr>
        <tr><td style="padding: 4px 12px;">Queue</td><td style="padding: 4px 12px;">{queue_pending} pending</td></tr>
        <tr><td style="padding: 4px 12px;">Watchlist</td><td style="padding: 4px 12px;">{watchlist_count} events</td></tr>
    </table>
    """

    # --- Token usage (compact) ---
    if token_usage and token_usage.get("total_tokens", 0) > 0:
        def fmt_k(n):
            return f"{n/1000:,.1f}k" if n >= 1000 else str(n)
        html += f"""
    <p style="color: #888; font-size: 12px; margin-top: 16px;">
        Tokens: {fmt_k(token_usage['total_tokens'])} total
        ({fmt_k(token_usage['input_tokens'])} in,
        {fmt_k(token_usage['output_tokens'])} out,
        {fmt_k(token_usage['cache_read_tokens'])} cached)
        &middot; {token_usage['api_calls']} API calls
    </p>
    """

    html += _PAGE_CLOSE
    send_email(subject, html)


if __name__ == "__main__":
    import sys
    if len(sys.argv) >= 4 and sys.argv[1] == "--session":
        # Called as: python email_report.py --session <type> <status> <log_file> [warnings]
        session_type = sys.argv[2]
        status = sys.argv[3]
        log_file = sys.argv[4] if len(sys.argv) > 4 else ""
        warnings = sys.argv[5] if len(sys.argv) > 5 else ""
        send_session_report(session_type, status, log_file, warnings)
    else:
        send_report()
