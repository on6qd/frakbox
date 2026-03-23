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
    """Build the daily research report as HTML."""
    summary = get_account_summary()
    research = get_research_summary()
    active = get_active_hypotheses()
    pending = get_pending_hypotheses()
    completed = get_completed_hypotheses()
    knowledge = load_knowledge()

    known_count = len(knowledge.get("known_effects", {}))
    dead_count = len(knowledge.get("dead_ends", []))

    html = _PAGE_OPEN + f"""
    <h2>Daily Research Report</h2>
    <p style="color: #888;">{datetime.now().strftime('%A, %B %d %Y')}</p>
    <table style="border-collapse: collapse; margin: 16px 0;"><tr>
    {_STAT_CELL.format(value=f"${summary['equity']:,.0f}", label="Equity")}
    {_STAT_CELL.format(value=known_count, label="Signals found")}
    {_STAT_CELL.format(value=dead_count, label="Dead ends")}
    {_STAT_CELL.format(value=research['total_hypotheses'], label="Hypotheses")}
    </tr></table>
    """

    # All hypotheses as stories
    all_h = active + pending + completed
    if all_h:
        html += '<h3>Hypotheses</h3>'
        for h in all_h:
            html += build_hypothesis_story(h)

    # Validated signals & dead ends
    html += build_findings_section(knowledge)

    # Research areas studied
    html += build_literature_section(knowledge)

    # Watchlist
    import db as _db
    rq = _db.load_queue()
    watchlist = rq.get("event_watchlist", [])
    if watchlist:
        html += '<h3>Watching for</h3><ul>'
        for w in watchlist:
            html += f'<li><b>{w.get("event", "?")}</b> — expected {w.get("expected_date", "?")}</li>'
        html += '</ul>'

    html += _PAGE_CLOSE
    return html


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
    import json
    import os

    journal_path = os.path.join(os.path.dirname(__file__), "logs", "research_journal.jsonl")
    last = None
    try:
        with open(journal_path) as f:
            for line in f:
                line = line.strip()
                if line:
                    last = json.loads(line)
    except Exception:
        pass
    return last


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
    n = h.get("backtest_events", h.get("sample_size", "?"))

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
        correct = result.get("direction_correct", False)
        emoji = "Correct" if correct else "Wrong"
        ret_color = "#2e7d32" if ret > 0 else "#c62828"
        html += f"""
        <div style="margin-top: 8px; background: {'#e8f5e9' if correct else '#ffebee'}; padding: 10px; border-radius: 4px;">
            <b>Result:</b> <span style="color: {ret_color};">{ret:+.1f}% abnormal return</span> — {emoji}
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
                pct = rate * 100 if isinstance(rate, float) and rate <= 1 else rate
                stats_parts.append(f"{pct:.0f}% positive")
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
