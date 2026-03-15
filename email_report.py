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


def build_daily_report():
    """Build the daily research report as HTML."""
    summary = get_account_summary()
    research = get_research_summary()
    active = get_active_hypotheses()
    pending = get_pending_hypotheses()
    completed = get_completed_hypotheses()
    patterns = load_patterns()
    knowledge = load_knowledge()

    recent_completed = sorted(completed, key=lambda h: h.get("result", {}).get("exit_time", ""), reverse=True)[:5]

    html = f"""
    <html><body style="font-family: Arial, sans-serif; max-width: 700px; margin: 0 auto;">
    <h2>Daily Research Report — {datetime.now().strftime('%Y-%m-%d')}</h2>

    <h3>Account</h3>
    <table style="border-collapse: collapse;">
        <tr><td style="padding: 4px 12px;">Equity</td><td style="padding: 4px 12px;"><b>${summary['equity']:,.0f}</b></td></tr>
        <tr><td style="padding: 4px 12px;">Cash</td><td style="padding: 4px 12px;">${summary['cash']:,.0f}</td></tr>
        <tr><td style="padding: 4px 12px;">Positions</td><td style="padding: 4px 12px;">{len(summary['positions'])}</td></tr>
    </table>

    <h3>Research Progress</h3>
    <table style="border-collapse: collapse;">
        <tr><td style="padding: 4px 12px;">Total hypotheses</td><td style="padding: 4px 12px;">{research['total_hypotheses']}</td></tr>
        <tr><td style="padding: 4px 12px;">Active experiments</td><td style="padding: 4px 12px;">{research['active']}</td></tr>
        <tr><td style="padding: 4px 12px;">Completed</td><td style="padding: 4px 12px;">{research['completed']}</td></tr>
        <tr><td style="padding: 4px 12px;">Direction accuracy</td><td style="padding: 4px 12px;"><b>{research['direction_accuracy']}</b></td></tr>
        <tr><td style="padding: 4px 12px;">Patterns discovered</td><td style="padding: 4px 12px;">{research['patterns_discovered']}</td></tr>
        <tr><td style="padding: 4px 12px;">Event types studied</td><td style="padding: 4px 12px;">{len(knowledge.get('literature', {}))}</td></tr>
        <tr><td style="padding: 4px 12px;">Dead ends recorded</td><td style="padding: 4px 12px;">{len(knowledge.get('dead_ends', []))}</td></tr>
    </table>
    """

    # Reliable patterns
    reliable = [p for p in patterns if p["total_tests"] >= 3]
    reliable.sort(key=lambda p: p["reliability_score"], reverse=True)
    if reliable:
        html += "<h3>Validated Patterns</h3><table style='border-collapse: collapse; width: 100%;'>"
        html += "<tr style='background: #f0f0f0;'><th style='padding: 6px; text-align: left;'>Event Type</th><th style='padding: 6px;'>Tests</th><th style='padding: 6px;'>Reliability</th><th style='padding: 6px;'>Avg Effect</th></tr>"
        for p in reliable:
            rel_pct = f"{p['reliability_score']*100:.0f}%"
            html += f"<tr><td style='padding: 6px;'>{p['event_type']}</td><td style='padding: 6px; text-align: center;'>{p['total_tests']}</td><td style='padding: 6px; text-align: center;'>{rel_pct}</td><td style='padding: 6px; text-align: center;'>{p['avg_actual_magnitude']:+.1f}%</td></tr>"
        html += "</table>"

    if research.get('by_event_type'):
        html += "<h4>Accuracy by Event Type</h4><ul>"
        for event_type, data in research['by_event_type'].items():
            html += f"<li><b>{event_type}</b>: {data['accuracy']} (avg confidence: {data['avg_confidence']})</li>"
        html += "</ul>"

    if active:
        html += "<h3>Active Experiments</h3><table style='border-collapse: collapse; width: 100%;'>"
        html += "<tr style='background: #f0f0f0;'><th style='padding: 6px; text-align: left;'>ID</th><th style='padding: 6px; text-align: left;'>Symbol</th><th style='padding: 6px; text-align: left;'>Direction</th><th style='padding: 6px; text-align: left;'>Event</th><th style='padding: 6px; text-align: left;'>P&L</th><th style='padding: 6px; text-align: left;'>Deadline</th></tr>"
        positions = {p['symbol']: p for p in summary['positions']}
        for h in active:
            pos = positions.get(h['expected_symbol'], {})
            pnl = f"{pos.get('unrealized_plpc', 0):+.1f}%" if pos else "n/a"
            deadline = h.get('trade', {}).get('deadline', 'n/a')[:10]
            html += f"<tr><td style='padding: 6px;'>{h['id']}</td><td style='padding: 6px;'>{h['expected_symbol']}</td><td style='padding: 6px;'>{h['expected_direction']}</td><td style='padding: 6px;'>{h['event_description'][:50]}</td><td style='padding: 6px;'>{pnl}</td><td style='padding: 6px;'>{deadline}</td></tr>"
        html += "</table>"

    if recent_completed:
        html += "<h3>Recent Results</h3><table style='border-collapse: collapse; width: 100%;'>"
        html += "<tr style='background: #f0f0f0;'><th style='padding: 6px; text-align: left;'>Symbol</th><th style='padding: 6px; text-align: left;'>Event</th><th style='padding: 6px; text-align: left;'>Expected</th><th style='padding: 6px; text-align: left;'>Actual</th><th style='padding: 6px; text-align: left;'>Correct?</th></tr>"
        for h in recent_completed:
            r = h.get("result", {})
            correct = "YES" if r.get("direction_correct") else "NO"
            html += f"<tr><td style='padding: 6px;'>{h['expected_symbol']}</td><td style='padding: 6px;'>{h['event_description'][:40]}</td><td style='padding: 6px;'>{h['expected_magnitude_pct']:+.1f}%</td><td style='padding: 6px;'>{r.get('actual_return_pct', 0):+.1f}%</td><td style='padding: 6px;'>{correct}</td></tr>"
        html += "</table>"

    if pending:
        html += f"<h3>Pending Hypotheses ({len(pending)})</h3><ul>"
        for h in pending:
            html += f"<li><b>{h['expected_symbol']}</b> ({h['expected_direction']}) — {h['event_description'][:60]} [confidence: {h['confidence']}/10]</li>"
        html += "</ul>"

    # Known effects summary
    known = knowledge.get("known_effects", {})
    if known:
        html += "<h3>Known Effects (Validated)</h3><ul>"
        for event_type, effect in known.items():
            html += f"<li><b>{event_type}</b>: {effect.get('description', '')} — status: {effect.get('status', 'unknown')}</li>"
        html += "</ul>"

    html += """
    <hr style="margin-top: 30px;">
    <p style="color: #888; font-size: 12px;">Automated research report — Stock Market Causal Research Project</p>
    </body></html>
    """
    return html


def send_report(subject=None, body_html=None):
    """Send the daily report email."""
    if body_html is None:
        body_html = build_daily_report()
    if subject is None:
        subject = f"Research Report — {datetime.now().strftime('%Y-%m-%d')}"

    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"] = GMAIL_ADDRESS
    msg["To"] = REPORT_RECIPIENT

    msg.attach(MIMEText(body_html, "html"))

    with smtplib.SMTP_SSL("smtp.gmail.com", 465) as server:
        server.login(GMAIL_ADDRESS, GMAIL_APP_PASSWORD)
        server.sendmail(GMAIL_ADDRESS, REPORT_RECIPIENT, msg.as_string())

    print(f"Report sent to {REPORT_RECIPIENT}")


if __name__ == "__main__":
    send_report()
