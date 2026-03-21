"""
Health monitor for the research daemon.
Runs independently (via launchd) to detect daemon failures and enforce risk controls.

- Checks if daemon is alive
- Checks if sessions are running on schedule
- Runs stop-loss checks as safety net
- Sends alert emails on problems
- Auto-restarts daemon if it dies with active positions
"""

import json
import os
import re
import subprocess
import sys
from datetime import datetime, timedelta
from pathlib import Path

BASE_DIR = Path(__file__).parent
DAEMON_LOG = BASE_DIR / "logs" / "daemon.log"
HEALTH_STATE = BASE_DIR / "logs" / "health_state.json"
VENV_PYTHON = BASE_DIR / "venv" / "bin" / "python3"
MAX_SILENCE_MINUTES = 120  # alert if no session in 2 hours

# Add project to path for imports
sys.path.insert(0, str(BASE_DIR))


def _load_state():
    try:
        with open(HEALTH_STATE) as f:
            return json.load(f)
    except Exception:
        return {}


def _save_state(state):
    state["last_check"] = datetime.now().isoformat()
    with open(HEALTH_STATE, "w") as f:
        json.dump(state, f, indent=2)


def _daemon_is_alive():
    """Check if run.sh is running."""
    try:
        result = subprocess.run(
            ["pgrep", "-f", "run.sh"],
            capture_output=True, text=True, timeout=5,
        )
        return result.returncode == 0
    except Exception:
        return False


def _last_session_time():
    """Parse daemon.log for the most recent session start time."""
    if not DAEMON_LOG.exists():
        return None
    try:
        text = DAEMON_LOG.read_text()
        # Match: === Session started Sat Mar 21 18:23:25 CET 2026 ===
        matches = re.findall(
            r"=== Session started (.+?) ===", text
        )
        if not matches:
            return None
        last = matches[-1].strip()
        # Parse the date — strip timezone name (CET, EST, etc.)
        # Format: "Sat Mar 21 18:23:25 CET 2026"
        parts = last.split()
        if len(parts) >= 5:
            # Remove timezone name (4th element) if it's not a year
            try:
                int(parts[-1])  # last part should be year
                # Remove timezone abbreviation
                clean = " ".join(parts[:4] + parts[-1:])
                return datetime.strptime(clean, "%a %b %d %H:%M:%S %Y")
            except (ValueError, IndexError):
                pass
        return None
    except Exception:
        return None


def _has_active_positions():
    """Check if there are active hypotheses with open trades."""
    hyp_path = BASE_DIR / "hypotheses.json"
    try:
        with open(hyp_path) as f:
            hypotheses = json.load(f)
        return any(h.get("status") == "active" for h in hypotheses)
    except Exception:
        return False


def _restart_daemon():
    """Restart the research daemon."""
    try:
        # Log the restart
        with open(DAEMON_LOG, "a") as f:
            f.write(f"\n=== Daemon restarted by health_check.py at {datetime.now()} ===\n")

        subprocess.Popen(
            ["nohup", str(BASE_DIR / "run.sh")],
            stdout=open(DAEMON_LOG, "a"),
            stderr=subprocess.STDOUT,
            stdin=subprocess.DEVNULL,
            cwd=str(BASE_DIR),
            start_new_session=True,
        )
        return True
    except Exception as e:
        print(f"Failed to restart daemon: {e}", file=sys.stderr)
        return False


def _send_alert(subject, body_text):
    """Send an alert email."""
    try:
        from email_report import send_email
        html = f"""
        <html><body style="font-family: -apple-system, Arial, sans-serif; max-width: 600px; margin: 0 auto;">
        <h2 style="color: #c62828;">Research System Alert</h2>
        <p style="color: #888;">{datetime.now().strftime('%Y-%m-%d %H:%M')}</p>
        <div style="background: #ffebee; border-left: 4px solid #c62828; padding: 12px 16px; margin: 12px 0;">
            {body_text}
        </div>
        <hr>
        <p style="color: #aaa; font-size: 11px;">Sent by health_check.py</p>
        </body></html>
        """
        send_email(subject, html)
        return True
    except Exception as e:
        print(f"Failed to send alert: {e}", file=sys.stderr)
        return False


def _send_recovery(body_text):
    """Send a recovery notification."""
    try:
        from email_report import send_email
        html = f"""
        <html><body style="font-family: -apple-system, Arial, sans-serif; max-width: 600px; margin: 0 auto;">
        <h2 style="color: #2e7d32;">Research System Recovered</h2>
        <p style="color: #888;">{datetime.now().strftime('%Y-%m-%d %H:%M')}</p>
        <div style="background: #e8f5e9; border-left: 4px solid #2e7d32; padding: 12px 16px; margin: 12px 0;">
            {body_text}
        </div>
        <hr>
        <p style="color: #aaa; font-size: 11px;">Sent by health_check.py</p>
        </body></html>
        """
        send_email("Research system recovered", html)
    except Exception:
        pass


def run_health_check():
    """Main health check. Returns list of issues found."""
    state = _load_state()
    issues = []
    was_alerting = state.get("alerting", False)

    alive = _daemon_is_alive()
    last_session = _last_session_time()
    has_positions = _has_active_positions()

    now = datetime.now()

    # Check 1: Is daemon process alive?
    if not alive:
        issues.append(f"Daemon process (run.sh) is not running.")
        if has_positions:
            issues.append(f"Active positions exist — restarting daemon.")
            if _restart_daemon():
                issues.append("Daemon restarted successfully.")
            else:
                issues.append("FAILED to restart daemon.")

    # Check 2: Has a session run recently?
    if last_session:
        silence_minutes = (now - last_session).total_seconds() / 60
        if silence_minutes > MAX_SILENCE_MINUTES:
            hours = silence_minutes / 60
            issues.append(
                f"No session has run in {hours:.1f} hours "
                f"(last: {last_session.strftime('%H:%M')}, threshold: {MAX_SILENCE_MINUTES} min)."
            )
    elif alive:
        issues.append("Daemon is running but no sessions found in daemon.log.")

    # Check 3: Run stop-loss checks (safety net independent of daemon)
    if has_positions:
        try:
            from trader import check_stop_losses
            actions = check_stop_losses()
            for a in actions:
                if a["action"] in ("closed", "close_failed", "drawdown_alert"):
                    issues.append(
                        f"Stop-loss action: [{a['action']}] "
                        f"{a.get('symbol', '')} {a.get('reason', a.get('message', ''))}"
                    )
        except Exception as e:
            issues.append(f"Stop-loss check failed: {e}")

    # Send alerts or recovery
    if issues:
        if not was_alerting:
            # First alert — send email
            body = "<br>".join(f"<b>{i}</b>" if "FAIL" in i or "not running" in i else i for i in issues)
            _send_alert("Research system issue detected", body)
            state["alerting"] = True
            state["alert_start"] = now.isoformat()
            state["last_alert"] = now.isoformat()
        else:
            # Already alerting — re-alert every 30 minutes
            last_alert = state.get("last_alert", "")
            try:
                last_dt = datetime.fromisoformat(last_alert)
                if (now - last_dt).total_seconds() > 1800:
                    body = "<br>".join(issues)
                    _send_alert("Research system still has issues", body)
                    state["last_alert"] = now.isoformat()
            except (ValueError, TypeError):
                pass
    else:
        if was_alerting:
            _send_recovery("All systems operational. Daemon is running and sessions are on schedule.")
            state["alerting"] = False
            state.pop("alert_start", None)
            state.pop("last_alert", None)

    state["daemon_alive"] = alive
    state["last_session"] = last_session.isoformat() if last_session else None
    state["has_positions"] = has_positions
    state["issues"] = issues
    _save_state(state)

    return issues


if __name__ == "__main__":
    # Load .env
    env_file = BASE_DIR / ".env"
    if env_file.exists():
        for line in env_file.read_text().splitlines():
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                k, v = line.split("=", 1)
                os.environ.setdefault(k.strip(), v.strip().strip("'\""))

    issues = run_health_check()
    if issues:
        for i in issues:
            print(f"  {i}")
    else:
        print("Healthy. Daemon running, sessions on schedule.")
