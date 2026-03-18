#!/bin/bash
# Generate and install launchd plists for the research scheduler.
# Resolves paths dynamically — no hardcoded user or directory.
#
# Usage:
#   ./install_scheduler.sh          # generate, install, and load
#   ./install_scheduler.sh --remove # unload and remove

set -euo pipefail

PROJECT_DIR="$(cd "$(dirname "$0")" && pwd)"
CURRENT_USER="$(whoami)"
PLIST_DIR="$HOME/Library/LaunchAgents"

SESSIONS=(
  "operations:13:03"    # 9 AM ET → 13:03 UTC (EST) / adjusts for EDT
  "event_scan:17:03"    # 1 PM ET → 17:03 UTC
  "research:21:03"      # 5 PM ET → 21:03 UTC
)

mkdir -p "$PLIST_DIR"

# --- Remove mode ---
if [ "${1:-}" = "--remove" ]; then
  for entry in "${SESSIONS[@]}"; do
    SESSION_TYPE="${entry%%:*}"
    LABEL="com.research.${SESSION_TYPE}"
    PLIST_PATH="$PLIST_DIR/${LABEL}.plist"
    if launchctl list | grep -q "$LABEL" 2>/dev/null; then
      launchctl unload "$PLIST_PATH" 2>/dev/null || true
      echo "Unloaded $LABEL"
    fi
    rm -f "$PLIST_PATH"
    echo "Removed $PLIST_PATH"
  done
  exit 0
fi

# --- Install mode ---
for entry in "${SESSIONS[@]}"; do
  SESSION_TYPE="${entry%%:*}"
  rest="${entry#*:}"
  HOUR="${rest%%:*}"
  MINUTE="${rest#*:}"

  LABEL="com.research.${SESSION_TYPE}"
  PLIST_PATH="$PLIST_DIR/${LABEL}.plist"

  # Unload if already loaded
  if launchctl list | grep -q "$LABEL" 2>/dev/null; then
    launchctl unload "$PLIST_PATH" 2>/dev/null || true
  fi

  cat > "$PLIST_PATH" <<EOF
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
    <key>Label</key>
    <string>${LABEL}</string>

    <key>ProgramArguments</key>
    <array>
        <string>/bin/bash</string>
        <string>${PROJECT_DIR}/daily_research.sh</string>
        <string>${SESSION_TYPE}</string>
    </array>

    <key>StartCalendarInterval</key>
    <dict>
        <key>Hour</key>
        <integer>${HOUR}</integer>
        <key>Minute</key>
        <integer>${MINUTE}</integer>
    </dict>

    <key>WorkingDirectory</key>
    <string>${PROJECT_DIR}</string>

    <key>StandardOutPath</key>
    <string>${PROJECT_DIR}/logs/launchd_stdout.log</string>

    <key>StandardErrorPath</key>
    <string>${PROJECT_DIR}/logs/launchd_stderr.log</string>

    <key>EnvironmentVariables</key>
    <dict>
        <key>PATH</key>
        <string>/usr/local/bin:/usr/bin:/bin:/opt/homebrew/bin</string>
    </dict>

    <key>UserName</key>
    <string>${CURRENT_USER}</string>

    <key>SessionCreate</key>
    <true/>
</dict>
</plist>
EOF

  launchctl load "$PLIST_PATH"
  echo "Installed and loaded: $LABEL → $SESSION_TYPE at ${HOUR}:${MINUTE} UTC"
done

echo ""
echo "Project directory: $PROJECT_DIR"
echo "Running as user: $CURRENT_USER"
echo "Plists installed to: $PLIST_DIR"
echo ""
echo "Verify with: launchctl list | grep com.research"
