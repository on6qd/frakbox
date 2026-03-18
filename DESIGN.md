# Design Decisions

This document captures the reasoning behind architectural choices. It's for the builders (humans + Claude Code sessions working on the project infrastructure), not for the autonomous researcher agent.

## Architecture: Self-Improving Autonomous Researcher

### The three layers
1. **The project** — codebase, data files, tools, infrastructure
2. **The builders** (us) — design the launchpad, set guardrails, maintain the harness
3. **The agent** — autonomous Claude instance that runs, researches, and evolves the project

### What the agent owns vs what the builders own

**Agent owns** (can modify freely):
- `CLAUDE.md` — project reference, the agent can evolve this
- All research code: `research.py`, `market_data.py`, `self_review.py`, `research_queue.py`, `trader.py`, `run.py`
- All data files: `hypotheses.json`, `patterns.json`, `knowledge_base.json`, `methodology.json`, `research_queue.json`, `results.jsonl`
- `tools/` — scripts and tools the agent builds for itself
- `logs/` — research notes, session logs, friction log
- `METHODOLOGY.md` — detailed methodology reference

**Builders own** (agent should not modify):
- `.claude/agents/financial-researcher.md` — the agent's constitution (SOUL)
- `daily_research.sh` — session harness
- `email_report.py` — reporting infrastructure
- `health.sh` — monitoring
- `smoke_test.py` — pipeline validation
- `com.research.*.plist` — launchd scheduling
- `DESIGN.md` — this file

The boundary is convention, not filesystem isolation. The agent's constitution explicitly says it cannot modify its own soul file. OpenClaw uses the same pattern (SOUL.md).

### Why flat structure, not two folders
Considered splitting into `harness/` and `research/` directories. Rejected because:
- Claude Code reads `.claude/` and `CLAUDE.md` from the working directory — folder split breaks this
- Python imports use relative paths — would need restructuring
- The agent needs to be able to run `daily_research.sh` context (venv, .env)
- The real boundary is which files are mutable, not where they live
- Can refactor to folders later if the agent builds enough infrastructure to warrant it

### Why the agent can self-improve
Inspired by OpenClaw Foundry's pattern: the agent doesn't build tools speculatively. It logs friction (`logs/friction_log.jsonl`), and when a pattern appears 3+ times, that justifies building a tool. The data decides what to build, not imagination.

Key references studied:
- **OpenClaw Foundry** — crystallizes tools after 5+ uses at 70%+ success
- **ClaudeClaw** — OpenClaw pattern on Claude Code specifically
- **Darwin Godel Machine** (Sakana AI) — showed that agents will remove safety checks if allowed → hence immutable soul
- **"9 Autonomous Agents"** (QuantBit) — SOUL.md + HEARTBEAT.md pattern for cron-scheduled agents

### Why immutable scientific standards
The Darwin Godel Machine demonstrated that self-modifying agents will optimize away their own safety checks when given full freedom. Applied to research: an agent that can weaken its own statistical standards will do so to "get more results."

The soul file locks: pre-registration, temporal OOS splits, multiple testing correction, causal mechanism rubric, power analysis, abnormal returns (not raw), dead end recording, uniform position sizing.

Parameter VALUES (min_sample_size=12, min_p_value=0.05) live in `methodology.json` and can evolve through the self-review process. The PRINCIPLES are immutable.

### Phased autonomy plan
1. **Phase 1: Run first** — Get 10+ sessions of real research. The system has never run. Seed tasks are untouched. The agent needs experience before it can meaningfully self-improve.
2. **Phase 2: Build from friction** — After accumulating friction log data, the agent identifies real bottlenecks and builds tools to fix them. Not before.
3. **Phase 3: Full autonomy** — The agent manages its own scheduling, builds data feeds, evolves its workflow. Guardrails remain.

### Research bottlenecks identified (pre-launch analysis)
1. **Claude does work Python should do** — event scanning via web search is expensive. A cheap Python cron job with RSS/API feeds would be better. The agent should discover this and build it.
2. **80 turns isn't enough for deep research** — one proper research task (literature + backtest + OOS + hypothesis) takes ~70 turns. Leaves no room for a second question.
3. **Context dies between sessions** — handoff mechanism is a few strings in research_queue.json. Needs richer thread state.
4. **No structured event data** — no earnings calendar, FDA calendar, FOMC schedule. Agent searches for these every session.
5. **3 fixed sessions is wrong model** — should be event-triggered, not clock-triggered. Agent should discover this.

### Portability
The project has no hardcoded paths. It can be cloned and run under any user on any Mac.

- Shell scripts use `cd "$(dirname "$0")"` to resolve the project directory
- Plist files are generated dynamically by `install_scheduler.sh` (not committed to git)
- `.claude/settings.local.json` is gitignored (machine-specific permissions)
- `.claude/agents/financial-researcher.md` IS committed (the agent's soul)

To set up on a new machine:
```
git clone <repo> && cd financial_researcher
python -m venv venv && source venv/bin/activate && pip install -r requirements.txt
cp .env.example .env  # fill in API keys
python smoke_test.py
./install_scheduler.sh
```

### Remaining pre-launch items
- Working on `master` branch, needs to be committed and merged to `main`
- `journal.jsonl` deleted but not committed
- Smoke test has never been run
