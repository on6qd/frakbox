"""
April 2, 2026 "Liberation Day" Tariff Runbook
==============================================
US tariff announcements expected ~6:00 PM ET on April 2.

After-market actions (run April 2 after close, 4:15-5:00 PM ET):
  1. Run this script to assess conditions
  2. If conditions met, it prints exact activation commands for April 7 open

CRITICAL: Good Friday April 3 = MARKET CLOSED. Next open = Monday April 7, 2026.

BACKTEST EXPECTATION:
  Systemic days (SPY<-0.5%, >=5 first-touch lows): -1.88% abnormal over 5 days
  VIX close > 30: SPY recovers +1.69% over 20 days

UPDATED PORTFOLIO STATE (as of 2026-03-28):
  - GO: CLOSED March 26 at $6.75 (entry $6.19, +9% return)
  - HD, ABT, BAX: CLOSED March 27 (all confirmed complete)
  - SYK: active short until April 2 (entry $326.23, currently -0.4%)
  - SPY LONG (b63a0168): TRIGGERED for March 31 open (VIX=31.0 on March 27!)
    *** VIX ALREADY CROSSED 30 — DO NOT activate SPY long again on April 2 ***
  - AMT, VGNT: ABANDONED (tickers not tradeable in Alpaca paper)
  - TDG: monitoring (near 52w low but not yet crossed)

PRE-SELLOFF REGIME CAVEAT (identified 2026-03-28, updated 2026-03-28):
  SPY is down ~7.3% over 20 days and -8.6% from 60d peak going into Liberation Day.
  Training distribution: 20d pre-moves ranged -3.5% to +4.5% (only 2019-08-23 was worse at -5.7%).
  Liberation Day 2026 at -7.3% pre-move is OUTSIDE the training distribution.

  CRITICAL ANALOG — 2019-08-23 (most similar pre-selloff of -5.7%):
    - Context: Trump threatened tariffs on $300B China goods (pre-selloff -5.7%)
    - Result: OPPOSITE of signal → SPY bounced +5.2%, GLD -5.5% abnormal, KO -3.7%, KRE +5.2%
    - Reason: Event was followed by tariff rollback, not escalation
    - Implication: When market already sold off AND tariffs don't escalate → market BOUNCES

  WHY THE SPY GATE PROTECTS US:
    - Gate: SPY must be DOWN on April 2 close (after announcement)
    - If announcement was a rollback/small: SPY likely UP → gate blocks defensive longs
    - If announcement was escalation: SPY DOWN → gate allows defensive longs
    - The 2019-08-23 analog would NOT pass the gate (SPY rallied after announcement)

  RESIDUAL RISK:
    - If tariffs are announced large AND market initially sells off at 6pm but bounces overnight
    - We would enter April 7 at a bounce high (worst case scenario)
    - No complete protection against this; but if tariffs are truly large, sustained selloff is likely

  KO/XLU UNCONDITIONAL TRIGGERS (fire April 7 regardless of gate):
    - Currently KO (dbe0dc29) and XLU (9184ba0f) do NOT have SPY gate
    - If SPY is UP on April 2, these WILL fire on April 7 into a bounce → BAD
    - RECOMMENDED ACTION: If SPY > +0.5% on April 2, manually cancel KO and XLU triggers
    - Command: Check db.update_hypothesis_fields for dbe0dc29, 9184ba0f

CAPACITY ON APRIL 7:
  - SPY long (1 slot, 20d hold from March 31)
  - SYK closes April 2 → frees 0 (not yet activated in system)
  - Slots available: 4 (max 5 - SPY)
  Priority: GLD > KO > KRE > XLU > WFC > STLD > COST (per original runbook)
  If SPY<-0.5% on April 2: GLD+WFC fire (+2) → Total used: SPY+GLD+KO+XLU+WFC = 5/5
  COST and STLD blocked if all 5 fill.

CRITICAL: Good Friday April 3 = MARKET CLOSED. All April 3 actions → April 7.

If WFC triggers: enter April 7 open (after Liberation Day announcement April 2)
VIX SPY long: ALREADY TRIGGERED for March 31 — do NOT fire again April 7
"""

import sys
import yfinance as yf
import pandas as pd
from datetime import datetime, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

import db


def check_conditions():
    """Check April 2 trigger conditions."""
    db.init_db()

    print("=" * 70)
    print("APRIL 2, 2026 LIBERATION DAY — POST-MARKET ASSESSMENT")
    print("=" * 70)
    print()

    # --- SPY Return ---
    end = datetime.now()
    start = end - timedelta(days=5)
    spy = yf.download('SPY', start=start, end=end + timedelta(days=1),
                      auto_adjust=True, progress=False)
    if spy.empty:
        print("ERROR: Could not fetch SPY data")
        return None

    if isinstance(spy.columns, pd.MultiIndex):
        spy_close = spy['Close']['SPY'].dropna()
    else:
        spy_close = spy['Close'].dropna()

    spy_ret = (spy_close.iloc[-1] / spy_close.iloc[-2] - 1)
    spy_pct = spy_ret * 100
    spy_condition = spy_ret < -0.005  # <-0.5%
    print(f"1. SPY Return Today: {spy_pct:.2f}%")
    print(f"   Condition (need <-0.5%): {'✓ PASS' if spy_condition else '✗ FAIL'}")

    # --- VIX Level ---
    vix = yf.download('^VIX', start=start, end=end + timedelta(days=1),
                      auto_adjust=True, progress=False)
    if not vix.empty:
        if isinstance(vix.columns, pd.MultiIndex):
            vix_series = vix['Close'].iloc[:, 0].dropna()
        else:
            vix_series = vix['Close'].dropna()
        vix_close = float(vix_series.iloc[-1])
        vix_condition = vix_close > 30
        print(f"\n2. VIX Close Today: {vix_close:.1f}")
        print(f"   Condition (need >30 for SPY long): {'✓ PASS' if vix_condition else '✗ FAIL'}")
    else:
        vix_close = None
        vix_condition = False
        print("\n2. VIX: Could not fetch")

    # --- 52w Low First Touches ---
    print("\n3. Scanning for first-touch 52w lows (this takes 1-2 min)...")
    try:
        from tools.systemic_52w_low_scanner import scan
        result = scan(date_str=None, verbose=False)
        n_lows = result.get('n_stocks_at_low', 0)
        stocks_at_low = result.get('stocks_at_low', [])
        lows_condition = n_lows >= 5
        print(f"   First-touch 52w lows: {n_lows} (need >=5)")
        if stocks_at_low:
            print(f"   Stocks: {', '.join(stocks_at_low[:10])}")
        print(f"   Condition: {'✓ PASS' if lows_condition else '✗ FAIL'}")
    except Exception as e:
        print(f"   ERROR scanning: {e}")
        n_lows = 0
        stocks_at_low = []
        lows_condition = False

    # --- Portfolio Capacity ---
    hypotheses = db.load_hypotheses()
    active_count = len([h for h in hypotheses if h.get('status') == 'active'])

    print(f"\n4. Portfolio capacity: {active_count}/5 active positions")
    available_slots = 5 - active_count
    print(f"   Available slots: {available_slots}")

    # --- Summary ---
    print()
    print("=" * 70)
    print("TRIGGER SUMMARY")
    print("=" * 70)

    systemic_fires = spy_condition and lows_condition
    vix_fires = vix_condition

    if systemic_fires:
        print("🚨 SYSTEMIC 52W LOW SHORT SIGNAL FIRES!")
        print("   → Run Monday April 7 at 9:30 AM (GOOD FRIDAY APRIL 3 = CLOSED):")
        print()
        print("   PRE-REGISTERED CANDIDATES (use these first):")
        print("   1. ADBE ($98B, hypothesis f93527a2) — PRIMARY CANDIDATE")
        print("      python tools/activate_adbe_trade.py --yes")
        print("   2. Any other large-cap at 52w low (hypothesis f055dc19):")
        # Sort by market cap (rough proxy: just list all)
        candidates = ['GIS', 'SBAC', 'DPZ', 'OTIS', 'BSX', 'TAP']
        if stocks_at_low:
            # Show scanner results + known candidates
            combined = list(dict.fromkeys(stocks_at_low + candidates))
        else:
            combined = candidates
        n_to_trade = min(available_slots - 1, len(combined))  # -1 because ADBE takes one slot
        print(f"      python tools/activate_systemic_short.py --ticker <TICKER> --yes")
        print(f"      Best candidates (pre-screened, verify still at 52w low):")
        for ticker in combined[:n_to_trade]:
            print(f"      - {ticker}")
        print()
        print("   Expected: -1.88% abnormal over 5 days each")
        print("   IMPORTANT: Check each stock is STILL at/below 52w low at April 7 open")
        print("   NOTE: Can short multiple stocks if portfolio capacity allows")
        print("   CAPACITY CHECK: If MKC+NKE+CAG+VGNT all active = 4/5. Can add ADBE only.")
    else:
        print("✗ Systemic short signal NOT triggered")
        if not spy_condition:
            print(f"  → SPY only down {spy_pct:.2f}% (need <-0.5%)")
        if not lows_condition:
            print(f"  → Only {n_lows} first-touch 52w lows (need >=5)")

    if vix_fires:
        print()
        print("⚠️  VIX SPIKE RECOVERY — ALREADY TRIGGERED (2026-03-27)")
        print("   VIX closed at 31.0 on March 27 — SPY long (b63a0168) was set for March 31 open.")
        print("   *** DO NOT ACTIVATE AGAIN — SPY long already running from March 31 entry ***")
        print()
        print("   If SPY long is NOT yet active (check python3 run.py --status):")
        print("   THEN run: python tools/activate_vix_spy_trade.py --yes")
        print()
        print("   Expected: +1.69% over 20 days (N=54, OOS validation +2.92%)")
        print("   IMPORTANT: Check portfolio capacity — max 5 positions")
        print("   NOTE: Compatible with tariff sector plays (different horizons)")
    else:
        if vix_close:
            print(f"\n✗ VIX long NOT triggered (VIX={vix_close:.1f}, need >30)")

    # --- WFC Tariff Short (NEW 2026-03-26) ---
    print()
    print("=" * 70)
    print("WFC TARIFF BANK SHORT SIGNAL (hypothesis b73efac3)")
    print("=" * 70)
    print("Signal: WFC underperforms SPY -2.39% avg over 5 days after major tariff events")
    print("  n=8, direction=88%, p=0.0045. Validated 2018-2025.")
    print()
    wfc_condition = spy_condition  # tariff shock = SPY down
    if spy_pct is not None:
        large_tariff = True  # Set manually based on news: is announcement >15% reciprocal?
        print(f"  SPY return today: {spy_pct:.2f}%")
        print(f"  SPY down condition: {'✓' if spy_condition else '✗'}")
    print()
    print("  CHECK MANUALLY: Was tariff announcement >15% universal/reciprocal?")
    print("  ONE MISS: 2025-02-01 (+0.5% abnormal when market rallied after announcement)")
    print("  → If SPY is UP: probably don't activate (market not pricing shock)")
    print()
    if spy_condition:
        print("  ✓ SPY is DOWN → Conditions favor WFC short activation")
        print("  → Run April 7 at market open (Good Friday April 3 = CLOSED):")
        print("     python tools/activate_wfc_tariff_trade.py --yes")
    else:
        print("  ✗ SPY is UP → Caution: WFC short may not work (see 2025-02-01 miss)")

    # --- KO/COST/XLU Tariff Defensive Longs (CONDITIONAL as of 2026-03-28) ---
    print()
    print("=" * 70)
    print("KO/COST/XLU TARIFF DEFENSIVE LONGS (hypotheses dbe0dc29/8c2f8cbb/9184ba0f)")
    print("=" * 70)
    print("Signal: KO +4.4% 10d, COST +3.57% 5d, XLU +3.36% 20d after tariff events")
    print("  Validated 2018-2025, but PRE-SELLOFF REGIME CAVEAT (identified 2026-03-28):")
    print("  In pre-sold markets (SPY down >5% before event), signal is UNRELIABLE:")
    print("  - 2018-03-01 (pre-sold -5%): COST_abn=-4.2%, GLD_abn=-1.8% (SPY bounced +2.4%)")
    print("  - 2019-08-23 (pre-sold -5.7%): COST_abn=+5.1% but GLD_abn=-5.5%")
    print("  Current SPY is down ~9% from highs — OUTSIDE training distribution")
    print("  TRIGGERS CLEARED (2026-03-28): These are now CONDITIONAL on April 2 outcome")
    print()
    if spy_condition:
        print("  ✓ SPY DOWN on April 2 → Tariff shock confirmed → ACTIVATE DEFENSIVES")
        print("  Run these BEFORE midnight April 6 (to fire at April 7 open):")
        print()
        print("  KO (hypothesis dbe0dc29, 10d hold):")
        print("    python3 -c \"import db; db.init_db(); db.update_hypothesis_fields(")
        print("    'dbe0dc29', trigger='2026-04-07T09:30', trigger_position_size=5000,")
        print("    trigger_stop_loss_pct=10)\"")
        print()
        print("  XLU (hypothesis 9184ba0f, 20d hold):")
        print("    python3 -c \"import db; db.init_db(); db.update_hypothesis_fields(")
        print("    '9184ba0f', trigger='2026-04-07T09:30', trigger_position_size=5000,")
        print("    trigger_stop_loss_pct=10)\"")
        print()
        print("  COST (hypothesis 8c2f8cbb, 5d hold) - lower priority, activate if capacity:")
        print("    python3 -c \"import db; db.init_db(); db.update_hypothesis_fields(")
        print("    '8c2f8cbb', trigger='2026-04-07T09:30', trigger_position_size=5000,")
        print("    trigger_stop_loss_pct=10)\"")
        print()
        print("  Capacity check: SPY(1) + GLD(2) + KO(3) + XLU(4) + KRE or WFC(5) = FULL")
        print("  COST blocked if at capacity. Also validated: WMT, XLP as alternatives.")
    else:
        print("  ✗ SPY UP on April 2 → Possible bounce/relief rally → DO NOT ACTIVATE")
        print("  → Risk: defensive longs lag in bounce (2018-03-01: COST -4.2% abnormal)")
        print("  → If you still want to activate, wait to see if SPY continues down April 7")
        print("  → Check: is the market reacting poorly April 7 morning? If yes, can still set.")
    print()

    # --- Other Pending Signals ---
    print()
    print("=" * 70)
    print("UPCOMING SIGNALS TO MONITOR (April-May 2026)")
    print("=" * 70)
    print("• VGNT/FDXF/HONA: ABANDONED (tickers not found in Alpaca paper trading, 2026-03-27)")
    print("• REPL: PDUFA April 10. If 2nd CRL and >40% drop → activate_repl_trade.py")
    print("  - If >40% but <55% drop: use hypothesis 5f805860 (REPL short)")
    print("  - If >55% drop: use hypothesis d302c84b (clinical efficacy failure)")
    print("• CAPR: PDUFA August 22, 2026 (Capricor Therapeutics, Duchenne MD)")
    print("• HON Investor Day June 3, 2026 → HONA ticker confirmation for spinco trade")
    print("• S&P 500 Q2 rebalance announcement: ~June 5, 2026")
    print()
    print("• If another systemic selloff occurs (April 3-8): re-run scanner daily")
    print("• VIX monitoring: needs to CLOSE above 30 (check each day at 4:15 PM ET)")
    print()
    print("Run: python tools/activate_systemic_short.py --ticker TICKER --yes")
    print("Run: python tools/activate_vix_spy_trade.py --yes")
    return spy_pct


if __name__ == '__main__':
    spy_pct = check_conditions()

    # --- GLD Tariff Long (NEW 2026-03-27) ---
    print()
    print("=" * 70)
    print("GLD GOLD LONG SIGNAL (hypothesis b768e8d8)")
    print("=" * 70)
    print("Signal: GLD outperforms SPY by +3.87% avg over 20 days after tariff events.")
    print("        N=19 events (2009-2025), 84% direction, p=0.0014 VALIDATED.")
    print("        5-day signal is noisy (50% direction) - initial selloff/margin calls.")
    print("        20-day effect is robust: gold recovers as inflation expectations rise.")
    print()
    if spy_pct is not None and spy_pct < -0.5:
        print(f"✓ SPY down {spy_pct:+.1f}% → Tariff shock confirmed → GLD LONG signal active")
        print(f"  Action: Set trigger 2026-04-07T09:30 (next trading day after Good Friday)")
        print(f"  python3 -c \"import db; db.init_db(); db.update_hypothesis_fields('b768e8d8', trigger='2026-04-07T09:30', trigger_position_size=5000, trigger_stop_loss_pct=10, trigger_take_profit_pct=15)\"")
    elif spy_pct is not None:
        print(f"  SPY return: {spy_pct:+.1f}% — Market not pricing tariff shock → GLD long uncertain")
        print(f"  Note: GLD historically rallies 20d even in partial-shock scenarios (84% dir)")
        print(f"  Judgment call: if tariff announcement was large, still consider activating.")
    else:
        print("  ERROR: Could not determine SPY return")

    # --- STLD Tariff Short (NEW 2026-03-27) ---
    print()
    print("=" * 70)
    print("STLD DOMESTIC STEEL SHORT SIGNAL (hypothesis 907d94ec)")
    print("=" * 70)
    print("Signal: STLD (Steel Dynamics) underperforms SPY -2.58% in 5 days after tariff events.")
    print("        N=10 (2018-2025), 80% SHORT direction, p=0.007. Passes multiple testing.")
    print("        Counter-intuitive: tariff protection < demand destruction for domestic steel.")
    print("        Discovery (2018-2019): n=6, avg=-3.37%, dir=83%")
    print("        OOS (2025): n=4, avg=-2.22%, dir=75% — CONFIRMED")
    print()
    print("  CAPACITY NOTE (UPDATED 2026-03-28): Only activate if portfolio has room (max 5).")
    print("  SPY long fires unconditionally March 31 (1 slot).")
    print("  If SPY<-0.5% on April 2: GLD+WFC+KRE+STLD are candidates (+4 conditional slots).")
    print("  KO/XLU/COST also conditional. Priority: SPY(1)>GLD(2)>KO(3)>KRE(4)>XLU(5).")
    print("  STLD = lower priority, likely blocked. Only activate if SPY>5 slots available.")
    print()
    if spy_pct is not None and spy_pct < -0.5:
        # Count active positions
        hypotheses = db.load_hypotheses()
        active = [h for h in hypotheses if h.get('status') == 'active']
        if len(active) < 4:
            print(f"✓ SPY down {spy_pct:+.1f}% + portfolio has room → STLD SHORT signal active")
            print(f"  Action: python3 -c \"import db; db.init_db(); db.update_hypothesis_fields(")
            print(f"    '907d94ec', trigger='2026-04-07T09:30', trigger_position_size=5000,")
            print(f"    trigger_stop_loss_pct=10)\"")
        else:
            print(f"⚠ SPY down {spy_pct:+.1f}% but portfolio full ({len(active)}/5 active)")
            print(f"  STLD signal valid but skipping for capacity. Wait for an existing position to close.")
    elif spy_pct is not None:
        print(f"  SPY return: {spy_pct:+.1f}% — Mild tariff reaction. STLD short less certain.")
        print(f"  Note: 2025-04-02 event shows STLD +0.39% when market reacted positively.")
        print(f"  Recommendation: Only activate STLD short if SPY is DOWN (tariff shock scenario).")
    else:
        print("  ERROR: Could not determine SPY return")

    # --- KRE Regional Bank Tariff Short (NEW 2026-03-27) ---
    print()
    print("=" * 70)
    print("KRE REGIONAL BANK SHORT SIGNAL (hypothesis 6e732966)")
    print("=" * 70)
    print("Signal: KRE underperforms SPY -3.08% avg over 20 DAYS after tariff escalation.")
    print("        N=10 (2018-2025), 89% SHORT direction, 20d p=0.016. Passes MT.")
    print("        10d: -2.73%, 90% direction, p=0.008 (even stronger direction)")
    print("        5d signal weak (p=0.21) — this is a SLOW-BURN 20-day play, not a 5d play")
    print("        Hold time: ~20 days. Entry April 7 → exit ~April 27")
    print()
    print("  PRIORITY NOTES:")
    print("  - WFC fires if SPY<-0.5% (5d play). KRE is COMPLEMENTARY (20d play).")
    print("  - If WFC is already active: KRE adds a DIFFERENT timeline/ETF short")
    print("  - KRE has STRONGER direction consistency (89%) than WFC (~88%)")
    print("  - But WFC has better expected 5d return (-1.94% vs -1.30% for KRE at 5d)")
    print()
    if spy_pct is not None and spy_pct < -0.5:
        hypotheses = db.load_hypotheses()
        active = [h for h in hypotheses if h.get('status') == 'active']
        if len(active) < 4:
            print(f"✓ SPY down {spy_pct:+.1f}% + portfolio has room → KRE SHORT signal active")
            print(f"  Action: python tools/activate_kre_tariff_trade.py --yes")
        else:
            print(f"⚠ SPY down but portfolio full ({len(active)}/5). KRE valid but no capacity.")
    else:
        print(f"  SPY return: {spy_pct:+.1f}% — Only activate KRE short if SPY DOWN and capacity allows")
