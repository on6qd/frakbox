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

PORTFOLIO STATE on April 2 (expected):
  - GO: closes March 26 (3d from March 23 entry)
  - HD, ABT, BAX: all close March 27
  - SYK: closes ~April 2 (5d from March 26 entry, deadline April 2)
  - AMT: enters March 30 at 9:30, closes ~April 6-7 (5d hold)
  - VGNT: activates April 1 16:00 (short, 5d hold = closes ~April 8)
  - TDG: may enter late March or early April if it crosses 52w low (hyp e25f0c6f)

Available positions on April 2: 2-3 open (SYK closing, AMT+VGNT active)
CRITICAL: Good Friday April 3 = MARKET CLOSED. All April 3 actions → April 7.

If WFC triggers: enter April 7 open (after Liberation Day announcement April 2)
If VIX fires: also enter April 7 open
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
        return

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
        print("🚨 VIX SPIKE RECOVERY SPY LONG SIGNAL FIRES!")
        print("   → Run Monday April 7 at 9:30 AM (Good Friday April 3 = CLOSED):")
        print("   python tools/activate_vix_spy_trade.py --yes")
        print()
        print("   Expected: +1.69% over 20 days (N=54, OOS validation +2.92%)")
        print("   IMPORTANT: Check portfolio capacity — max 5 positions")
        print("   NOTE: Compatible with systemic shorts (different horizons)")
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

    # --- COST Tariff Defensive Long (NEW 2026-03-26) ---
    print()
    print("=" * 70)
    print("COST TARIFF DEFENSIVE RETAIL LONG SIGNAL (hypothesis 8c2f8cbb)")
    print("=" * 70)
    print("Signal: COST outperforms SPY +3.57% avg over 5 days after major tariff events")
    print("  n=21 (COST+WMT+XLP x 7 events), direction=86%, MT PASSES (5 horizons p<0.05)")
    print("  OOS validation: 6/6 positive (100%) in 2025 events")
    print("  TRIGGER: UNCONDITIONAL - fires regardless of SPY direction")
    print()
    print("  ✓ COST trigger already set: 2026-04-07T09:30 (April 7 open)")
    print("  → COST hypothesis pre-registered, trade_loop WILL fire automatically")
    print("  → No manual action needed UNLESS portfolio >5/5 capacity on April 7")
    print()
    print("  Also validated: WMT (+3.97% 10d, p=0.0356), XLP (+2.40% 10d, p=0.0329)")
    print("  → If COST capacity issue: can use WMT or XLP instead")
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


if __name__ == '__main__':
    check_conditions()

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
    if spy_pct < -0.5:
        print(f"✓ SPY down {spy_pct:+.1f}% → Tariff shock confirmed → GLD LONG signal active")
        print(f"  Action: Set trigger 2026-04-07T09:30 (next trading day after Good Friday)")
        print(f"  python3 -c \"import db; db.init_db(); db.update_hypothesis_fields('b768e8d8', trigger='2026-04-07T09:30', trigger_position_size=5000, trigger_stop_loss_pct=10, trigger_take_profit_pct=15)\"")
    else:
        print(f"  SPY return: {spy_pct:+.1f}% — Market not pricing tariff shock → GLD long uncertain")
        print(f"  Note: GLD historically rallies 20d even in partial-shock scenarios (84% dir)")
        print(f"  Judgment call: if tariff announcement was large, still consider activating.")
