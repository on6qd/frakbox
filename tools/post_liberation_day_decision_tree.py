"""
Post-Liberation Day Decision Tree (April 2-3, 2026)
=====================================================
Run AFTER CLOSE on April 2, 2026 (or April 3 morning - Good Friday, markets closed).
Next market open: April 6 (Monday) or April 7 (Tuesday).

ACTIVE POSITIONS:
- SPY long (b63a0168): Entry $639.74, hold through April 27. DO NOT touch.
- SYK short (5b09b097): DEADLINE April 2. Should auto-close at deadline.
  Verify: python3 -c "import db; db.init_db(); h = db.get_hypothesis_by_id('5b09b097'); print(h['status'])"

PENDING TRADES (conditional on Liberation Day outcome):
=======================================================

1. WFC SHORT (b73efac3) - fires April 6 09:30
   Entry condition: SPY dropped >2% on April 2 close OR VIX > 30
   If Liberation Day was MILD (SPY flat to +5%): CANCEL
   Cancel: python3 -c "import db; db.init_db(); db.update_hypothesis_fields('b73efac3', trigger=None, status='abandoned')"
   Wait... can't abandon through db directly. Use: record_dead_end? Or leave pending?
   SAFE OPTION: Leave pending but manually note it's conditional.
   
2. AMD SHORT (132e9128) - fires April 6 09:30
   Same condition as WFC. AMD is semiconductor (10% tariff on chips from China worst case).
   Cancel if: SPY UP on April 2 AND VIX < 25
   
3. QCOM SHORT (14de5527) - fires April 6 09:30
   Same as AMD.

4. GLD LONG (b768e8d8) - fires April 7 09:30
   UNCONDITIONAL: Fire regardless. GLD was +4.3% in 2025 OOS.
   
5. AEP LONG (35b63a23) - fires April 7 09:30
   UNCONDITIONAL: Fire regardless. Utility defensive.
   
6. COST LONG (8c2f8cbb) - fires April 7 09:30
   UNCONDITIONAL: Fire regardless. Defensive retail.

7. KRE SHORT (6e732966) - fires April 13 09:30
   Delayed entry (waiting for WFC/AMD/QCOM slots to free up).
   Conditional: Keep if tariffs were significant. Cancel if rollback announced before April 13.

DECISION RULES:
==============
SPY April 2 reaction:
  < -2%: ALL trades fire (WFC, AMD, QCOM + GLD, AEP, COST)
  -2% to 0%: Judgment call. Fire WFC (most validated). AMD/QCOM borderline.
  > 0%: CANCEL WFC, AMD, QCOM. GLD/AEP/COST still fire unconditionally.
  > +2%: Cancel WFC/AMD/QCOM. Consider canceling AEP too (if tariffs rolled back).

VIX April 2 or April 3 level:
  > 35: All trades fire (maximum fear = maximum signal)
  30-35: All trades fire
  25-30: WFC only. Consider canceling AMD/QCOM.
  < 25: Cancel WFC/AMD/QCOM. Keep GLD/AEP/COST.

STLD OOS BASELINE (c2eeee84):
  Record April 6 OPEN price for steel tariff OOS observation.
  No trade placed - observational only.
  5d result check: April 13 close.

COMMANDS:
=========
# To cancel a pending hypothesis trade:
# python3 -c "import db; db.init_db(); db.cancel_pending_trade('HYPOTHESIS_ID')"
# NOTE: Check if cancel_pending_trade exists or use update_hypothesis_fields

# To check current positions:
# python3 run.py --context

# Liberation Day monitor:
# python3 tools/liberation_day_monitor.py
"""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))
import yfinance as yf
import db

BASELINES = {
    'SPY_mar31': 650.34,
    'SPY_apr1': None,  # Will be filled April 1 close
}

def check_liberation_day_conditions():
    """
    Check April 2 conditions and print recommended actions.
    Run AFTER April 2 close.
    """
    print("=== POST-LIBERATION DAY DECISION TREE ===")
    print()
    
    # Get SPY price
    spy = yf.Ticker('SPY').history(period='3d')
    if spy.empty:
        print("ERROR: Cannot get SPY price")
        return
    
    spy_latest = float(spy['Close'].iloc[-1])
    spy_baseline = BASELINES['SPY_mar31']
    spy_change_pct = (spy_latest / spy_baseline - 1) * 100
    
    # Get VIX
    vix = yf.Ticker('^VIX').history(period='3d')
    vix_level = float(vix['Close'].iloc[-1]) if not vix.empty else None
    
    print(f"SPY: {spy_latest:.2f} vs Mar31 baseline {spy_baseline:.2f} = {spy_change_pct:+.1f}%")
    if vix_level:
        print(f"VIX: {vix_level:.1f}")
    print()
    
    # Decision logic
    wfc_fire = False
    amd_qcom_fire = False
    gld_aep_cost_fire = True  # Always fire
    
    if spy_change_pct < -2:
        print("✓ STRONG SELL-OFF: All tariff trades should fire")
        wfc_fire = True
        amd_qcom_fire = True
    elif spy_change_pct < 0:
        print("~ MILD SELL-OFF: Fire WFC, borderline for AMD/QCOM")
        wfc_fire = True
        amd_qcom_fire = True  # Keep but note borderline
    else:
        print(f"✗ MARKET UP ({spy_change_pct:+.1f}%): Cancel WFC/AMD/QCOM")
        wfc_fire = False
        amd_qcom_fire = False
    
    if vix_level and vix_level < 25:
        print(f"✗ LOW VIX ({vix_level:.1f}): Conditions not met for bank/semis short")
        wfc_fire = False
        amd_qcom_fire = False
    
    print()
    print("ACTIONS:")
    if not wfc_fire:
        print("  CANCEL WFC (b73efac3): python3 tools/cancel_trade.py b73efac3")
    else:
        print("  KEEP WFC (b73efac3): Will fire April 6 09:30")
    
    if not amd_qcom_fire:
        print("  CANCEL AMD (132e9128): python3 tools/cancel_trade.py 132e9128")
        print("  CANCEL QCOM (14de5527): python3 tools/cancel_trade.py 14de5527")
    else:
        print("  KEEP AMD (132e9128): Will fire April 6 09:30")
        print("  KEEP QCOM (14de5527): Will fire April 6 09:30")
    
    print("  KEEP GLD (b768e8d8): Fires April 7 09:30 (unconditional)")
    print("  KEEP AEP (35b63a23): Fires April 7 09:30 (unconditional)")
    print("  KEEP COST (8c2f8cbb): Fires April 7 09:30 (unconditional)")
    print("  KEEP KRE (6e732966): Fires April 13 09:30 (conditional on sustained tariffs)")
    print()
    print("ALSO:")
    print("  Record STLD April 6 open (c2eeee84 OOS baseline)")
    print("  Verify SYK (5b09b097) auto-closed April 2")

if __name__ == '__main__':
    check_liberation_day_conditions()
