"""
Activate STLD short trade after Liberation Day tariff announcement.
Only run if: (1) broad tariffs announced April 2, (2) portfolio has capacity.
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

import db, trader

HYPOTHESIS_ID = '907d94ec'
SYMBOL = 'STLD'
POSITION_SIZE = 5000
STOP_LOSS_PCT = 10

def activate(dry_run=True):
    db.init_db()
    h = db.get_hypothesis_by_id(HYPOTHESIS_ID)
    if not h:
        print(f"ERROR: Hypothesis {HYPOTHESIS_ID} not found")
        return

    print(f"=== STLD Tariff Short Activation ===")
    print(f"Hypothesis: {HYPOTHESIS_ID}")
    print(f"Signal: STLD underperforms SPY -2.58% avg in 5d after tariff events (n=10, p=0.007, dir=80%)")
    print(f"Status: {h.get('status')}")
    print()

    # Check capacity
    all_hyps = db.load_hypotheses()
    active = [x for x in all_hyps if x.get('status') == 'active']
    print(f"Portfolio: {len(active)}/5 active positions")
    if len(active) >= 5:
        print("⚠ AT CAPACITY — cannot activate STLD short")
        return

    if dry_run:
        print(f"DRY RUN — would activate {SYMBOL} short ${POSITION_SIZE} with {STOP_LOSS_PCT}% stop")
        print("Run with --yes to execute")
        return

    # Set trigger for next market open
    db.update_hypothesis_fields(
        HYPOTHESIS_ID,
        trigger='next_market_open',
        trigger_position_size=POSITION_SIZE,
        trigger_stop_loss_pct=STOP_LOSS_PCT,
    )
    print(f"✓ STLD short trigger set: next_market_open, ${POSITION_SIZE}, {STOP_LOSS_PCT}% stop")

if __name__ == '__main__':
    dry_run = '--yes' not in sys.argv
    activate(dry_run=dry_run)
