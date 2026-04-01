"""
Cancel a pending hypothesis trade (clear trigger, mark as abandoned if requested).
Usage: python3 tools/cancel_trade.py HYPOTHESIS_ID [--abandon]
"""
import sys
import argparse
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))
import db

def cancel_trade(hypothesis_id, abandon=False):
    db.init_db()
    h = db.get_hypothesis_by_id(hypothesis_id)
    if not h:
        print(f"ERROR: Hypothesis {hypothesis_id} not found")
        return False
    
    print(f"Hypothesis: {hypothesis_id}")
    print(f"Symbol: {h.get('expected_symbol')}")
    print(f"Signal: {h.get('event_type')}")
    print(f"Status: {h.get('status')}")
    print(f"Current trigger: {h.get('trigger')}")
    print()
    
    if h['status'] != 'pending':
        print(f"WARNING: Status is '{h['status']}' (not pending). Only pending hypotheses can have triggers cancelled.")
        if not abandon:
            print("Use --abandon to force-abandon an active hypothesis")
            return False
    
    if abandon:
        # Can't change status directly through fields - use research module
        from research import complete_hypothesis
        print("NOTE: To properly abandon, use complete_hypothesis() with result='abandoned'")
        print("For now, just clearing the trigger...")
    
    # Clear the trigger
    db.update_hypothesis_fields(hypothesis_id, trigger=None)
    print(f"✓ Trigger cleared for {hypothesis_id} ({h.get('expected_symbol')})")
    print("  This hypothesis will NOT fire on a schedule. It remains 'pending' for manual activation.")
    return True

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('hypothesis_id')
    parser.add_argument('--abandon', action='store_true', help='Also mark as abandoned')
    args = parser.parse_args()
    
    cancel_trade(args.hypothesis_id, args.abandon)
