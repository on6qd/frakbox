"""
Scanner blacklist manager — persistent disqualification memory.
Prevents re-investigating already-ruled-out symbols.
"""
import json
import os
from datetime import datetime

BLACKLIST_PATH = os.path.join(os.path.dirname(__file__), 'scanner_blacklist.json')

def load_blacklist():
    """Load the blacklist from disk."""
    if os.path.exists(BLACKLIST_PATH):
        with open(BLACKLIST_PATH) as f:
            return json.load(f)
    return {}

def save_blacklist(blacklist):
    """Save the blacklist to disk."""
    with open(BLACKLIST_PATH, 'w') as f:
        json.dump(blacklist, f, indent=2)

def is_blacklisted(scanner_name, symbol):
    """Check if a symbol is blacklisted for a given scanner."""
    bl = load_blacklist()
    scanner = bl.get(scanner_name, {})
    # Check both 'disqualified_symbols' and 'false_positives' sections
    return (symbol in scanner.get('disqualified_symbols', {}) or
            symbol in scanner.get('false_positives', {}))

def get_blacklist_reason(scanner_name, symbol):
    """Get the reason a symbol was blacklisted."""
    bl = load_blacklist()
    scanner = bl.get(scanner_name, {})
    entry = (scanner.get('disqualified_symbols', {}).get(symbol) or
             scanner.get('false_positives', {}).get(symbol))
    if entry:
        return entry.get('reason', 'No reason recorded')
    return None

def add_to_blacklist(scanner_name, symbol, reason, section='disqualified_symbols',
                      hypothesis_reference=None):
    """Add a symbol to the blacklist for a scanner."""
    bl = load_blacklist()
    if scanner_name not in bl:
        bl[scanner_name] = {}
    if section not in bl[scanner_name]:
        bl[scanner_name][section] = {}

    bl[scanner_name][section][symbol] = {
        'reason': reason,
        'disqualified_date': datetime.now().strftime('%Y-%m-%d'),
        'hypothesis_reference': hypothesis_reference
    }
    save_blacklist(bl)
    print(f"Added {symbol} to {scanner_name}/{section} blacklist: {reason}")

def filter_blacklisted(scanner_name, symbols):
    """Filter out blacklisted symbols from a list. Returns (clean_list, removed_list)."""
    bl = load_blacklist()
    scanner = bl.get(scanner_name, {})
    blacklisted = set(scanner.get('disqualified_symbols', {}).keys()) | set(scanner.get('false_positives', {}).keys())

    clean = [s for s in symbols if s not in blacklisted]
    removed = [s for s in symbols if s in blacklisted]
    return clean, removed

if __name__ == '__main__':
    import sys
    if len(sys.argv) >= 4:
        # Usage: python blacklist_manager.py <scanner> <symbol> <reason>
        add_to_blacklist(sys.argv[1], sys.argv[2], sys.argv[3])
    else:
        # Show current blacklist
        bl = load_blacklist()
        print(json.dumps(bl, indent=2))
