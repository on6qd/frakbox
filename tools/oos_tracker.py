#!/usr/bin/env python3
"""OOS Observation Tracker — automates daily tracking of out-of-sample observations.

Reads active observations from the knowledge base (key: 'active_oos_observations'),
fetches latest prices, computes abnormal returns, and stores results.

Usage:
    python tools/oos_tracker.py              # Track all active observations
    python tools/oos_tracker.py --init       # Initialize/reset observation config
    python tools/oos_tracker.py --add        # Add a new observation interactively

Observation format (stored in knowledge base as JSON):
{
    "id": "mnr_seo_apr2026",
    "description": "MNR SEO bought deal OOS",
    "symbols": ["MNR"],
    "benchmark": "SPY",
    "entry_date": "2026-04-08",
    "entry_prices": {"MNR": 12.70, "SPY": 676.01},
    "direction": "short",
    "final_day": 5,
    "hypothesis_id": "673aaa32",
    "knowledge_prefix": "seo_mnr_april2026_oos"
}
"""

import sys
import os
import json
from datetime import datetime, timedelta

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import db as _db
from tools.yfinance_utils import safe_download


def get_trading_days(start_date_str, end_date_str=None):
    """Count trading days between two dates using SPY price data."""
    if end_date_str is None:
        end_date_str = (datetime.now() + timedelta(days=1)).strftime('%Y-%m-%d')

    try:
        df = safe_download('SPY', start=start_date_str, end=end_date_str)
        if df is None or df.empty:
            return 0
        # Exclude the entry date itself — Day1 is the first day AFTER entry
        return len(df) - 1  # -1 because entry date is Day0
    except Exception:
        return 0


def fetch_latest_prices(symbols, benchmark='SPY'):
    """Fetch latest closing prices for symbols and benchmark."""
    all_symbols = list(set(symbols + [benchmark]))
    prices = {}
    end = (datetime.now() + timedelta(days=1)).strftime('%Y-%m-%d')  # tomorrow to include today
    start = (datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d')

    for sym in all_symbols:
        try:
            df = safe_download(sym, start=start, end=end)
            if df is not None and not df.empty:
                prices[sym] = {
                    'close': round(float(df['Close'].iloc[-1]), 4),
                    'date': str(df.index[-1].date()),
                }
        except Exception as e:
            prices[sym] = {'error': str(e)}

    return prices


def compute_abnormal_return(entry_prices, current_prices, symbols, benchmark, direction):
    """Compute abnormal return (symbol return - benchmark return).

    For short direction: positive abnormal = stock underperformed benchmark (good for short).
    For long direction: positive abnormal = stock outperformed benchmark (good for long).
    """
    benchmark_entry = entry_prices.get(benchmark)
    benchmark_current = current_prices.get(benchmark, {}).get('close')

    if not benchmark_entry or not benchmark_current:
        return None

    benchmark_return = (benchmark_current / benchmark_entry - 1) * 100

    results = {}
    for sym in symbols:
        sym_entry = entry_prices.get(sym)
        sym_current = current_prices.get(sym, {}).get('close')

        if not sym_entry or not sym_current:
            results[sym] = {'error': 'missing price'}
            continue

        raw_return = (sym_current / sym_entry - 1) * 100
        abnormal = raw_return - benchmark_return

        # For short: negative abnormal means stock underperformed (correct direction)
        if direction == 'short':
            direction_correct = abnormal < -0.5
        else:
            direction_correct = abnormal > 0.5

        results[sym] = {
            'entry_price': sym_entry,
            'current_price': sym_current,
            'raw_return_pct': round(raw_return, 2),
            'benchmark_return_pct': round(benchmark_return, 2),
            'abnormal_return_pct': round(abnormal, 2),
            'direction_correct': direction_correct,
        }

    return results


def load_observations():
    """Load active OOS observations from knowledge base."""
    _db.init_db()
    effect = _db.get_known_effect('active_oos_observations')
    if not effect:
        return []

    return effect.get('observations', [])


def save_observations(observations):
    """Save active OOS observations to knowledge base."""
    _db.init_db()
    _db.record_known_effect('active_oos_observations', {
        'observations': observations,
        'updated': datetime.now().isoformat(),
    })


def track_observation(obs, current_prices):
    """Track a single observation and return results."""
    entry_date = obs['entry_date']
    symbols = obs['symbols']
    benchmark = obs.get('benchmark', 'SPY')
    direction = obs.get('direction', 'long')
    final_day = obs.get('final_day', 5)

    # Current trading day
    trading_day = get_trading_days(entry_date)

    # Compute abnormal returns
    returns = compute_abnormal_return(
        obs['entry_prices'], current_prices,
        symbols, benchmark, direction
    )

    # Average abnormal return across symbols
    abnormals = [r['abnormal_return_pct'] for r in (returns or {}).values()
                 if isinstance(r, dict) and 'abnormal_return_pct' in r]
    avg_abnormal = round(sum(abnormals) / len(abnormals), 2) if abnormals else None

    is_final = trading_day >= final_day
    status = 'FINAL' if is_final else 'IN_PROGRESS'

    result = {
        'id': obs['id'],
        'description': obs['description'],
        'direction': direction,
        'trading_day': trading_day,
        'final_day': final_day,
        'status': status,
        'avg_abnormal_pct': avg_abnormal,
        'symbol_results': returns,
        'price_date': next(
            (p.get('date') for p in current_prices.values() if isinstance(p, dict) and 'date' in p),
            None
        ),
    }

    return result


def init_observations():
    """Initialize with current active observations."""
    observations = [
        {
            "id": "mnr_seo_apr2026",
            "description": "MNR SEO bought deal OOS — 424B4 filed April 8",
            "symbols": ["MNR"],
            "benchmark": "SPY",
            "entry_date": "2026-04-08",
            "entry_prices": {"MNR": 12.70, "SPY": 676.01},
            "direction": "short",
            "final_day": 5,
            "hypothesis_id": "673aaa32",
            "knowledge_prefix": "seo_mnr_april2026_oos",
        },
        {
            "id": "auto_tariff_event2_apr2026",
            "description": "Auto import tariff Event2 — TM/HMC short from Apr 2",
            "symbols": ["TM", "HMC"],
            "benchmark": "SPY",
            "entry_date": "2026-04-02",
            "entry_prices": {"TM": 207.01, "HMC": 24.15, "SPY": 655.83},
            "direction": "short",
            "final_day": 10,
            "knowledge_prefix": "auto_import_tariff_short_event2",
        },
    ]
    save_observations(observations)
    print(f"Initialized {len(observations)} active OOS observations.")
    return observations


def main():
    _db.init_db()

    if '--init' in sys.argv:
        init_observations()
        return

    observations = load_observations()
    if not observations:
        print("No active OOS observations. Run with --init to initialize.")
        return

    # Collect all symbols we need prices for
    all_symbols = set()
    benchmarks = set()
    for obs in observations:
        all_symbols.update(obs['symbols'])
        benchmarks.add(obs.get('benchmark', 'SPY'))
    all_symbols.update(benchmarks)

    # Fetch prices once
    print(f"Fetching prices for {len(all_symbols)} symbols...", file=sys.stderr)
    current_prices = fetch_latest_prices(list(all_symbols), list(benchmarks)[0])

    # Track each observation
    results = []
    completed = []

    for obs in observations:
        result = track_observation(obs, current_prices)
        results.append(result)

        # Mark completed observations
        if result['status'] == 'FINAL':
            completed.append(obs['id'])

    # Print summary
    print("\n" + "=" * 70)
    print(f"OOS OBSERVATION TRACKER — {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    print("=" * 70)

    for r in results:
        direction_arrow = "↓" if r['direction'] == 'short' else "↑"
        status_icon = "✅" if r['status'] == 'FINAL' else "📊"

        print(f"\n{status_icon} {r['description']}")
        print(f"   Day {r['trading_day']}/{r['final_day']} | Direction: {r['direction']} {direction_arrow}")

        if r['avg_abnormal_pct'] is not None:
            sign = '+' if r['avg_abnormal_pct'] > 0 else ''
            correct = '✓' if (r['direction'] == 'short' and r['avg_abnormal_pct'] < -0.5) or \
                              (r['direction'] == 'long' and r['avg_abnormal_pct'] > 0.5) else '✗'
            print(f"   Avg abnormal: {sign}{r['avg_abnormal_pct']}% {correct}")

        for sym, sr in (r.get('symbol_results') or {}).items():
            if isinstance(sr, dict) and 'abnormal_return_pct' in sr:
                sign = '+' if sr['abnormal_return_pct'] > 0 else ''
                print(f"   {sym}: {sr['current_price']:.2f} (entry {sr['entry_price']:.2f}) "
                      f"raw={sign}{sr['raw_return_pct']}% abn={sign}{sr['abnormal_return_pct']}%")

        if r['status'] == 'FINAL':
            print(f"   ⚠️  FINAL — record result and remove from tracking")

    print("\n" + "-" * 70)

    # Store results in knowledge base
    for r in results:
        obs = next((o for o in observations if o['id'] == r['id']), None)
        if obs:
            prefix = obs.get('knowledge_prefix', r['id'])
            day = r['trading_day']
            key = f"{prefix}_day{day}"

            _db.record_known_effect(key, {
                'status': r['status'],
                'trading_day': day,
                'avg_abnormal_pct': r['avg_abnormal_pct'],
                'symbol_results': r['symbol_results'],
                'price_date': r['price_date'],
                'direction': obs.get('direction', 'neutral'),
            })

    # JSON output for machine consumption
    output = {
        "scan_time": datetime.now().isoformat(),
        "n_observations": len(observations),
        "n_completed": len(completed),
        "results": results,
    }
    print(json.dumps(output, indent=2, default=str))

    # Remove completed observations from active list
    if completed:
        remaining = [o for o in observations if o['id'] not in completed]
        save_observations(remaining)
        print(f"\nRemoved {len(completed)} completed observation(s). {len(remaining)} remaining.",
              file=sys.stderr)


if __name__ == "__main__":
    main()
