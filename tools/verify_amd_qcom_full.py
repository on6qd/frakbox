"""Verify full AMD+QCOM basket statistics across all 8 events."""
import sys
sys.path.insert(0, '.')
import pandas as pd
import numpy as np
from scipy import stats
from tools.yfinance_utils import get_close_prices

all_events = ['2018-03-01', '2018-03-22', '2018-06-15', '2018-07-06', '2018-09-24',
              '2019-05-10', '2025-03-26', '2025-04-02']

tickers = ['AMD', 'QCOM', 'SPY']
close = get_close_prices(tickers, start='2018-01-01', end='2025-04-30')

results_5d = []
results_10d = []

for event_str in all_events:
    event = pd.Timestamp(event_str)
    if event not in close.index:
        idx = close.index.searchsorted(event)
        event = close.index[idx]
    
    for horizon, results in [(5, results_5d), (10, results_10d)]:
        exit_idx = close.index.searchsorted(event) + horizon
        if exit_idx >= len(close.index):
            continue
        exit_date = close.index[exit_idx]
        
        amd_ret = (close.loc[exit_date, 'AMD'] - close.loc[event, 'AMD']) / close.loc[event, 'AMD'] * 100
        qcom_ret = (close.loc[exit_date, 'QCOM'] - close.loc[event, 'QCOM']) / close.loc[event, 'QCOM'] * 100
        spy_ret = (close.loc[exit_date, 'SPY'] - close.loc[event, 'SPY']) / close.loc[event, 'SPY'] * 100
        basket_abn = (amd_ret + qcom_ret) / 2 - spy_ret
        results.append((event_str, basket_abn))

# Discovery (0-4) vs Validation (5-7)
print("DISCOVERY (2018, n=5):")
disc_5d = [r[1] for r in results_5d[:5]]
disc_10d = [r[1] for r in results_10d[:5]]
print(f"  5d avg={np.mean(disc_5d):.2f}%, neg_rate={sum(x<-0.5 for x in disc_5d)}/5")
print(f"  10d avg={np.mean(disc_10d):.2f}%, neg_rate={sum(x<-0.5 for x in disc_10d)}/5")

print("\nVALIDATION (2019-2025, n=3):")
val_5d = [r[1] for r in results_5d[5:]]
val_10d = [r[1] for r in results_10d[5:]]
print(f"  5d avg={np.mean(val_5d):.2f}%, neg_rate={sum(x<-0.5 for x in val_5d)}/3")
print(f"  10d avg={np.mean(val_10d):.2f}%, neg_rate={sum(x<-0.5 for x in val_10d)}/3")
for r in results_5d[5:]:
    print(f"    {r[0]} 5d={r[1]:.1f}%")
for r in results_10d[5:]:
    print(f"    {r[0]} 10d={r[1]:.1f}%")

print("\nFULL SET (n=8):")
all_5d = [r[1] for r in results_5d]
all_10d = [r[1] for r in results_10d]
_, p5 = stats.ttest_1samp(all_5d, 0)
_, p10 = stats.ttest_1samp(all_10d, 0)
print(f"  5d avg={np.mean(all_5d):.2f}%, neg_rate={sum(x<-0.5 for x in all_5d)}/8, p={p5:.4f}")
print(f"  10d avg={np.mean(all_10d):.2f}%, neg_rate={sum(x<-0.5 for x in all_10d)}/8, p={p10:.4f}")
