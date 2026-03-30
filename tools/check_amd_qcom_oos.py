"""Verify AMD+QCOM basket OOS returns for 3 validation events."""
import sys
sys.path.insert(0, '.')
import pandas as pd
from tools.yfinance_utils import get_close_prices

oos_events = ['2019-05-10', '2025-03-26', '2025-04-02']
tickers = ['AMD', 'QCOM', 'SPY']

close = get_close_prices(tickers, start='2019-04-01', end='2025-04-30')
print("Data shape:", close.shape, "Columns:", list(close.columns))

for event_str in oos_events:
    event = pd.Timestamp(event_str)
    if event not in close.index:
        idx = close.index.searchsorted(event)
        if idx >= len(close.index):
            print(f'{event_str}: NOT FOUND')
            continue
        event = close.index[idx]
    
    for horizon in [5, 10]:
        exit_idx = close.index.searchsorted(event) + horizon
        if exit_idx >= len(close.index):
            continue
        exit_date = close.index[exit_idx]
        
        amd_ret = (close.loc[exit_date, 'AMD'] - close.loc[event, 'AMD']) / close.loc[event, 'AMD'] * 100
        qcom_ret = (close.loc[exit_date, 'QCOM'] - close.loc[event, 'QCOM']) / close.loc[event, 'QCOM'] * 100
        spy_ret = (close.loc[exit_date, 'SPY'] - close.loc[event, 'SPY']) / close.loc[event, 'SPY'] * 100
        basket_abn = (amd_ret + qcom_ret) / 2 - spy_ret
        
        direction = 'CORRECT (neg)' if basket_abn < -0.5 else 'WRONG'
        print(f'{event_str} {horizon}d: basket_abn={basket_abn:.1f}% amd_abn={amd_ret-spy_ret:.1f}% qcom_abn={qcom_ret-spy_ret:.1f}% | {direction}')
