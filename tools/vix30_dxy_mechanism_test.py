"""Mechanism test: does DXY fall after VIX>30 first-close (30d cluster)?

Tests whether the validated XLB/EEM outperformance mechanism is dollar-driven.
"""
import yfinance as yf
import pandas as pd
import numpy as np
from scipy import stats


def fetch_close(ticker, start, end):
    df = yf.download(ticker, start=start, end=end, progress=False, auto_adjust=False)
    df.columns = [c[0] if isinstance(c, tuple) else c for c in df.columns]
    return df['Close']


def identify_vix30_first_close(vix_close, cluster_days=30):
    events = []
    last_event = None
    for dt, v in vix_close.items():
        if v > 30:
            if last_event is None or (dt - last_event).days > cluster_days:
                events.append(dt)
            last_event = dt
    return events


def measure_forward_returns(series, event_dates, windows=(1, 5, 10, 20)):
    """Measure forward returns from t+1 open... but since we have close data,
    use t close to t+N close return."""
    results = {w: [] for w in windows}
    details = []
    for ev in event_dates:
        try:
            i = series.index.get_loc(ev)
        except KeyError:
            continue
        row = {'event': ev.date()}
        for w in windows:
            if i + w >= len(series):
                row[f'{w}d'] = None
                continue
            ret = (series.iloc[i + w] / series.iloc[i] - 1) * 100
            results[w].append(ret)
            row[f'{w}d'] = ret
        details.append(row)
    return results, details


def stats_summary(rets, label):
    if len(rets) < 2:
        return f'{label}: n<2'
    rets = np.array(rets)
    t, p = stats.ttest_1samp(rets, 0)
    direction_down = (rets < 0).mean()
    return (f'{label}: n={len(rets)} mean={rets.mean():+.2f}% median={np.median(rets):+.2f}% '
            f'p={p:.3f} pos_rate={(rets > 0).mean()*100:.0f}% '
            f'neg_rate={direction_down*100:.0f}%')


def main():
    vix = fetch_close('^VIX', '2010-01-01', '2026-04-17')
    dxy = fetch_close('DX-Y.NYB', '2010-01-01', '2026-04-17')
    spy = fetch_close('SPY', '2010-01-01', '2026-04-17')

    events = identify_vix30_first_close(vix)
    print(f'VIX>30 first-close events (30d cluster): {len(events)}\n')

    # DXY forward returns
    dxy_rets, dxy_detail = measure_forward_returns(dxy, events)
    spy_rets, spy_detail = measure_forward_returns(spy, events)

    print('=== DXY FORWARD RETURNS (full sample, n=16) ===')
    for w in (1, 5, 10, 20):
        print(f'  {stats_summary(dxy_rets[w], f"DXY {w}d")}')

    print('\n=== SPY FORWARD RETURNS (comparison, full sample) ===')
    for w in (1, 5, 10, 20):
        print(f'  {stats_summary(spy_rets[w], f"SPY {w}d")}')

    # OOS split
    pre_2022 = [i for i, ev in enumerate(events) if ev.year < 2022]
    post_2022 = [i for i, ev in enumerate(events) if ev.year >= 2022]

    print(f'\n=== OOS SPLIT (pre-2022 n={len(pre_2022)}, post-2022 n={len(post_2022)}) ===')
    for w in (5, 10, 20):
        pre_dxy = [dxy_rets[w][i] for i in pre_2022 if i < len(dxy_rets[w])]
        post_dxy = [dxy_rets[w][i] for i in post_2022 if i < len(dxy_rets[w])]
        print(f'  DXY {w}d pre-2022: {stats_summary(pre_dxy, "")}'.replace(': ', ''))
        print(f'  DXY {w}d post-2022: {stats_summary(post_dxy, "")}'.replace(': ', ''))
        print()

    # Per-event detail
    print('\n=== PER-EVENT DXY AND SPY RETURNS ===')
    print(f'{"Event":<12} {"DXY 5d":>9} {"DXY 10d":>9} {"DXY 20d":>9}  {"SPY 5d":>9} {"SPY 10d":>9} {"SPY 20d":>9}')
    for dd, ss in zip(dxy_detail, spy_detail):
        dxy5 = f'{dd["5d"]:+.2f}%' if dd.get('5d') is not None else 'N/A'
        dxy10 = f'{dd["10d"]:+.2f}%' if dd.get('10d') is not None else 'N/A'
        dxy20 = f'{dd["20d"]:+.2f}%' if dd.get('20d') is not None else 'N/A'
        spy5 = f'{ss["5d"]:+.2f}%' if ss.get('5d') is not None else 'N/A'
        spy10 = f'{ss["10d"]:+.2f}%' if ss.get('10d') is not None else 'N/A'
        spy20 = f'{ss["20d"]:+.2f}%' if ss.get('20d') is not None else 'N/A'
        print(f'{str(dd["event"]):<12} {dxy5:>9} {dxy10:>9} {dxy20:>9}  {spy5:>9} {spy10:>9} {spy20:>9}')

    # Correlation check: does DXY decline align with SPY/XLB/EEM rally?
    print('\n=== CORRELATION: DXY 20d return vs SPY 20d return ===')
    dxy_arr = np.array([d for d in dxy_rets[20] if d is not None])
    spy_arr = np.array([s for s in spy_rets[20] if s is not None])
    n = min(len(dxy_arr), len(spy_arr))
    if n > 2:
        r, p = stats.pearsonr(dxy_arr[:n], spy_arr[:n])
        print(f'  Pearson r = {r:.3f}, p={p:.3f}, n={n}')
        print(f'  (Expect negative: weaker DXY -> stronger SPY)')

    # Check XLB and EEM specifically for mechanism
    print('\n=== CROSS-CHECK: XLB and EEM 10d/20d returns at same events ===')
    xlb = fetch_close('XLB', '2010-01-01', '2026-04-17')
    eem = fetch_close('EEM', '2010-01-01', '2026-04-17')
    xlb_rets, _ = measure_forward_returns(xlb, events)
    eem_rets, _ = measure_forward_returns(eem, events)
    print(f'  XLB 20d: {stats_summary(xlb_rets[20], "")}'.replace(': ', ''))
    print(f'  EEM 10d: {stats_summary(eem_rets[10], "")}'.replace(': ', ''))

    # Primary test pass/fail
    print('\n=== PRIMARY TEST (success criteria) ===')
    dxy_10d = np.array(dxy_rets[10])
    dxy_20d = np.array(dxy_rets[20])
    t10, p10 = stats.ttest_1samp(dxy_10d, 0)
    t20, p20 = stats.ttest_1samp(dxy_20d, 0)
    crit_10 = dxy_10d.mean() < -0.5 and p10 < 0.05
    crit_20 = dxy_20d.mean() < -1.0 and p20 < 0.05
    direction_down_10 = (dxy_10d < 0).mean() >= 0.60
    direction_down_20 = (dxy_20d < 0).mean() >= 0.60
    print(f'  10d mean<-0.5% AND p<0.05: {crit_10} (mean={dxy_10d.mean():+.2f}%, p={p10:.3f})')
    print(f'  20d mean<-1.0% AND p<0.05: {crit_20} (mean={dxy_20d.mean():+.2f}%, p={p20:.3f})')
    print(f'  Direction >=60% down 10d: {direction_down_10} ({(dxy_10d<0).mean()*100:.0f}%)')
    print(f'  Direction >=60% down 20d: {direction_down_20} ({(dxy_20d<0).mean()*100:.0f}%)')
    print(f'  OVERALL: {"PASSED" if (crit_10 or crit_20) and (direction_down_10 or direction_down_20) else "FAILED"}')


if __name__ == '__main__':
    main()
