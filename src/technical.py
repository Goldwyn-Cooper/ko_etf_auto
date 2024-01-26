import pandas as pd
from sklearn.mixture import GaussianMixture

FIBO = (3, 5, 8, 13, 21)
# FIBO = (2, 3, 5, 8, 13)


def momentum(close: pd.Series, period: int):
    t = close.tail(period)
    return (t.iloc[-1] / t.iloc[0] - 1) / period


def tr(df: pd.DataFrame):
    concat = lambda *x: pd.concat(x, axis=1)
    th = concat(df.high, df.close.shift(1)).max(axis=1)
    tl = concat(df.low, df.close.shift(1)).min(axis=1)
    return th - tl


def atr(s: pd.Series, period: int) -> float:
    return s.ewm(period).mean().iloc[-1]


def correlation(volatility: dict):
    v = pd.concat(volatility.values(), axis=1)
    v.columns = list(volatility.keys())
    return v.corr()


def clustering(corr: pd.DataFrame, cnt: int):
    gmm = GaussianMixture(n_components=cnt * 2, random_state=42)
    gmm.fit(corr)
    cluster_labels = gmm.predict(corr)
    return pd.DataFrame({"symbol": corr.columns, "group": cluster_labels + 1})
