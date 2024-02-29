# 표준
import os
import ast
# 서드파티
import requests
import pandas as pd

prices = {}
periods = [2, 3, 5, 8, 13, 21, 34, 55]


def get_price(symbol):
    if symbol in prices:
        return prices.get(symbol)
    url = "https://api.finance.naver.com/siseJson.naver"
    params = dict(
        symbol=symbol,
        requestType=1,
        startTime="20010101",
        endTime="20991201",
        timeframe="day",
    )
    r = requests.get(url, params=params)
    text = r.text.strip().replace("\n", "").replace("\t", "")
    data = ast.literal_eval(text)
    df = pd.DataFrame(data[1:], columns=data[0]).loc[:, ["날짜", "고가", "저가", "종가"]]
    df.set_index("날짜", inplace=True)
    df.index = pd.to_datetime(df.index)
    prices[symbol] = df
    return df


def get_volatility(df: pd.DataFrame, period: int):
    th = pd.concat([df.고가, df.종가.shift(1)], axis=1).max(axis=1)
    tl = pd.concat([df.저가, df.종가.shift(1)], axis=1).max(axis=1)
    tr = th - tl
    return tr.ewm(period).mean().iloc[-1] / tl.iloc[-1]


def get_score(df: pd.DataFrame, periods: list):
    score = 0
    for p in periods:
        # score += df.종가.rolling(p).apply(lambda x: x.iloc[-1] / x.iloc[0] - 1)\
        #     .div(p).iloc[-1]
        score += df.종가.rolling(p).apply(lambda x: x.iloc[-1] / x.iloc[0] - 1).iloc[-1]
    return score / len(periods) * 252


def get_table():
    id = os.environ.get("GOOGLE_SHEET_ID")
    url = f"https://docs.google.com/spreadsheets/d/{id}/export?format=csv"
    candidate = pd.read_csv(url, dtype=str)
    candidate["변동성"] = [
        get_volatility(get_price(c), periods[-1]) for c in candidate.종목코드
    ]
    candidate["변동성2"] = (candidate["변동성"] / candidate["변동성"].quantile(.25)
                         ).apply(lambda x: 1 if x <= 1 else x)
    candidate["모멘텀"] = [get_score(get_price(c), periods) for c in candidate.종목코드]
    # candidate["모멘텀2"] = candidate["모멘텀"] / candidate["변동성2"]
    candidate["비중"] = 1 / candidate["분류"].nunique() / candidate["변동성2"]
    # print(candidate)
    # return candidate.query("모멘텀2 > 0")\
    return candidate.query("모멘텀 > 0")\
        .sort_values(by=["분류", "모멘텀"], ascending=[True, False])\
        .groupby("분류")\
        .head(1)\
        .loc[:, ["종목명", "종목코드", "분류", "비중"]]\
        .reset_index(drop=True)

if __name__ == "__main__":
    t = get_table()
    print(t)
    print(t.비중.sum())
