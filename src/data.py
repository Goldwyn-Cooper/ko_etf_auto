# 표준
from datetime import datetime as dt
from dateutil.relativedelta import relativedelta as rd
import ast

# 서드파티
import pandas as pd
import requests

pd.options.display.float_format = "{:.3f}".format

MARKETCAP = "https://finance.naver.com/api/sise/etfItemList.nhn?etfType=0"
PRICE = "https://api.finance.naver.com/siseJson.naver"

def get_marketcap_from_naver() -> pd.DataFrame:
    response = requests.get(MARKETCAP)
    cols = ("itemcode", "etfTabCode", "itemname", "amonut", "quant", "marketSum")
    data = response.json().get("result").get("etfItemList")
    df = pd.DataFrame(data).dropna().loc[:, cols]
    include_itemcode = "465350|304660|385560|411060|114800|251340"\
        + "|069500|229200|379800|379810"
    # 시가총액 상위 50% 이상, 거래량 평균 이상
    df["category_marketSum"] = df["etfTabCode"].apply(
        lambda x: df[df.etfTabCode == x].marketSum.median())
    df["category_quant"] = df["etfTabCode"].apply(
        lambda x: df[df.etfTabCode == x].quant.mean())
    # 쿼리 적용
    df.query(f'(itemname.str.contains("KODEX|TIGER")\
        and not itemname.str.contains("배당|은행|리츠|소재")\
        and not itemname.str.contains("글로벌|차이나|인도|일본|닥100|P500")\
        and not itemname.str.contains("액티브|\(합성\)|선물|\(H\)|레버리지|2X")\
        and marketSum >= category_marketSum\
            and quant >= category_quant\
            and etfTabCode in [2, 4])\
            or itemcode.str.contains("{include_itemcode}")', inplace=True)
    return df.loc[:, ["itemcode", "itemname"]].reset_index(drop=True)

def get_price(symbol):
    td = dt.utcnow() + rd(hours=9)
    year_1 = td - rd(years=1)
    literal = (
        requests.get(
            PRICE,
            params=dict(
                symbol=symbol,
                requestType=1,
                startTime=year_1.strftime("%Y%m%d"),
                endTime="20991231",
                timeframe="day",
            ),
        )
        .text.replace("\n", "")
        .replace("\t", "")
    )
    data = ast.literal_eval(literal)
    df = pd.DataFrame(data[1:], columns=data[0]).loc[:, ("고가", "저가", "종가")]
    df.columns = ["high", "low", "close"]
    return df


if __name__ == "__main__":
    print(get_marketcap_from_naver())
