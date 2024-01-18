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
    # 시가총액 상위 50% 이상, 거래금액 & 거래량 평균 이상
    df["category_marketSum_median"] = df["etfTabCode"].apply(
        lambda x: df[df.etfTabCode == x].marketSum.median()
    )
    df["category_amonut_mean"] = df["etfTabCode"].apply(
        lambda x: df[df.etfTabCode == x].amonut.mean()
    )
    df["category_quant_mean"] = df["etfTabCode"].apply(
        lambda x: df[df.etfTabCode == x].quant.mean()
    )
    include_itemcode = "465350|304660|385560|261240|261270|292560"
    exclude_itemcode = "465330|292150"
    expr = f'(marketSum >= category_marketSum_median\
             and amonut >= category_amonut_mean\
             and quant >= category_quant_mean)\
             or itemcode.str.contains("{include_itemcode}")'
    df.query(expr, inplace=True)
    kwd1 = "2X|레버리지|TDF|TRF|혼합|금리|배당|리츠"
    kwd2 = "SOL|KoAct|TIMEFOLIO|KOSEF|HANARO|ARIRANG"
    kwd3 = "글로벌|미국|일본|차이나|베트남|인도|밸류|포커스|소재"
    expr = f'(not itemname.str.contains("{kwd1}")\
            and not itemname.str.contains("{kwd2}")\
            and not itemname.str.contains("{kwd3}")\
            and not itemcode.str.contains("{exclude_itemcode}")\
            and not etfTabCode in [1, 6]) or itemcode.str.contains("{include_itemcode}")'
    df.query(expr, inplace=True)
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
