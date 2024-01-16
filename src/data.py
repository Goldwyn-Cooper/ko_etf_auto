# 표준
from datetime import datetime as dt
from dateutil.relativedelta import relativedelta as rd
import ast
# 서드파티
import pandas as pd
import requests

pd.options.display.float_format = '{:.3f}'.format

MARKETCAP='https://finance.naver.com/api/sise/etfItemList.nhn?etfType=0'
PRICE='https://api.finance.naver.com/siseJson.naver'

def get_marketcap_from_naver() -> pd.DataFrame:
    response = requests.get(MARKETCAP) 
    cols = ('itemcode', 'etfTabCode', 'itemname', 'amonut', 'quant', 'marketSum')
    data = response.json().get('result').get('etfItemList')
    df = pd.DataFrame(data).dropna().loc[:, cols]
    kwds_pre = '레버리지|2X'\
            +'|HANARO|KOSEF|SOL|ARIRANG'
    df.query(f'not itemname.str.contains("{kwds_pre}")',
             inplace=True)
    df['category_marketSum_median'] = df['etfTabCode'].apply(
        lambda x: df[df.etfTabCode == x].marketSum.median())
    df['category_amonut_median'] = df['etfTabCode'].apply(
        lambda x: df[df.etfTabCode == x].amonut.median())
    df['category_quant_mean'] = df['etfTabCode'].apply(
        lambda x: df[df.etfTabCode == x].quant.mean())
    kwds = '액티브|혼합|합성|TR|콜'\
            +'|3년|10년|단기|장기|금리'\
            +'|배당|은행|증권|리츠'\
            +'|BBIG|ESG|삼성|메타버스|소재|\sTOP10$'
    df.query(f'not itemname.str.contains("{kwds}")\
               or itemname.str.contains("엔선물|달러선물|TOP10인버스")',
               inplace=True)
    expr = f'marketSum >= category_marketSum_median\
            and ((amonut >= category_amonut_median\
            and (quant >= category_quant_mean or etfTabCode == 3))\
            or etfTabCode == 7)\
            and etfTabCode not in [1, 4]'
    return df.query(expr).loc[:, ['itemcode', 'itemname']].reset_index(drop=True)

def get_price(symbol):
    td = dt.utcnow() + rd(hours=9)
    year_1 = td - rd(years=1)
    literal = requests.get(PRICE, params=dict(
        symbol=symbol,
        requestType=1,
        startTime=year_1.strftime('%Y%m%d'),
        endTime='20991231',
        timeframe='day'
    )).text.replace('\n', '').replace('\t', '')
    data = ast.literal_eval(literal)
    df = pd.DataFrame(data[1:], columns=data[0]).loc[:, ('고가', '저가', '종가')]
    df.columns = ['high', 'low', 'close']
    return df

if __name__ == '__main__':
    print(get_marketcap_from_naver())