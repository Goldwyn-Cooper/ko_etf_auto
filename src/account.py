# 표준
import os
from datetime import datetime as dt
from dateutil.relativedelta import relativedelta as rd
# 서드파티
import requests
import pandas as pd
# 커스텀
from src.telegram import send_message
from src.data import get_price

KIS_DOMAIN='https://openapi.koreainvestment.com:9443'
KIS_TRADING='/uapi/domestic-stock/v1/trading'
KIS_ACCOUNT_BALACNE_COL = (
    '매입금액', '평가금액', '평가손익금액', '신용대출금액', '실제순자산금액', '전체비중율')
KIS_ACCOUNT_BALACNE_ROW = (
    '주식', '펀드/MMW', '채권', 'ELS/DLS', 'WRAP', '신탁/퇴직연금/외화신탁',
    'RP/발행어음', '해외주식', '해외채권', '금현물', 'CD/CP', '단기사채', '타사상품',
    '외화단기사채', '외화 ELS/DLS', '외화', '예수금+CMA', '청약자예수금')
KIS_BALANCE_COL_RAW = ('pdno', 'prdt_name', 'hldg_qty', 'evlu_pfls_rt')

appkey = os.getenv('KIS_AK')
appsecret = os.getenv('KIS_SK')
cano = os.getenv('KIS_CANO')
token_storage = os.getenv('KIS_TOKEN_STORAGE')

def get_token_from_server() -> tuple:
    payload = dict(grant_type='client_credentials',
                    appkey=appkey,
                    appsecret=appsecret)
    json = requests.post(KIS_DOMAIN + '/oauth2/tokenP',
                         json=payload).json()
    return (json.get('access_token'),
            json.get('access_token_token_expired'))

def save_token(token: str, expired: str) -> tuple:
    payload = dict(
        cano={'S': cano},
        token={'S': token},
        expired={'S': expired})
    requests.post(token_storage, json=payload)

def check_expired_token(expired):
    dt_fmt = '%Y-%m-%d %H:%M:%S'
    now = dt.utcnow() + rd(hours=9)
    expired_dt = dt.strptime(expired, dt_fmt) - rd(hours=1)
    return expired_dt < now

def get_token() -> str:
    params = dict(cano=cano)
    json = requests.get(token_storage, params=params).json()
    item : dict = json.get('Item', {})
    get_string = lambda x, y: x.get(y, {}).get('S')
    try:
        token = get_string(item, 'token')
        if not token:
            raise Exception('저장된 토큰 없음')
        expired = get_string(item, 'expired')
        if check_expired_token(expired):
            raise Exception('만료된 토큰')
    except Exception as e:
        send_message(e)
        token, expired = get_token_from_server()
        save_token(token, expired)
    send_message('토큰 검증 완료')
    return token

def get_headers(token, tr_id):
    return {'content-type': 'application/json; charset=utf-8',
            'authorization': f'Bearer {token}',
            'appkey': appkey,
            'appsecret': appsecret,
            'custtype': 'P',
            'tr_id': tr_id}
    
def get_params():
    return dict(CANO=cano,
                ACNT_PRDT_CD='01',
                INQR_DVSN_1='',
                BSPR_BF_DT_APLY_YN='')
    

def get_account_balance(token):
    PATH = KIS_TRADING + '/inquire-account-balance'
    json = requests.get(KIS_DOMAIN + PATH,
            params=get_params(),
            headers=get_headers(token, 'CTRP6548R')).json()
    df = pd.DataFrame(json.get('output1')[:-1])
    df.columns=KIS_ACCOUNT_BALACNE_COL
    df.index=KIS_ACCOUNT_BALACNE_ROW
    send_message(f'예수금 : ₩{int(df.loc["예수금+CMA", "평가금액"]):,}')
    send_message(f'RP : ₩{int(df.loc["RP/발행어음", "평가금액"]):,}')
    return df.loc[:, "평가금액"].drop('채권').astype(int).sum()

def get_balance(token):
    PATH = KIS_TRADING +  '/inquire-balance'
    params = get_params()
    params['AFHR_FLPR_YN'] = 'N'
    params['OFL_YN'] = ''
    params['INQR_DVSN'] = '02'
    params['UNPR_DVSN'] = '01'
    params['FUND_STTL_ICLD_YN'] = 'N'
    params['FNCG_AMT_AUTO_RDPT_YN'] = 'N'
    params['PRCS_DVSN'] = '00'
    params['CTX_AREA_FK100'] = ''
    params['CTX_AREA_NK100'] = ''
    json = requests.get(KIS_DOMAIN + PATH,
        params=params,
        headers=get_headers(token, 'TTTC8434R')).json()
    df = pd.DataFrame(json.get('output1'),
        columns=KIS_BALANCE_COL_RAW)\
        .astype({'hldg_qty': int, 'evlu_pfls_rt': float}).query('hldg_qty > 0')
    df.columns = ['상품번호', '상품명', '보유수량', '평가손익률']
    return df.set_index('상품번호')

def trading_payload(pdno: str, qty: int):
    return {'CANO': cano, 'PDNO': pdno, 'ACNT_PRDT_CD': '01',
            'ORD_DVSN': '01', 'ORD_QTY': f'{qty}', 'ORD_UNPR': '0'}

def sell_market_order(symbol, qty, token):
    send_message(f'🐻 매도 ({symbol})')
    send_message(f'📌 매도량 {qty:,}')
    json = requests.post(KIS_DOMAIN + KIS_TRADING + '/order-cash',
            json=trading_payload(symbol, qty),
            headers=get_headers(token, 'TTTC0801U')).json()
    msg_cd, msg = json.get('msg_cd'), json.get('msg1')
    send_message((f'🚀 {msg}' if msg_cd == 'APBK0013' else f'😱 {msg_cd} : {msg}'))

def buy_market_order(symbol, amt, price, token):
    send_message(f'🐮 매수 ({symbol})')
    send_message(f'📌 매수단가 ₩{price:,}')
    send_message(f'📌 매수금액 ₩{amt:,}')
    json = requests.post(KIS_DOMAIN + KIS_TRADING + '/order-cash',
            json=trading_payload(symbol, int(amt // price)),
            headers=get_headers(token, 'TTTC0802U')).json()
    msg_cd, msg = json.get('msg_cd'), json.get('msg1')
    send_message((f'🚀 {msg}' if msg_cd == 'APBK0013' else f'😱 {msg_cd} : {msg}'))

def exit_position(balance: pd.DataFrame, candidate: pd.DataFrame, token: str):
    exit_table = balance.join(
        candidate, how='left')\
        .query('risk.isna()').loc[:,['상품명', '보유수량']]
    if len(exit_table):
        send_message('[EXIT]')
        send_message(exit_table)
    for r in exit_table.itertuples():
        send_message(f'매도 주문 : {r.Index}')
        sell_market_order(r.Index, r.보유수량, token)

def enter_position(balance: pd.DataFrame, candidate: pd.DataFrame, budget:int, token: str):
    enter_table = balance.join(candidate, how='right')\
        .query('보유수량.isna()').loc[:,['itemname', 'risk']]
    if len(enter_table):
        send_message('[ENTER]')
        send_message(enter_table)
    for r in enter_table.itertuples():
        send_message(f'매수 주문 : {r.Index}')
        price = get_price(r).close.iloc[-1]
        buy_market_order(r.Index, int(r.risk * budget), price, token)