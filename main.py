from src.telegram import send_message
from src.data import get_price, get_marketcap_from_naver
from src.technical import FIBO, momentum, tr, atr, correlation, clustering
from src.account import get_token, get_account_balance, get_balance, exit_position, enter_position

def get_candidate():
    mc = get_marketcap_from_naver()
    # send_message(', '.join(mc.itemname.to_list()))
    prices = {}
    volatility = {}
    for s in mc.itemcode:
        p = get_price(s)
        if len(p) < FIBO[-1]:
            continue
        prices[s] = p
        volatility[s] = tr(p)
    corr = correlation(volatility)
    cluster = clustering(corr)
    scoring = lambda x: sum([momentum(prices[x].close, f) for f in FIBO]) / len(FIBO)
    cluster['momentum'] = cluster.symbol.apply(scoring) * 252
    risk = lambda x: min(1, 0.01 / (atr(volatility[x], max(FIBO)) / prices[x].close.iloc[-1])) / 4
    cluster['risk'] = cluster.symbol.apply(risk)
    send_message(
        cluster.merge(mc, left_on='symbol', right_on='itemcode')            
            .loc[:, ('group', 'itemname')]
            .sort_values(['group'])
            .set_index('group'))
    return cluster\
                .loc[(cluster.groupby('group')['momentum'].idxmax())]\
                .query('momentum > 0')\
                .sort_values('momentum', ascending=False)\
                .merge(mc, left_on='symbol', right_on='itemcode')\
                .head(4).set_index('symbol').drop('itemcode', axis=1)

# if __name__ == '__main__':
#     candidate = get_candidate()
#     print(candidate.set_index('itemname'))

if __name__ == '__main__':
    send_message('🚀')
    send_message('[TOKEN]')
    token = get_token()
    budget = get_account_balance(token)
    balance = get_balance(token)
    send_message('[BALANCE]')
    send_message(f'보유자산 : ₩{int(budget):,}')
    if len(balance):
        send_message(balance)
    send_message('[CANDIDATE]')
    candidate = get_candidate()
    send_message(candidate.set_index('itemname'))
    exit_position(balance, candidate, token)
    enter_position(balance, candidate, budget, token)