from src.telegram import send_message
# from src.data import get_price, get_marketcap_from_naver
# from src.technical import FIBO, momentum, tr, atr, correlation, clustering
from src.table import get_table
from src.account import (
    get_token,
    get_account_balance,
    get_balance,
    exit_position,
    enter_position,
)


if __name__ == "__main__":
    send_message("[TOKEN]")
    token = get_token()
    budget = get_account_balance(token)
    balance = get_balance(token)
    send_message("[BALANCE]")
    send_message(f"보유자산 : ₩{int(budget):,}")
    if len(balance):
        send_message(balance)
    send_message("[CANDIDATE]")
    table = get_table().set_index("종목코드")
    send_message(table)
    exit_position(balance, table, token)
    enter_position(balance, table, budget, token)
