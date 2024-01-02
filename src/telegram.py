# 표준
import os
# 서드파티
import requests
import pandas as pd

def send_message(text):
    bot_token = os.getenv('TELEGRAM_BOT_TOKEN')
    URL = 'https://api.telegram.org/bot' + bot_token
    chat_id = os.getenv('TELEGRAM_BOT_CHAT_ID')
    if type(text) == pd.DataFrame:
        text = text.reset_index().to_string(index=False)
    requests.post(URL + '/sendMessage', dict(
        chat_id=chat_id,
        text=text))