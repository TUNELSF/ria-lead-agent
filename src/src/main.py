import requests
from bs4 import BeautifulSoup
import random
import os

TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
CHAT_ID = os.getenv("TELEGRAM_CHANNEL_ID")

def get_sample_firms():
    return [
        {"name": "Wealth Partners Group", "website": "https://example.com"},
        {"name": "Alpha Advisors", "website": "https://example.com"},
        {"name": "Summit Financial", "website": "https://example.com"}
    ]

def detect_signal(text):
    keywords = ["hiring", "webinar", "expanding", "join", "partner"]

    matches = [k for k in keywords if k in text.lower()]

    if len(matches) >= 2:
        return "moderate"
    elif len(matches) == 1:
        return "weak"
    return None

def send_telegram(msg):
    url = f"https://api.telegram.org/bot{TOKEN}/sendMessage"
    requests.post(url, json={"chat_id": CHAT_ID, "text": msg})

def run():
    firms = get_sample_firms()

    for firm in firms:
        text = "We are hiring and expanding our team"  # fake signal for now

        signal = detect_signal(text)

        if signal:
            msg = f"🚀 RIA Lead\n\nFirm: {firm['name']}\nSignal: {signal}"
            send_telegram(msg)

if __name__ == "__main__":
    run()
