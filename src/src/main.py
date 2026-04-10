import requests
from bs4 import BeautifulSoup
import os

TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
CHAT_ID = os.getenv("TELEGRAM_CHANNEL_ID")

FIRMS = [
    {"name": "Creative Planning", "website": "https://creativeplanning.com"},
    {"name": "Mariner Wealth Advisors", "website": "https://marinerwealthadvisors.com"},
    {"name": "Mercer Advisors", "website": "https://merceradvisors.com"}
]

KEYWORDS = ["hiring", "join", "expanding", "webinar", "event", "partner"]

def scrape_text(url):
    try:
        r = requests.get(url, timeout=10)
        soup = BeautifulSoup(r.text, "html.parser")
        return soup.get_text(" ", strip=True).lower()
    except:
        return ""

def detect_signal(text):
    matches = [k for k in KEYWORDS if k in text]

    if len(matches) >= 3:
        return "strong"
    elif len(matches) >= 1:
        return "moderate"
    return None

def send_telegram(msg):
    url = f"https://api.telegram.org/bot{TOKEN}/sendMessage"
    requests.post(url, json={"chat_id": CHAT_ID, "text": msg})

def run():
    for firm in FIRMS:
        text = scrape_text(firm["website"])

        if not text:
            continue

        signal = detect_signal(text)

        if signal:
            msg = f"""
🚀 RIA Lead

Firm: {firm['name']}
Signal: {signal}
Website: {firm['website']}
"""
            send_telegram(msg)

if __name__ == "__main__":
    run()
