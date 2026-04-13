import os
import re
import html
import random
import traceback
import requests
import pandas as pd
from bs4 import BeautifulSoup
from urllib.parse import urljoin, quote_plus
from email.utils import parsedate_to_datetime
import xml.etree.ElementTree as ET

SEC_UNIVERSE_PATH = "data/current_universe.csv"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (RIA-Lead-Agent)"
}

REQUEST_TIMEOUT = 15
DAILY_SAMPLE_SIZE = 20
NEWS_LOOKBACK_DAYS = 180

SITE_CONTENT_PATHS = ["/news", "/insights", "/blog", "/press", "/events"]
TEAM_PATHS = ["/team", "/leadership", "/about", "/management"]

HIGH_SIGNAL_PATTERNS = [
    r"\bcrypto\b",
    r"\bcryptocurrency\b",
    r"\bdigital asset\b",
    r"\bdigital assets\b",
    r"\bbitcoin\b",
    r"\bethereum\b",
    r"\bblockchain\b",
    r"\btokenization\b",
    r"\bstablecoin\b",
    r"\bbitcoin etf\b",
    r"\bcrypto etf\b",
]

MEDIUM_SIGNAL_PATTERNS = [
    r"\balternative investments\b",
    r"\balternatives\b",
    r"\bprivate markets\b",
    r"\bnew asset classes\b",
]

BAD_DOMAINS = [
    "facebook.com",
    "linkedin.com",
    "instagram.com",
    "twitter.com",
    "x.com",
    "youtube.com",
]

BAD_NAME_WORDS = ["read more", "learn more", "click here", "contact us", "our team"]

GOOD_FIRM_WORDS = [
    "capital",
    "partners",
    "wealth",
    "advisors",
    "management",
    "invest",
    "asset",
]

def clean(x):
    return "" if pd.isna(x) else str(x).strip()

def ensure_url(url):
    url = clean(url)
    if not url:
        return ""
    if re.match(r"^https?://", url, flags=re.IGNORECASE):
        scheme, rest = url.split("://", 1)
        return scheme.lower() + "://" + rest
    return "https://" + url

def is_bad_domain(url):
    return any(d in url.lower() for d in BAD_DOMAINS)

def fetch(url):
    try:
        return requests.get(url, headers=HEADERS, timeout=REQUEST_TIMEOUT, allow_redirects=True)
    except Exception:
        return None

def fetch_html(url):
    r = fetch(url)
    if r and r.status_code == 200 and "text/html" in r.headers.get("content-type", "").lower():
        return r.text
    return None

def html_to_text(html_text):
    soup = BeautifulSoup(html_text, "html.parser")
    for tag in soup(["script", "style", "noscript"]):
        tag.decompose()
    return soup.get_text(" ", strip=True)

def sentence_snippet(text, pattern):
    text = re.sub(r"\s+", " ", clean(text))
    if not text:
        return ""
    m = re.search(pattern, text, flags=re.IGNORECASE)
    if not m:
        return text[:240]
    start = max(0, m.start() - 120)
    end = min(len(text), m.end() + 120)
    return text[start:end].strip()[:320]

def valid_name(x):
    x = clean(x)
    if not x:
        return False
    if any(w in x.lower() for w in BAD_NAME_WORDS):
        return False
    parts = x.split()
    return 1 < len(parts) <= 4 and all(p and p[0].isupper() for p in parts)

def extract_contacts(html_text):
    soup = BeautifulSoup(html_text, "html.parser")
    lines = [l.strip() for l in soup.get_text("\n").split("\n") if l.strip()]

    contacts = []
    for i, line in enumerate(lines):
        ll = line.lower()
        if any(k in ll for k in ["ceo", "chief executive", "cio", "chief investment", "president", "partner", "founder"]):
            for j in range(max(0, i - 3), i):
                if valid_name(lines[j]):
                    contacts.append((lines[j], line))
                    break

    seen = set()
    out = []
    for name, title in contacts:
        if name.lower() not in seen:
            seen.add(name.lower())
            out.append((name, title))

    return out[:2]

def load_universe():
    if not os.path.exists(SEC_UNIVERSE_PATH):
        raise Exception(f"Missing SEC universe file: {SEC_UNIVERSE_PATH}")

    df = pd.read_csv(SEC_UNIVERSE_PATH, encoding="latin1", low_memory=False)

    possible_name_cols = [c for c in df.columns if "business name" in c.lower() or c.lower() == "legal name"]
    possible_website_cols = [c for c in df.columns if "website" in c.lower()]

    if not possible_name_cols:
        raise Exception(f"Could not find firm-name column. First columns: {list(df.columns[:30])}")
    if not possible_website_cols:
        raise Exception(f"Could not find website column. First columns: {list(df.columns[:30])}")

    name_col = possible_name_cols[0]
    web_col = possible_website_cols[0]

    out = df[[name_col, web_col]].copy()
    out.columns = ["firm", "website"]

    out["firm"] = out["firm"].apply(clean)
    out["website"] = out["website"].apply(ensure_url)

    out = out[out["firm"] != ""]
    out = out[out["website"] != ""]
    out = out[~out["website"].apply(is_bad_domain)]
    out = out.drop_duplicates()

    return out

def score_firm(row):
    score = 0
    firm = row["firm"].lower()
    website = row["website"].lower()

    if website.startswith("http://") or website.startswith("https://"):
        score += 2

    if any(w in firm for w in GOOD_FIRM_WORDS):
        score += 2

    if any(w in website for w in ["wealth", "capital", "advis", "invest", "asset"]):
        score += 1

    return score

def select_batch(df):
    df = df.copy()
    df["score"] = df.apply(score_firm, axis=1)
    df = df.sort_values(["score", "firm"], ascending=[False, True])

    top = df.head(min(200, len(df)))
    return top.sample(min(DAILY_SAMPLE_SIZE, len(top)), random_state=42).to_dict("records")

def parse_pubdate(pubdate_text):
    try:
        return parsedate_to_datetime(pubdate_text)
    except Exception:
        return None

def is_recent(pubdate_text):
    dt = parse_pubdate(pubdate_text)
    if dt is None:
        return False

    now_utc = pd.Timestamp.now("UTC")
    dt_ts = pd.Timestamp(dt)

    # If dt is naive, make it UTC. If it's already tz-aware, convert to UTC.
    if dt_ts.tzinfo is None:
        dt_ts = dt_ts.tz_localize("UTC")
    else:
        dt_ts = dt_ts.tz_convert("UTC")

    age_days = (now_utc - dt_ts).days
    return age_days <= NEWS_LOOKBACK_DAYS

def build_news_query(firm_name):
    # tighter query for explicit crypto signals
    query = f'"{firm_name}" (crypto OR "digital assets" OR bitcoin OR ethereum OR blockchain OR tokenization)'
    return "https://news.google.com/rss/search?q=" + quote_plus(query) + "&hl=en-US&gl=US&ceid=US:en"

def parse_google_news_rss(xml_text):
    items = []
    root = ET.fromstring(xml_text)

    for item in root.findall(".//item"):
        title = item.findtext("title", default="")
        link = item.findtext("link", default="")
        pub_date = item.findtext("pubDate", default="")
        description = item.findtext("description", default="")

        description = html.unescape(description)
        description = BeautifulSoup(description, "html.parser").get_text(" ", strip=True)

        items.append({
            "title": clean(title),
            "link": clean(link),
            "pub_date": clean(pub_date),
            "description": clean(description),
        })

    return items

def detect_signal_from_news(firm_name):
    rss_url = build_news_query(firm_name)
    r = fetch(rss_url)
    if not r or r.status_code != 200:
        return None

    try:
        items = parse_google_news_rss(r.text)
    except Exception:
        return None

    candidates = []

    for item in items:
        if not item["link"]:
            continue
        if not is_recent(item["pub_date"]):
            continue

        blob = f'{item["title"]} {item["description"]}'
        for pat in HIGH_SIGNAL_PATTERNS:
            if re.search(pat, blob, re.IGNORECASE):
                candidates.append({
                    "priority": "high",
                    "source": item["link"],
                    "source_date": item["pub_date"],
                    "trigger": "Explicit crypto-related language found in recent media coverage",
                    "evidence": sentence_snippet(blob, pat),
                })

        for pat in MEDIUM_SIGNAL_PATTERNS:
            if re.search(pat, blob, re.IGNORECASE):
                candidates.append({
                    "priority": "medium",
                    "source": item["link"],
                    "source_date": item["pub_date"],
                    "trigger": "Adjacent alternatives language found in recent media coverage",
                    "evidence": sentence_snippet(blob, pat),
                })

    if not candidates:
        return None

    highs = [c for c in candidates if c["priority"] == "high"]
    if highs:
        return highs[0]

    meds = [c for c in candidates if c["priority"] == "medium"]
    if meds:
        return meds[0]

    return None

def looks_article(url, text):
    if any(x in url.lower() for x in ["/news/", "/blog/", "/insights/", "/events/", "/press/"]):
        return True
    if re.search(r"\b20\d{2}\b", text):
        return True
    return False

def score_page(url, text):
    score = 0
    if looks_article(url, text):
        score += 3
    if len(text) > 800:
        score += 1
    if url.count("/") <= 3:
        score -= 2
    return score

def detect_signal_from_site(site):
    pages = []
    for path in SITE_CONTENT_PATHS:
        url = urljoin(site, path)
        html_text = fetch_html(url)
        if html_text:
            pages.append({"url": url, "text": html_to_text(html_text)})

    candidates = []

    for page in pages:
        text = page["text"]
        url = page["url"]
        quality = score_page(url, text)

        for pat in HIGH_SIGNAL_PATTERNS:
            if re.search(pat, text, re.IGNORECASE) and quality >= 2:
                candidates.append({
                    "priority": "high",
                    "source": url,
                    "source_date": None,
                    "trigger": "Explicit crypto-related language found on a firm page",
                    "evidence": sentence_snippet(text, pat),
                })

        for pat in MEDIUM_SIGNAL_PATTERNS:
            if re.search(pat, text, re.IGNORECASE) and quality >= 3:
                candidates.append({
                    "priority": "medium",
                    "source": url,
                    "source_date": None,
                    "trigger": "Adjacent alternatives language found on a firm page",
                    "evidence": sentence_snippet(text, pat),
                })

    if not candidates:
        return None

    highs = [c for c in candidates if c["priority"] == "high"]
    if highs:
        return highs[0]

    meds = [c for c in candidates if c["priority"] == "medium"]
    if meds:
        return meds[0]

    return None

def find_contacts(site):
    contacts = []
    for path in TEAM_PATHS:
        html_text = fetch_html(urljoin(site, path))
        if html_text:
            contacts = extract_contacts(html_text)
            if contacts:
                break

    if not contacts:
        contacts = [("Chief Investment Officer", ""), ("Managing Partner", "")]
    elif len(contacts) == 1:
        contacts.append(("Chief Investment Officer", ""))

    return contacts[:2]

def build_hook(signal):
    if signal["priority"] == "high":
        return "Saw the recent crypto-related signal — curious how you're thinking about digital asset access and implementation for clients."
    return "Noticed the recent alternatives-related signal — curious whether digital assets are starting to enter those portfolio conversations."

def build_why_now(signal):
    if signal["priority"] == "high":
        return "This is recent, explicit crypto or digital-asset language tied to a specific media or firm source."
    return "This is a recent adjacent signal, though it is not yet explicit crypto language."

def run():
    df = load_universe()
    print("Firms:", len(df))

    if len(df) == 0:
        print("No firms available after filtering.")
        return

    batch = select_batch(df)

    results = []

    for firm in batch:
        firm_name = firm["firm"]
        site = firm["website"]
        print("Scanning:", firm_name)

        # 1) media-first
        signal = detect_signal_from_news(firm_name)

        # 2) fallback to site only if no media hit
        if signal is None:
            signal = detect_signal_from_site(site)

        if signal is None:
            continue

        contacts = find_contacts(site)

        results.append({
            "priority": signal["priority"],
            "firm": firm_name,
            "source": signal["source"],
            "source_date": signal["source_date"],
            "trigger": signal["trigger"],
            "why_now": build_why_now(signal),
            "evidence": signal["evidence"],
            "hook": build_hook(signal),
            "contacts": contacts,
        })

    print("\n🚀 RIA CRYPTO LEADS — PREVIEW\n")

    if not results:
        print("No strong leads found.")
        return

    # High first
    results.sort(key=lambda x: 0 if x["priority"] == "high" else 1)

    for r in results:
        print(f"\n{'🔥 HIGH' if r['priority']=='high' else '🟡 MED'}")
        print(r["firm"])
        print("Trigger:", r["trigger"])
        print("Why now:", r["why_now"])
        print("Source:", r["source"])
        if r["source_date"]:
            print("Source date:", r["source_date"])
        print("Evidence:", r["evidence"])
        print("Hook:", r["hook"])
        print("Contacts:")
        print("-", r["contacts"][0][0])
        print("-", r["contacts"][1][0] if len(r["contacts"]) > 1 else r["contacts"][0][0])
        print("-----")

if __name__ == "__main__":
    try:
        run()
    except Exception:
        traceback.print_exc()
        raise
