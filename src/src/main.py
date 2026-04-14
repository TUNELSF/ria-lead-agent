import os
import re
import html
import traceback
import requests
import pandas as pd
from bs4 import BeautifulSoup
from urllib.parse import quote_plus, urljoin, urlparse, parse_qs, unquote
from email.utils import parsedate_to_datetime
import xml.etree.ElementTree as ET

SEC_UNIVERSE_PATH = "data/current_universe.csv"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (RIA-Lead-Agent)"
}

REQUEST_TIMEOUT = 15
NEWS_LOOKBACK_DAYS = 180
MAX_ARTICLES_PER_QUERY = 20
MAX_LEADS_TO_PRINT = 15

TEAM_PATHS = ["/team", "/leadership", "/about", "/management"]

DISCOVERY_QUERIES = [
    'RIA crypto',
    '"wealth management" "digital assets"',
    '"advisor" bitcoin ETF',
    '"registered investment adviser" crypto',
    '"wealth advisor" blockchain',
    '"digital assets" "advisor demand"',
    '"wealth management" webinar crypto',
    'alternatives ETF crypto advisors',
]

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
    r"\bbought shares\b",
    r"\bsold shares\b",
    r"\bincreased stake\b",
    r"\breduced position\b",
    r"\betf\b",
    r"\bclient demand\b",
    r"\badvisor demand\b",
    r"\bwebinar\b",
    r"\bpanel\b",
    r"\bconference\b",
]

PORTFOLIO_ACTIVITY_PATTERNS = [
    r"\bbought shares\b",
    r"\bsold shares\b",
    r"\bincreased stake\b",
    r"\breduced position\b",
    r"\betf\b",
    r"\btrust etf\b",
    r"\betha\b",
    r"\bibit\b",
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

CORP_SUFFIXES = [
    "llc", "inc", "l.p.", "lp", "llp", "corp", "corporation", "co.", "co", "ltd", "limited"
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

def sentence_snippet(text, pattern=None):
    text = re.sub(r"\s+", " ", clean(text))
    if not text:
        return ""

    if pattern:
        m = re.search(pattern, text, flags=re.IGNORECASE)
        if m:
            start = max(0, m.start() - 120)
            end = min(len(text), m.end() + 120)
            return text[start:end].strip()[:320]

    return text[:320]

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

def normalize_firm_name(name):
    name = clean(name).lower()
    name = name.replace("&", " and ")
    name = re.sub(r"[^\w\s]", " ", name)
    words = [w for w in name.split() if w not in CORP_SUFFIXES]
    return " ".join(words).strip()

def build_firm_index(df):
    firm_map = {}
    normalized_names = []

    for _, row in df.iterrows():
        norm = normalize_firm_name(row["firm"])
        if norm and norm not in firm_map:
            firm_map[norm] = {
                "firm": row["firm"],
                "website": row["website"],
            }
            normalized_names.append(norm)

    return firm_map, normalized_names

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

    if dt_ts.tzinfo is None:
        dt_ts = dt_ts.tz_localize("UTC")
    else:
        dt_ts = dt_ts.tz_convert("UTC")

    age_days = (now_utc - dt_ts).days
    return age_days <= NEWS_LOOKBACK_DAYS

def build_news_query(query_text):
    return "https://www.bing.com/news/search?q=" + quote_plus(query_text) + "&format=rss"

def extract_real_bing_url(url):
    if not url:
        return url

    parsed = urlparse(url)
    qs = parse_qs(parsed.query)

    if "url" in qs and qs["url"]:
        return unquote(qs["url"][0])

    return url

def parse_news_rss(xml_text):
    items = []
    root = ET.fromstring(xml_text)

    for item in root.findall(".//item"):
        title = item.findtext("title", default="")
        link = item.findtext("link", default="")
        pub_date = item.findtext("pubDate", default="")
        description = item.findtext("description", default="")

        description_html = html.unescape(description)
        soup = BeautifulSoup(description_html, "html.parser")
        description_text = soup.get_text(" ", strip=True)

        real_link = extract_real_bing_url(clean(link))

        items.append({
            "title": clean(title),
            "link": clean(real_link),
            "pub_date": clean(pub_date),
            "description": clean(description_text),
        })

    return items

def detect_priority(blob):
    explicit = any(re.search(pat, blob, re.IGNORECASE) for pat in HIGH_SIGNAL_PATTERNS)
    mediumish = any(re.search(pat, blob, re.IGNORECASE) for pat in MEDIUM_SIGNAL_PATTERNS)
    portfolio = any(re.search(pat, blob, re.IGNORECASE) for pat in PORTFOLIO_ACTIVITY_PATTERNS)

    if explicit:
        if portfolio:
            return "medium", "Portfolio activity involving crypto-related instruments detected in recent media"
        return "high", "Explicit crypto-related language found in recent media coverage"

    if mediumish:
        return "medium", "Adjacent alternatives or portfolio-activity language found in recent media coverage"

    return None, None

def pick_evidence_snippet(blob):
    for pat in HIGH_SIGNAL_PATTERNS + MEDIUM_SIGNAL_PATTERNS:
        if re.search(pat, blob, re.IGNORECASE):
            return sentence_snippet(blob, pat)
    return sentence_snippet(blob)

def match_article_to_sec_firm(blob, firm_map, normalized_names):
    blob_norm = normalize_firm_name(blob)

    # Prefer longer names first to avoid short-name collisions
    for norm_name in sorted(normalized_names, key=len, reverse=True):
        if len(norm_name) < 8:
            continue
        if norm_name in blob_norm:
            return firm_map[norm_name]

    return None

def gather_media_candidates():
    articles = []

    for query in DISCOVERY_QUERIES:
        rss_url = build_news_query(query)
        r = fetch(rss_url)
        if not r or r.status_code != 200:
            continue

        try:
            items = parse_news_rss(r.text)
        except Exception:
            continue

        for item in items[:MAX_ARTICLES_PER_QUERY]:
            if not item["link"]:
                continue
            if not is_recent(item["pub_date"]):
                continue

            blob = f'{item["title"]} {item["description"]}'
            priority, trigger = detect_priority(blob)
            if not priority:
                continue

            articles.append({
                "priority": priority,
                "trigger": trigger,
                "source": item["link"],
                "source_date": item["pub_date"],
                "blob": blob,
                "evidence": pick_evidence_snippet(blob),
                "query": query,
            })

    return articles

def build_hook(priority, trigger, evidence):
    text = f"{trigger} {evidence}".lower()

    if "webinar" in text or "panel" in text or "conference" in text:
        return "Saw the recent event-related signal — curious how you're educating clients or advisors around digital assets."
    if "bought shares" in text or "sold shares" in text or "etf" in text:
        return "Saw the recent ETF or portfolio activity — curious whether digital assets are becoming more relevant in portfolio construction conversations."
    if "client demand" in text or "advisor demand" in text:
        return "Saw the recent demand signal — curious how you're thinking about digital asset access for clients or advisors."
    if priority == "high":
        return "Saw the recent crypto-related signal — curious how you're thinking about digital asset access and implementation for clients."

    return "Saw the recent alternatives-related signal — curious whether digital assets are starting to enter those portfolio conversations."

def build_why_now(priority):
    if priority == "high":
        return "This is recent, explicit crypto or digital-asset language tied to a specific media source."
    return "This is a recent portfolio, ETF, demand, or adjacent alternatives signal that may indicate growing relevance of digital assets."

def dedupe_results(results):
    seen = set()
    out = []

    for r in results:
        key = (r["firm"].lower(), r["source"])
        if key not in seen:
            seen.add(key)
            out.append(r)

    return out

def run():
    df = load_universe()
    print("Firms in SEC universe:", len(df))

    if len(df) == 0:
        print("No firms available after filtering.")
        return

    firm_map, normalized_names = build_firm_index(df)

    print("Searching media...")
    articles = gather_media_candidates()
    print("Media candidates found:", len(articles))

    results = []

    for article in articles:
        match = match_article_to_sec_firm(article["blob"], firm_map, normalized_names)
        if not match:
            continue

        contacts = find_contacts(match["website"])

        results.append({
            "priority": article["priority"],
            "firm": match["firm"],
            "website": match["website"],
            "source": article["source"],
            "source_date": article["source_date"],
            "trigger": article["trigger"],
            "why_now": build_why_now(article["priority"]),
            "evidence": article["evidence"],
            "hook": build_hook(article["priority"], article["trigger"], article["evidence"]),
            "contacts": contacts,
        })

    results = dedupe_results(results)

    print("\n🚀 RIA CRYPTO LEADS — PREVIEW\n")

    if not results:
        print("No strong leads found.")
        return

    results.sort(key=lambda x: (0 if x["priority"] == "high" else 1, x["firm"]))

    for r in results[:MAX_LEADS_TO_PRINT]:
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
