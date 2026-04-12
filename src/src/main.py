import re
import random
import requests
import pandas as pd
from bs4 import BeautifulSoup
from urllib.parse import urljoin
import zipfile
import io
import traceback

SEC_PAGE_URL = "https://www.sec.gov/data-research/sec-markets-data/information-about-registered-investment-advisers-exempt-reporting-advisers"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (RIA-Lead-Agent)"
}

REQUEST_TIMEOUT = 15

DAILY_TIER1_LIMIT = 10
DAILY_TIER2_LIMIT = 5

CONTENT_PATHS = ["/news", "/insights", "/blog", "/press", "/events"]
TEAM_PATHS = ["/team", "/leadership"]

HIGH_SIGNAL_PATTERNS = [
    r"\bcrypto\b",
    r"\bdigital asset",
    r"\bbitcoin\b",
    r"\bethereum\b",
    r"\bblockchain\b",
]

MEDIUM_SIGNAL_PATTERNS = [
    r"\balternative investments\b",
    r"\balternatives\b",
]

BAD_DOMAINS = ["facebook.com", "linkedin.com", "instagram.com", "twitter.com", "x.com", "youtube.com"]
BAD_NAME_WORDS = ["read more", "learn more", "click here"]

GOOD_FIRM_WORDS = ["capital", "partners", "wealth", "advisors", "management", "invest"]

# ------------------------

def clean(x):
    return "" if pd.isna(x) else str(x).strip()

def ensure_url(url):
    url = clean(url)
    if not url:
        return ""
    if not url.startswith("http"):
        return "https://" + url
    return url.lower()

def is_bad_domain(url):
    return any(d in url for d in BAD_DOMAINS)

def is_good_firm(name):
    name = name.lower()
    return any(w in name for w in GOOD_FIRM_WORDS)

def fetch(url):
    try:
        return requests.get(url, headers=HEADERS, timeout=REQUEST_TIMEOUT)
    except:
        return None

def fetch_html(url):
    r = fetch(url)
    if r and r.status_code == 200 and "text/html" in r.headers.get("content-type", ""):
        return r.text
    return None

def html_to_text(html):
    soup = BeautifulSoup(html, "html.parser")
    return soup.get_text(" ", strip=True)

# ------------------------
# SEC LOAD
# ------------------------

def get_sec_data():
    r = fetch(SEC_PAGE_URL)
    if r is None:
        raise Exception("Could not fetch SEC page")

    r.raise_for_status()
    soup = BeautifulSoup(r.text, "html.parser")

    candidates = []

    for a in soup.find_all("a", href=True):
        text = clean(a.get_text(" ", strip=True)).lower()
        href = clean(a["href"])

        if "registered investment advisers" not in text:
            continue

        full_url = urljoin("https://www.sec.gov", href)

        if full_url.lower().endswith(".zip"):
            candidates.append(full_url)

    if not candidates:
        raise Exception("SEC file not found")

    # SEC page lists newest first
    sec_zip_url = candidates[0]
    print("Using SEC ZIP:", sec_zip_url)

    zip_response = fetch(sec_zip_url)
    if zip_response is None:
        raise Exception("Could not download SEC ZIP")

    zip_response.raise_for_status()
    z = zipfile.ZipFile(io.BytesIO(zip_response.content))

    for filename in z.namelist():
        lower_name = filename.lower()

        if lower_name.endswith(".csv"):
            with z.open(filename) as f:
                return pd.read_csv(f, encoding="latin1", low_memory=False)

        if lower_name.endswith(".xlsx"):
            with z.open(filename) as f:
                return pd.read_excel(f)

    raise Exception("No CSV or XLSX found inside SEC ZIP")

def load_universe():
    df = get_sec_data()

    name_col = [c for c in df.columns if "business name" in c.lower()][0]
    web_col = [c for c in df.columns if "website" in c.lower()][0]

    df = df[[name_col, web_col]].copy()
    df.columns = ["firm", "website"]

    df["website"] = df["website"].apply(ensure_url)

    df = df[df["firm"] != ""]
    df = df[df["website"] != ""]
    df = df[~df["website"].apply(is_bad_domain)]
    df = df[df["firm"].apply(is_good_firm)]

    return df.drop_duplicates()

# ------------------------
# SIGNAL QUALITY
# ------------------------

def looks_article(url, text):
    if any(x in url for x in ["/news/", "/blog/", "/insights/", "/events/", "/press/"]):
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

def find_signal(pages):
    candidates = []

    for p in pages:
        text = p["text"]
        url = p["url"]
        quality = score_page(url, text)

        for pat in HIGH_SIGNAL_PATTERNS:
            if re.search(pat, text, re.I):
                candidates.append(("high", url, text, quality))

        for pat in MEDIUM_SIGNAL_PATTERNS:
            if re.search(pat, text, re.I):
                candidates.append(("medium", url, text, quality))

    if not candidates:
        return None

    # prioritize high
    highs = [c for c in candidates if c[0] == "high" and c[3] >= 2]
    if highs:
        return highs[0]

    meds = [c for c in candidates if c[0] == "medium" and c[3] >= 3]
    if meds:
        return meds[0]

    return None

# ------------------------
# CONTACTS
# ------------------------

def valid_name(x):
    x = clean(x)
    if any(w in x.lower() for w in BAD_NAME_WORDS):
        return False
    parts = x.split()
    return 1 < len(parts) <= 4 and all(p[0].isupper() for p in parts if p)

def extract_contacts(html):
    soup = BeautifulSoup(html, "html.parser")
    lines = [l.strip() for l in soup.get_text("\n").split("\n") if l.strip()]

    contacts = []
    for i, l in enumerate(lines):
        if any(k in l.lower() for k in ["ceo", "cio", "president", "partner"]):
            for j in range(max(0, i-3), i):
                if valid_name(lines[j]):
                    contacts.append((lines[j], l))
                    break

    seen = set()
    out = []
    for n,t in contacts:
        if n.lower() not in seen:
            seen.add(n.lower())
            out.append((n,t))

    return out[:2]

# ------------------------
# MAIN
# ------------------------

def run():
    df = load_universe()
    print("Firms:", len(df))

    batch = df.sample(min(15, len(df))).to_dict("records")

    results = []

    for firm in batch:
        site = firm["website"]
        print("Scanning:", firm["firm"])

        pages = []
        for path in CONTENT_PATHS:
            url = urljoin(site, path)
            html = fetch_html(url)
            if html:
                pages.append({"url": url, "text": html_to_text(html)})

        sig = find_signal(pages)
        if not sig:
            continue

        priority, url, text, _ = sig

        # extract contacts
        contacts = []
        for p in TEAM_PATHS:
            html = fetch_html(urljoin(site, p))
            if html:
                contacts = extract_contacts(html)
                if contacts:
                    break

        if not contacts:
            contacts = [("Chief Investment Officer",""), ("Managing Partner","")]

        results.append((priority, firm["firm"], url, text[:200], contacts))

    print("\n🚀 PREVIEW\n")

    if not results:
        print("No strong leads found.")
        return

    for r in results:
        pr, name, url, ev, c = r
        print(f"\n{'🔥 HIGH' if pr=='high' else '🟡 MED'}\n{name}")
        print("Source:", url)
        print("Evidence:", ev)
        print("Contacts:")
        print("-", c[0][0])
        print("-", c[1][0] if len(c)>1 else c[0][0])
        print("-----")

if __name__ == "__main__":
    try:
        run()
    except:
        traceback.print_exc()
