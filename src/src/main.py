import os
import re
import random
import requests
import pandas as pd
from bs4 import BeautifulSoup
from urllib.parse import urljoin
import zipfile
import io

# =========================
# CONFIG
# =========================

SEC_URL = "https://www.sec.gov/data-research/sec-markets-data/information-about-registered-investment-advisers-exempt-reporting-advisers"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; RIA-Crypto-Scout/1.0)"
}

REQUEST_TIMEOUT = 12

DAILY_TIER1_LIMIT = 60
DAILY_TIER2_LIMIT = 40

CONTENT_PATHS = [
    "/",
    "/news",
    "/press",
    "/blog",
    "/insights",
    "/media",
    "/resources",
    "/events",
]

TEAM_PATHS = [
    "/team",
    "/our-team",
    "/leadership",
    "/about",
    "/about-us",
]

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
    r"\bprivate markets\b",
]

CONTACT_TITLE_PATTERNS = [
    r"chief executive officer",
    r"\bceo\b",
    r"chief investment officer",
    r"\bcio\b",
    r"president",
    r"managing partner",
    r"founder",
]

# =========================
# SEC DOWNLOAD
# =========================

def download_latest_sec_file():
    print("Fetching SEC page...")

    resp = requests.get(SEC_URL, headers=HEADERS)
    soup = BeautifulSoup(resp.text, "html.parser")

    links = soup.find_all("a", href=True)

    zip_links = []
    for link in links:
        href = link["href"]
        text = link.get_text().lower()

        if "registered investment advisers" in text and ".zip" in href:
            zip_links.append(href)

    if not zip_links:
        raise Exception("No SEC ZIP file found")

    latest_link = zip_links[0]

    if not latest_link.startswith("http"):
        latest_link = "https://www.sec.gov" + latest_link

    print(f"Downloading SEC file: {latest_link}")

    file_resp = requests.get(latest_link, headers=HEADERS)
    z = zipfile.ZipFile(io.BytesIO(file_resp.content))

    for name in z.namelist():
        if name.endswith(".csv"):
            with z.open(name) as f:
                return pd.read_csv(f, encoding="latin1", low_memory=False)

        if name.endswith(".xlsx"):
            with z.open(name) as f:
                return pd.read_excel(f)

    raise Exception("No usable file found inside ZIP")

# =========================
# LOAD + CLEAN
# =========================

def load_sec_universe():
    df = download_latest_sec_file()

    rename_map = {
        "Primary Business Name": "firm_name",
        "Website Address": "website",
        "5F(2)(c)": "aum"
    }

    for old, new in rename_map.items():
        if old in df.columns:
            df = df.rename(columns={old: new})

    df["firm_name"] = df["firm_name"].fillna("").astype(str)
    df["website"] = df["website"].fillna("").astype(str)

    df = df[df["firm_name"] != ""]
    df = df[df["website"] != ""]

    return df

# =========================
# SCORING (OPTION B)
# =========================

def score_firm(row):
    score = 0

    website = row["website"]
    name = row["firm_name"].lower()

    if website.startswith("http"):
        score += 3

    if any(x in name for x in ["capital", "partners", "wealth"]):
        score += 1

    aum = str(row.get("aum", ""))
    if any(x in aum for x in ["000000000"]):  # crude large AUM signal
        score += 3

    return score

def assign_tier(score):
    if score >= 5:
        return "tier1"
    if score >= 3:
        return "tier2"
    return "skip"

# =========================
# SCRAPING
# =========================

def fetch_html(url):
    try:
        r = requests.get(url, headers=HEADERS, timeout=REQUEST_TIMEOUT)
        if r.status_code == 200:
            return r.text
    except:
        pass
    return None

def html_to_text(html):
    soup = BeautifulSoup(html, "html.parser")
    return soup.get_text(" ", strip=True)

# =========================
# SIGNAL DETECTION
# =========================

def detect_signal(pages):
    for page in pages:
        text = page["text"]

        for p in HIGH_SIGNAL_PATTERNS:
            if re.search(p, text, re.IGNORECASE):
                return {
                    "priority": "high",
                    "source": page["url"]
                }

    for page in pages:
        text = page["text"]

        for p in MEDIUM_SIGNAL_PATTERNS:
            if re.search(p, text, re.IGNORECASE):
                return {
                    "priority": "medium",
                    "source": page["url"]
                }

    return None

# =========================
# CONTACT EXTRACTION
# =========================

def extract_contacts(html):
    soup = BeautifulSoup(html, "html.parser")
    texts = soup.get_text("\n").split("\n")

    contacts = []

    for i, line in enumerate(texts):
        line = line.strip()

        if any(re.search(p, line.lower()) for p in CONTACT_TITLE_PATTERNS):
            if i > 0:
                name = texts[i - 1].strip()
                if len(name.split()) <= 4:
                    contacts.append((name, line))

    return contacts[:2]

# =========================
# MAIN
# =========================

def run():
    print("Loading SEC data...")
    df = load_sec_universe()

    print(f"{len(df)} firms loaded")

    df["score"] = df.apply(score_firm, axis=1)
    df["tier"] = df["score"].apply(assign_tier)

    tier1 = df[df["tier"] == "tier1"]
    tier2 = df[df["tier"] == "tier2"]

    batch = list(tier1.head(DAILY_TIER1_LIMIT).to_dict("records"))

    tier2_list = tier2.to_dict("records")
    random.shuffle(tier2_list)
    batch += tier2_list[:DAILY_TIER2_LIMIT]

    print(f"Scanning {len(batch)} firms...")

    high = []
    medium = []

    for firm in batch:
        website = firm["website"]

        pages = []

        for path in CONTENT_PATHS:
            url = urljoin(website, path)
            html = fetch_html(url)
            if html:
                pages.append({
                    "url": url,
                    "text": html_to_text(html)
                })

        signal = detect_signal(pages)

        if not signal:
            continue

        contacts = []
        for path in TEAM_PATHS:
            url = urljoin(website, path)
            html = fetch_html(url)
            if html:
                contacts = extract_contacts(html)
                if contacts:
                    break

        contact_lines = []
        for c in contacts:
            contact_lines.append(f"{c[0]} — {c[1]}")

        if not contact_lines:
            contact_lines = ["Chief Investment Officer", "Managing Partner"]

        output = f"""
{'🔥 HIGH PRIORITY' if signal['priority']=='high' else '🟡 MEDIUM PRIORITY'}

{firm['firm_name']}
Source: {signal['source']}
Potential contacts:
- {contact_lines[0]}
- {contact_lines[1] if len(contact_lines)>1 else contact_lines[0]}
"""

        if signal["priority"] == "high":
            high.append(output)
        else:
            medium.append(output)

    print("\n🚀 RIA CRYPTO LEADS — PREVIEW\n")

    for h in high[:10]:
        print(h)
        print("-----")

    for m in medium[:10]:
        print(m)
        print("-----")


if __name__ == "__main__":
    run()
