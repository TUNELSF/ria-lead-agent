import os
import re
import math
import random
import requests
import pandas as pd
from bs4 import BeautifulSoup
from urllib.parse import urljoin

# =========================
# CONFIG
# =========================

SEC_UNIVERSE_PATH = "data/current_universe.csv"

DAILY_TIER1_LIMIT = 60
DAILY_TIER2_LIMIT = 40
REQUEST_TIMEOUT = 12

HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; RIA-Crypto-Scout/1.0)"
}

# Pages we will scan for signals
CONTENT_PATHS = [
    "/",
    "/news",
    "/press",
    "/blog",
    "/insights",
    "/articles",
    "/media",
    "/resources",
    "/events",
    "/webinars",
]

# Pages we will scan for contacts
TEAM_PATHS = [
    "/team",
    "/our-team",
    "/leadership",
    "/about",
    "/about-us",
    "/who-we-are",
    "/our-people",
    "/management",
]

# Explicit crypto / digital asset language = HIGH
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
    r"\bcrypto etf\b",
    r"\bbitcoin etf\b",
]

# Adjacent language = MEDIUM only if found on a specific page
MEDIUM_SIGNAL_PATTERNS = [
    r"\balternative investments\b",
    r"\balternatives\b",
    r"\bprivate markets\b",
    r"\bnew asset classes\b",
    r"\bportfolio diversification\b",
    r"\binvestment innovation\b",
    r"\bevolving client demand\b",
]

CONTACT_TITLE_PATTERNS = [
    r"chief executive officer",
    r"\bceo\b",
    r"chief investment officer",
    r"\bcio\b",
    r"president",
    r"managing partner",
    r"founder",
    r"partner",
    r"head of investments",
    r"chief operating officer",
    r"\bcoo\b",
    r"chief financial officer",
    r"\bcfo\b",
]

# =========================
# HELPERS
# =========================

def normalize_text(value):
    if pd.isna(value):
        return ""
    return str(value).strip()

def safe_lower(value):
    return normalize_text(value).lower()

def ensure_url(url):
    url = normalize_text(url)
    if not url:
        return ""
    if not url.startswith("http://") and not url.startswith("https://"):
        return "https://" + url
    return url

def looks_like_valid_website(url):
    url = ensure_url(url).lower()
    return url.startswith("http://") or url.startswith("https://")

def parse_money_to_float(value):
    """
    Handles values like:
    "$1,234,567,890"
    "1234567890"
    "1.2E9"
    """
    raw = normalize_text(value)
    if not raw:
        return 0.0

    try:
        cleaned = re.sub(r"[^0-9.eE\-+]", "", raw)
        if not cleaned:
            return 0.0
        return float(cleaned)
    except Exception:
        return 0.0

def dedupe_preserve_order(items):
    seen = set()
    out = []
    for item in items:
        if item not in seen:
            seen.add(item)
            out.append(item)
    return out

def fetch_html(url):
    try:
        response = requests.get(url, headers=HEADERS, timeout=REQUEST_TIMEOUT, allow_redirects=True)
        if response.status_code == 200 and "text/html" in response.headers.get("Content-Type", ""):
            return response.text
    except Exception:
        pass
    return None

def html_to_text(html):
    soup = BeautifulSoup(html, "html.parser")
    for tag in soup(["script", "style", "noscript"]):
        tag.decompose()
    return soup.get_text(" ", strip=True)

def clean_text(text):
    return re.sub(r"\s+", " ", normalize_text(text)).strip()

def sentence_snippet(text, pattern):
    """
    Return the sentence-ish snippet around the first match.
    """
    text = clean_text(text)
    if not text:
        return ""

    m = re.search(pattern, text, flags=re.IGNORECASE)
    if not m:
        return text[:240]

    start = max(0, m.start() - 120)
    end = min(len(text), m.end() + 120)
    return text[start:end].strip()

# =========================
# SEC UNIVERSE LOADING
# =========================

def load_sec_universe():
    """
    Uses the real column names from your SEC roster.
    """
    if not os.path.exists(SEC_UNIVERSE_PATH):
        raise FileNotFoundError(
            f"Could not find SEC universe file at {SEC_UNIVERSE_PATH}. "
            "Put the latest SEC file there as current_universe.csv"
        )

    df = pd.read_csv(SEC_UNIVERSE_PATH, encoding="latin1", low_memory=False)

    # Map the SEC columns we know exist in your file
    rename_map = {
        "Organization CRD#": "crd",
        "Primary Business Name": "firm_name",
        "Legal Name": "legal_name",
        "Main Office City": "city",
        "Main Office State": "state",
        "Website Address": "website",
        "5F(2)(c)": "aum",  # this is commonly RAUM in SEC export; if empty, code still works
        "Latest ADV Filing Date": "latest_adv_filing_date",
        "SEC Current Status": "sec_status",
        "Firm Type": "firm_type",
    }

    for old, new in rename_map.items():
        if old in df.columns:
            df = df.rename(columns={old: new})

    required = ["firm_name", "website"]
    for col in required:
        if col not in df.columns:
            raise ValueError(f"Missing expected SEC column: {col}")

    # Basic clean-up
    for col in ["firm_name", "legal_name", "city", "state", "website", "sec_status", "firm_type"]:
        if col in df.columns:
            df[col] = df[col].fillna("").astype(str).str.strip()

    if "aum" not in df.columns:
        df["aum"] = 0

    df["aum_num"] = df["aum"].apply(parse_money_to_float)
    df["website"] = df["website"].apply(ensure_url)

    # Remove obviously unusable rows
    df = df[df["firm_name"].astype(str).str.strip() != ""].copy()
    df = df[df["website"].astype(str).str.strip() != ""].copy()
    df = df[df["website"].apply(looks_like_valid_website)].copy()

    # Deduplicate by CRD if available, otherwise by firm name + website
    if "crd" in df.columns:
        df["dedupe_key"] = df["crd"].astype(str).str.strip()
        df.loc[df["dedupe_key"] == "", "dedupe_key"] = (
            df["firm_name"].str.lower() + "|" + df["website"].str.lower()
        )
    else:
        df["dedupe_key"] = df["firm_name"].str.lower() + "|" + df["website"].str.lower()

    df = df.drop_duplicates(subset=["dedupe_key"]).copy()

    return df

# =========================
# FIRM SCORING (OPTION B)
# =========================

def score_firm(row):
    score = 0

    firm_name = safe_lower(row.get("firm_name", ""))
    website = safe_lower(row.get("website", ""))
    sec_status = safe_lower(row.get("sec_status", ""))
    firm_type = safe_lower(row.get("firm_type", ""))
    aum = float(row.get("aum_num", 0) or 0)

    blob = " ".join([
        firm_name,
        safe_lower(row.get("legal_name", "")),
        safe_lower(row.get("firm_type", "")),
    ])

    if looks_like_valid_website(website):
        score += 3

    if "approved" in sec_status or "registered" in sec_status:
        score += 1

    if aum >= 500_000_000:
        score += 3
    if aum >= 1_000_000_000:
        score += 2

    if any(word in blob for word in ["capital", "partners", "wealth", "advisors"]):
        score += 1

    if any(word in blob for word in ["institutional", "private", "fund"]):
        score += 1

    return score

def assign_tier(score):
    if score >= 6:
        return "tier1"
    if score >= 3:
        return "tier2"
    return "skip"

def build_daily_batch(df):
    df = df.copy()
    df["score"] = df.apply(score_firm, axis=1)
    df["tier"] = df["score"].apply(assign_tier)

    tier1 = df[df["tier"] == "tier1"].copy().sort_values("score", ascending=False)
    tier2 = df[df["tier"] == "tier2"].copy().sort_values("score", ascending=False)

    # Stable-ish daily rotation for Tier 2
    today_seed = int(pd.Timestamp.utcnow().strftime("%Y%m%d"))
    random.seed(today_seed)

    tier1_records = tier1.head(DAILY_TIER1_LIMIT).to_dict("records")
    tier2_records = tier2.to_dict("records")
    random.shuffle(tier2_records)
    tier2_records = tier2_records[:DAILY_TIER2_LIMIT]

    batch = tier1_records + tier2_records
    return batch, tier1, tier2

# =========================
# SIGNAL SCANNING
# =========================

def find_signal_pages(base_url):
    pages = []
    for path in CONTENT_PATHS:
        url = urljoin(base_url, path)
        html = fetch_html(url)
        if not html:
            continue
        text = html_to_text(html)
        pages.append({
            "url": url,
            "html": html,
            "text": text,
        })
    return pages

def detect_signal_from_pages(pages):
    """
    Rules:
    - HIGH only if explicit crypto/digital-asset language appears on an exact page
    - MEDIUM only if adjacent language appears on an exact page
    - Exact page URL is preserved
    """
    # First pass: HIGH
    for page in pages:
        text = page["text"]
        for pattern in HIGH_SIGNAL_PATTERNS:
            if re.search(pattern, text, flags=re.IGNORECASE):
                return {
                    "priority": "high",
                    "trigger": f"Explicit crypto-related language found on a public firm page",
                    "source_url": page["url"],
                    "source_type": "website",
                    "source_title": None,
                    "snippet": sentence_snippet(text, pattern),
                }

    # Second pass: MEDIUM
    for page in pages:
        text = page["text"]
        for pattern in MEDIUM_SIGNAL_PATTERNS:
            if re.search(pattern, text, flags=re.IGNORECASE):
                return {
                    "priority": "medium",
                    "trigger": f"Adjacent alternatives language found on a public firm page",
                    "source_url": page["url"],
                    "source_type": "website",
                    "source_title": None,
                    "snippet": sentence_snippet(text, pattern),
                }

    return None

# =========================
# CONTACT EXTRACTION
# =========================

def looks_like_name(text):
    text = clean_text(text)

    blacklist = {
        "learn more", "read more", "contact us", "our team", "about us", "leadership",
        "financial advisor", "wealth advisor", "investment management", "private wealth",
        "chief executive officer", "chief investment officer", "managing partner",
        "president", "founder", "partner", "chief operating officer", "chief financial officer"
    }

    if not text or text.lower() in blacklist:
        return False

    words = text.split()
    if len(words) < 2 or len(words) > 4:
        return False

    for w in words:
        if not re.match(r"^[A-Z][a-zA-Z\-\']+$", w):
            return False

    return True

def title_matches(text):
    text = text.lower()
    return any(re.search(pattern, text) for pattern in CONTACT_TITLE_PATTERNS)

def extract_contacts_from_html(html):
    soup = BeautifulSoup(html, "html.parser")
    contacts = []

    candidate_tags = soup.find_all(["h1", "h2", "h3", "h4", "strong", "b", "p", "div", "span", "li"])
    texts = [clean_text(tag.get_text(" ", strip=True)) for tag in candidate_tags]
    texts = [t for t in texts if t]

    # Strategy 1: title near name
    for i, text in enumerate(texts):
        if title_matches(text):
            for j in range(max(0, i - 3), i):
                possible_name = texts[j]
                if looks_like_name(possible_name):
                    contacts.append({
                        "name": possible_name,
                        "title": text
                    })
                    break

    # Strategy 2: card scanning
    cards = soup.find_all(["article", "section", "div", "li"])
    for card in cards:
        card_text = clean_text(card.get_text(" ", strip=True))
        if not card_text or not title_matches(card_text):
            continue

        lines = [clean_text(t) for t in card.stripped_strings]
        found_name = None
        found_title = None

        for line in lines[:8]:
            if not found_name and looks_like_name(line):
                found_name = line
            if not found_title and title_matches(line):
                found_title = line

        if found_name and found_title:
            contacts.append({
                "name": found_name,
                "title": found_title
            })

    # Deduplicate
    deduped = []
    seen = set()
    for c in contacts:
        key = (c["name"].lower(), c["title"].lower())
        if key not in seen:
            seen.add(key)
            deduped.append(c)

    return deduped

def score_contact(contact):
    title = contact["title"].lower()
    if "chief executive officer" in title or re.search(r"\bceo\b", title):
        return 100
    if "chief investment officer" in title or re.search(r"\bcio\b", title):
        return 95
    if "president" in title:
        return 90
    if "managing partner" in title:
        return 85
    if "founder" in title:
        return 80
    if "head of investments" in title:
        return 78
    if "chief operating officer" in title or re.search(r"\bcoo\b", title):
        return 75
    if "chief financial officer" in title or re.search(r"\bcfo\b", title):
        return 72
    if "partner" in title:
        return 70
    return 50

def find_contacts(website):
    all_contacts = []

    for path in TEAM_PATHS:
        url = urljoin(website, path)
        html = fetch_html(url)
        if not html:
            continue
        contacts = extract_contacts_from_html(html)
        all_contacts.extend(contacts)

    # Deduplicate by name, keep highest title score
    unique = {}
    for c in all_contacts:
        name_key = c["name"].strip().lower()
        if name_key not in unique or score_contact(c) > score_contact(unique[name_key]):
            unique[name_key] = c

    ranked = sorted(unique.values(), key=score_contact, reverse=True)

    if len(ranked) >= 2:
        return ranked[:2]

    fallback = [
        {"name": None, "title": "Chief Investment Officer"},
        {"name": None, "title": "Managing Partner"},
    ]

    if len(ranked) == 1:
        return [ranked[0], fallback[0]]

    return fallback

def format_contact(contact):
    if contact["name"]:
        return f"{contact['name']} — {contact['title']}"
    return contact["title"]

# =========================
# OUTPUT
# =========================

def build_hook(signal):
    if signal["priority"] == "high":
        return (
            "Saw the explicit digital-asset language on your recent public page — "
            "curious how you're thinking about crypto access and implementation for clients."
        )

    return (
        "Noticed the recent alternatives-related language — curious whether digital assets are "
        "starting to enter those portfolio conversations internally."
    )

def build_why_now(signal):
    if signal["priority"] == "high":
        return "This is explicit crypto or digital-asset language on a current public page, which makes it a strong, timely signal."
    return "This is a fresh adjacent signal that may suggest openness to new asset classes, but it is not an explicit crypto signal yet."

def format_lead_for_telegram_style(firm, signal, contacts):
    icon = "🔥 HIGH PRIORITY" if signal["priority"] == "high" else "🟡 MEDIUM PRIORITY"

    return (
        f"{icon}\n\n"
        f"{firm['firm_name']}\n"
        f"Trigger: {signal['trigger']}\n"
        f"Why now: {build_why_now(signal)}\n"
        f"Source: {signal['source_url']}\n"
        f"Hook: {build_hook(signal)}\n"
        f"Potential contacts:\n"
        f"- {format_contact(contacts[0])}\n"
        f"- {format_contact(contacts[1])}\n"
    )

# =========================
# MAIN
# =========================

def run():
    print("Loading SEC universe...")
    df = load_sec_universe()
    print(f"Loaded {len(df):,} scannable firms from SEC universe.")

    batch, tier1, tier2 = build_daily_batch(df)

    print(f"Tier 1 firms available: {len(tier1):,}")
    print(f"Tier 2 firms available: {len(tier2):,}")
    print(f"Daily batch size: {len(batch):,}")
    print("\nStarting scan...\n")

    high_leads = []
    medium_leads = []

    for idx, firm in enumerate(batch, start=1):
        firm_name = firm.get("firm_name", "")
        website = firm.get("website", "")

        print(f"[{idx}/{len(batch)}] Scanning {firm_name} — {website}")

        pages = find_signal_pages(website)
        if not pages:
            continue

        signal = detect_signal_from_pages(pages)
        if not signal:
            continue

        contacts = find_contacts(website)
        formatted = format_lead_for_telegram_style(firm, signal, contacts)

        if signal["priority"] == "high":
            high_leads.append(formatted)
        elif signal["priority"] == "medium":
            medium_leads.append(formatted)

    print("\n" + "=" * 80)
    print("🚀 RIA CRYPTO LEADS — PREVIEW")
    print("=" * 80 + "\n")

    if not high_leads and not medium_leads:
        print("No crypto-relevant leads found in this run.")
        return

    if high_leads:
        for lead in high_leads[:10]:
            print(lead)
            print("---\n")

    if medium_leads:
        for lead in medium_leads[:10]:
            print(lead)
            print("---\n")

if __name__ == "__main__":
    run()
