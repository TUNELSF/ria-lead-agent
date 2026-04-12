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
    "User-Agent": "Mozilla/5.0 (compatible; RIA-Crypto-Scout/1.0; +https://www.sec.gov/)"
}

REQUEST_TIMEOUT = 20

# Keep these small while testing
DAILY_TIER1_LIMIT = 10
DAILY_TIER2_LIMIT = 5

CONTENT_PATHS = [
    "/",
    "/news",
    "/insights",
    "/blog",
    "/events",
]

TEAM_PATHS = [
    "/team",
    "/our-team",
    "/leadership",
    "/about",
    "/about-us",
    "/management",
]

HIGH_SIGNAL_PATTERNS = [
    r"\bcrypto\b",
    r"\bcryptocurrency\b",
    r"\bdigital asset\b",
    r"\bdigital assets\b",
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
    r"chief compliance officer",
    r"\bcco\b",
    r"partner",
]

BAD_WEBSITE_DOMAINS = [
    "facebook.com",
    "linkedin.com",
    "instagram.com",
    "twitter.com",
    "x.com",
    "youtube.com",
    "youtu.be",
]

def clean_str(value):
    if pd.isna(value):
        return ""
    return str(value).strip()

def ensure_url(url):
    url = clean_str(url)
    if not url:
        return ""

    url = url.strip()

    # If it already has a scheme, normalize scheme casing only
    if re.match(r"^https?://", url, flags=re.IGNORECASE):
        scheme, rest = url.split("://", 1)
        return scheme.lower() + "://" + rest

    return "https://" + url

def is_bad_website(url):
    url = clean_str(url).lower()
    return any(domain in url for domain in BAD_WEBSITE_DOMAINS)

def fetch(url):
    return requests.get(
        url,
        headers=HEADERS,
        timeout=REQUEST_TIMEOUT,
        allow_redirects=True,
    )

def fetch_html(url):
    try:
        r = fetch(url)
        content_type = r.headers.get("content-type", "").lower()
        if r.status_code == 200 and "text/html" in content_type:
            return r.text
    except Exception:
        pass
    return None

def html_to_text(html):
    soup = BeautifulSoup(html, "html.parser")
    for tag in soup(["script", "style", "noscript"]):
        tag.decompose()
    return soup.get_text(" ", strip=True)

def clean_text(text):
    return re.sub(r"\s+", " ", clean_str(text)).strip()

def sentence_snippet(text, pattern):
    text = clean_text(text)
    if not text:
        return ""

    match = re.search(pattern, text, flags=re.IGNORECASE)
    if not match:
        return text[:220]

    start = max(0, match.start() - 120)
    end = min(len(text), match.end() + 120)
    snippet = text[start:end].strip()
    return snippet[:300]

def find_latest_sec_download():
    print("Fetching SEC listing page...")
    r = fetch(SEC_PAGE_URL)
    r.raise_for_status()

    soup = BeautifulSoup(r.text, "html.parser")
    candidates = []

    for a in soup.find_all("a", href=True):
        href = a["href"].strip()
        text = a.get_text(" ", strip=True).lower()

        if "registered investment advisers" not in text:
            continue

        full_url = urljoin("https://www.sec.gov", href)
        if any(full_url.lower().endswith(ext) for ext in [".zip", ".xlsx", ".csv"]):
            candidates.append((text, full_url))

    if not candidates:
        raise RuntimeError("Could not find a Registered Investment Advisers download link on the SEC page.")

    # First result on page is usually the newest
    return candidates[0][1]

def load_dataframe_from_sec_file(file_url):
    print(f"Downloading SEC file: {file_url}")
    r = fetch(file_url)
    r.raise_for_status()

    lower = file_url.lower()

    if lower.endswith(".csv"):
        return pd.read_csv(io.BytesIO(r.content), encoding="latin1", low_memory=False)

    if lower.endswith(".xlsx"):
        return pd.read_excel(io.BytesIO(r.content))

    if lower.endswith(".zip"):
        z = zipfile.ZipFile(io.BytesIO(r.content))
        for name in z.namelist():
            lname = name.lower()
            if lname.endswith(".csv"):
                with z.open(name) as f:
                    return pd.read_csv(f, encoding="latin1", low_memory=False)
            if lname.endswith(".xlsx"):
                with z.open(name) as f:
                    return pd.read_excel(f)

        raise RuntimeError("ZIP downloaded from SEC did not contain a CSV or XLSX file.")

    raise RuntimeError(f"Unsupported SEC file type: {file_url}")

def choose_column(df, options, required=True):
    for col in options:
        if col in df.columns:
            return col
    if required:
        raise RuntimeError(
            f"Missing expected column. Tried: {options}. "
            f"Actual columns start with: {list(df.columns[:20])}"
        )
    return None

def load_sec_universe():
    file_url = find_latest_sec_download()
    df = load_dataframe_from_sec_file(file_url)

    firm_col = choose_column(df, [
        "Primary Business Name",
        "Legal Name",
        "Firm Name",
        "Business Name",
    ])

    website_col = choose_column(df, [
        "Website Address",
        "Website",
        "Web Address",
    ])

    aum_col = choose_column(df, [
        "5F(2)(c)",
        "AUM",
        "RAUM",
        "Regulatory Assets Under Management",
    ], required=False)

    out = pd.DataFrame()
    out["firm_name"] = df[firm_col].apply(clean_str)
    out["website"] = df[website_col].apply(clean_str).apply(ensure_url)
    out["aum"] = df[aum_col].apply(clean_str) if aum_col else ""

    out = out[out["firm_name"] != ""].copy()
    out = out[out["website"] != ""].copy()
    out = out[~out["website"].apply(is_bad_website)].copy()
    out = out.drop_duplicates(subset=["firm_name", "website"]).copy()

    print(f"Scannable firms after cleanup: {len(out)}")
    return out

def score_firm(row):
    score = 0

    website = row["website"]
    name = row["firm_name"].lower()
    aum = clean_str(row.get("aum", ""))

    if website.startswith("http://") or website.startswith("https://"):
        score += 3

    if any(x in name for x in ["capital", "partners", "wealth", "advisors"]):
        score += 1

    digits = re.sub(r"\D", "", aum)
    if len(digits) >= 10:
        score += 2
    if len(digits) >= 12:
        score += 1

    return score

def assign_tier(score):
    if score >= 5:
        return "tier1"
    if score >= 3:
        return "tier2"
    return "skip"

def detect_signal(pages):
    for page in pages:
        text = page["text"]
        for p in HIGH_SIGNAL_PATTERNS:
            if re.search(p, text, re.IGNORECASE):
                return {
                    "priority": "high",
                    "source": page["url"],
                    "trigger": "Explicit crypto-related language found on a public page",
                    "evidence": sentence_snippet(text, p),
                }

    for page in pages:
        text = page["text"]
        for p in MEDIUM_SIGNAL_PATTERNS:
            if re.search(p, text, re.IGNORECASE):
                return {
                    "priority": "medium",
                    "source": page["url"],
                    "trigger": "Adjacent alternatives language found on a public page",
                    "evidence": sentence_snippet(text, p),
                }

    return None

def looks_like_name(text):
    text = clean_text(text)
    words = text.split()
    if len(words) < 2 or len(words) > 4:
        return False

    for w in words:
        if not re.match(r"^[A-Z][a-zA-Z\-\']+$", w):
            return False

    return True

def extract_contacts(html):
    soup = BeautifulSoup(html, "html.parser")
    texts = [t.strip() for t in soup.get_text("\n").split("\n") if t.strip()]
    contacts = []

    for i, line in enumerate(texts):
        ll = line.lower()
        if any(re.search(p, ll) for p in CONTACT_TITLE_PATTERNS):
            for j in range(max(0, i - 3), i):
                candidate = texts[j].strip()
                if looks_like_name(candidate):
                    contacts.append((candidate, line))
                    break

    # Deduplicate exact name-title pairs
    deduped = []
    seen = set()
    for name, title in contacts:
        key = (name.lower(), title.lower())
        if key not in seen:
            seen.add(key)
            deduped.append((name, title))

    # Deduplicate by name only
    final_contacts = []
    seen_names = set()
    for name, title in deduped:
        if name.lower() not in seen_names:
            seen_names.add(name.lower())
            final_contacts.append((name, title))

    return final_contacts[:2]

def build_hook(signal):
    if signal["priority"] == "high":
        return (
            "Saw the crypto-related language on your public materials — "
            "curious how you're thinking about digital asset access and implementation for clients."
        )
    return (
        "Noticed the alternatives language — curious whether digital assets "
        "are starting to enter those portfolio conversations."
    )

def build_why_now(signal):
    if signal["priority"] == "high":
        return "This is explicit crypto or digital-asset language on a current public page, which makes it a strong live signal."
    return "This is a fresh adjacent signal, though it is not yet explicit crypto language."

def run():
    df = load_sec_universe()

    df["score"] = df.apply(score_firm, axis=1)
    df["tier"] = df["score"].apply(assign_tier)

    tier1 = df[df["tier"] == "tier1"].copy()
    tier2 = df[df["tier"] == "tier2"].copy()

    batch = list(tier1.head(DAILY_TIER1_LIMIT).to_dict("records"))
    tier2_list = tier2.to_dict("records")
    random.shuffle(tier2_list)
    batch += tier2_list[:DAILY_TIER2_LIMIT]

    print(f"Tier1: {len(tier1)} | Tier2: {len(tier2)} | Batch: {len(batch)}")

    high = []
    medium = []

    for idx, firm in enumerate(batch, start=1):
        website = firm["website"]
        print(f"[{idx}/{len(batch)}] {firm['firm_name']} -> {website}")

        pages = []
        for path in CONTENT_PATHS:
            url = urljoin(website, path)
            html = fetch_html(url)
            if html:
                pages.append({
                    "url": url,
                    "text": html_to_text(html),
                })

        if not pages:
            continue

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

        contact_lines = [f"{name} — {title}" for name, title in contacts]

        if len(contact_lines) == 1:
            contact_lines.append("Chief Investment Officer")
        if not contact_lines:
            contact_lines = ["Chief Investment Officer", "Managing Partner"]

        output = (
            f"\n{'🔥 HIGH PRIORITY' if signal['priority'] == 'high' else '🟡 MEDIUM PRIORITY'}\n\n"
            f"{firm['firm_name']}\n"
            f"Trigger: {signal['trigger']}\n"
            f"Why now: {build_why_now(signal)}\n"
            f"Source: {signal['source']}\n"
            f"Evidence: {signal['evidence']}\n"
            f"Hook: {build_hook(signal)}\n"
            f"Potential contacts:\n"
            f"- {contact_lines[0]}\n"
            f"- {contact_lines[1]}\n"
        )

        if signal["priority"] == "high":
            high.append(output)
        else:
            medium.append(output)

    print("\n🚀 RIA CRYPTO LEADS — PREVIEW\n")

    if not high and not medium:
        print("No crypto-relevant leads found in this run.")
        return

    for item in high[:10]:
        print(item)
        print("-----")

    for item in medium[:10]:
        print(item)
        print("-----")

if __name__ == "__main__":
    try:
        run()
    except Exception as e:
        print("\nSCRIPT FAILED\n")
        print(type(e).__name__, ":", e)
        print("\nFULL TRACEBACK:\n")
        traceback.print_exc()
        raise
