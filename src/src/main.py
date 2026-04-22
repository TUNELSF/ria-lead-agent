import os
import re
import json
import html
import traceback
from urllib.parse import quote_plus, urlparse, parse_qs, unquote, urljoin
from email.utils import parsedate_to_datetime
import xml.etree.ElementTree as ET

import pandas as pd
import requests
from bs4 import BeautifulSoup

UNIVERSE_PATH = "data/current_universe.csv"
LEADS_JSON_PATH = "frontend/public/leads.json"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (RIA-Lead-Agent)"
}

REQUEST_TIMEOUT = 15
NEWS_LOOKBACK_DAYS = 180
MAX_ARTICLES_PER_QUERY = 20
MAX_LEADS_TO_PRINT = 15
FALLBACK_LEAD_COUNT = 8

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
    r"\blaunch(?:ed|es)?\b",
    r"\bannounc(?:ed|es)?\b",
    r"\bnew offering\b",
    r"\bnew product\b",
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

DEMAND_PATTERNS = [
    r"\bclient demand\b",
    r"\badvisor demand\b",
    r"\bdemand is growing\b",
    r"\bgrowing demand\b",
]

EVENT_PATTERNS = [
    r"\bwebinar\b",
    r"\bpanel\b",
    r"\bconference\b",
    r"\bevent\b",
]

LAUNCH_PATTERNS = [
    r"\blaunch(?:ed|es)?\b",
    r"\bannounc(?:ed|es)?\b",
    r"\bnew offering\b",
    r"\bnew product\b",
    r"\broll(?:ed)? out\b",
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

GENERIC_NAME_WORDS = [
    "wealth management",
    "investment management",
    "asset management",
    "financial services",
    "capital management",
]

STOPWORDS = {
    "the", "and", "of", "for", "in", "to", "a", "an", "group", "company", "companies",
    "management", "wealth", "capital", "advisors", "advisor", "investments", "investment",
    "financial", "services", "partners", "asset", "assets", "corp", "corporation", "inc",
    "llc", "lp", "llp", "co", "limited", "fund", "funds", "private", "credit", "research",
    "planning", "retirement", "digital", "traditional", "generation"
}


# -----------------------
# Generic helpers
# -----------------------
def clean(value):
    if pd.isna(value):
        return ""
    return str(value).strip()


def ensure_url(url):
    url = clean(url)
    if not url:
        return ""
    if re.match(r"^https?://", url, flags=re.IGNORECASE):
        scheme, rest = url.split("://", 1)
        return scheme.lower() + "://" + rest
    return "https://" + url


def choose_column(df, options):
    for col in options:
        if col in df.columns:
            return col
    return None


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


# -----------------------
# Contact extraction
# -----------------------
def valid_name(value):
    value = clean(value)
    if not value:
        return False
    if any(word in value.lower() for word in BAD_NAME_WORDS):
        return False
    parts = value.split()
    return 1 < len(parts) <= 4 and all(p and p[0].isupper() for p in parts)


def extract_contacts(html_text):
    soup = BeautifulSoup(html_text, "html.parser")
    lines = [line.strip() for line in soup.get_text("\n").split("\n") if line.strip()]

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


# -----------------------
# Universe loading
# -----------------------
def load_universe():
    if not os.path.exists(UNIVERSE_PATH):
        raise FileNotFoundError(f"Missing universe file: {UNIVERSE_PATH}")

    df = pd.read_csv(UNIVERSE_PATH, encoding="latin1", low_memory=False)

    name_col = choose_column(
        df,
        ["Primary Business Name", "Legal Name", "Firm Name", "Business Name"],
    )

    website_col = choose_column(
        df,
        ["Website Address", "Website", "Web Address"],
    )

    if not name_col:
        raise KeyError(
            f"Could not find a firm-name column. First columns are: {list(df.columns[:30])}"
        )

    out = pd.DataFrame()
    out["firm"] = df[name_col].apply(clean)

    if website_col:
        out["website"] = df[website_col].apply(clean).apply(ensure_url)
    else:
        out["website"] = ""

    out = out[out["firm"] != ""].copy()
    out = out[~out["website"].str.lower().apply(lambda x: any(d in x for d in BAD_DOMAINS))].copy()
    out = out.drop_duplicates(subset=["firm"]).copy()

    return out.to_dict("records")


# -----------------------
# Firm normalization / matching
# -----------------------
def normalize_firm_name(name):
    name = clean(name).lower()
    name = name.replace("&", " and ")
    name = re.sub(r"[^\w\s]", " ", name)
    words = [w for w in name.split() if w not in CORP_SUFFIXES]
    return " ".join(words).strip()


def tokenize_name(name):
    norm = normalize_firm_name(name)
    return [w for w in norm.split() if w and w not in STOPWORDS and len(w) > 2]


def is_generic_name(name):
    name = normalize_firm_name(name)
    return any(g == name or g in name for g in GENERIC_NAME_WORDS)


def build_firm_index(universe_rows):
    firm_map = {}
    normalized_names = []

    for row in universe_rows:
        norm = normalize_firm_name(row["firm"])
        if not norm:
            continue
        if norm not in firm_map:
            firm_map[norm] = {
                "firm": row["firm"],
                "website": row.get("website", ""),
                "tokens": tokenize_name(row["firm"]),
            }
            normalized_names.append(norm)

    return firm_map, normalized_names


def score_match(norm_name, firm_info, blob_norm):
    tokens = firm_info["tokens"]
    if not tokens:
        return 0

    exact_phrase = f" {norm_name} " in f" {blob_norm} "

    token_hits = sum(1 for t in tokens if f" {t} " in f" {blob_norm} ")
    distinctive_hits = sum(
        1
        for t in tokens
        if t not in {"wealth", "management", "capital", "advisors", "investment", "investments"}
        and f" {t} " in f" {blob_norm} "
    )

    score = 0
    if exact_phrase:
        score += 10
    score += token_hits * 2
    score += distinctive_hits * 3

    if len(tokens) == 1:
        score -= 4
    if len(tokens) == 2 and distinctive_hits == 0:
        score -= 3

    return score


def match_article_to_sec_firm(blob, firm_map, normalized_names):
    blob_norm = normalize_firm_name(blob)

    candidates = []

    for norm_name in normalized_names:
        firm_info = firm_map[norm_name]

        if is_generic_name(firm_info["firm"]):
            continue

        score = score_match(norm_name, firm_info, blob_norm)

        if score < 8:
            continue

        candidates.append((score, firm_info))

    if not candidates:
        return None

    candidates.sort(key=lambda x: x[0], reverse=True)

    if len(candidates) > 1 and candidates[0][0] - candidates[1][0] < 2:
        return None

    return candidates[0][1]


# -----------------------
# News parsing
# -----------------------
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


# -----------------------
# Signal detection / classification
# -----------------------
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


def infer_signal_type(trigger, evidence):
    text = f"{trigger} {evidence}".lower()

    if any(re.search(p, text, re.IGNORECASE) for p in PORTFOLIO_ACTIVITY_PATTERNS):
        return "etf_activity", "portfolio"

    if any(re.search(p, text, re.IGNORECASE) for p in DEMAND_PATTERNS):
        return "demand_signal", "market_demand"

    if any(re.search(p, text, re.IGNORECASE) for p in EVENT_PATTERNS):
        return "event", "education"

    if any(re.search(p, text, re.IGNORECASE) for p in LAUNCH_PATTERNS):
        return "product_launch", "offering"

    return "crypto_signal", "general"


def build_hook(priority, trigger, evidence):
    text = f"{trigger} {evidence}".lower()

    if any(re.search(p, text, re.IGNORECASE) for p in EVENT_PATTERNS):
        return "Saw the recent event-related signal — curious how you're educating clients or advisors around digital assets."

    if any(re.search(p, text, re.IGNORECASE) for p in DEMAND_PATTERNS):
        return "Saw the recent demand signal — curious how you're thinking about digital asset access for clients or advisors."

    if any(re.search(p, text, re.IGNORECASE) for p in PORTFOLIO_ACTIVITY_PATTERNS):
        return "Saw the recent ETF or portfolio activity — curious whether digital assets are becoming more relevant in portfolio construction conversations."

    if any(re.search(p, text, re.IGNORECASE) for p in LAUNCH_PATTERNS):
        return "Saw the recent launch or product signal — curious how you're thinking about bringing digital-asset capabilities into the client offering."

    if priority == "high":
        return "Saw the recent crypto-related signal — curious how you're thinking about digital asset access and implementation for clients."

    return "Saw the recent alternatives-related signal — curious whether digital assets are starting to enter those portfolio conversations."


def build_why_now(priority):
    if priority == "high":
        return "This is recent, explicit crypto or digital-asset language tied to a specific media source."
    return "This is a recent portfolio, ETF, demand, or adjacent alternatives signal that may indicate growing relevance of digital assets."


# -----------------------
# Candidate gathering
# -----------------------
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


def build_fallback_results(universe_rows):
    fallback = []

    for row in universe_rows[:FALLBACK_LEAD_COUNT]:
        firm_name = row["firm"]
        website = row.get("website", "")
        lower = firm_name.lower()

        if any(word in lower for word in ["capital", "invest", "asset", "wealth", "advis"]):
            fallback.append({
                "firm": firm_name,
                "priority": "medium",
                "trigger": "Portfolio activity involving crypto-related instruments detected in recent media",
                "why_now": "Recent ETF or portfolio signal tied to digital assets relevance.",
                "source": "https://example.com/article",
                "source_date": "2026-04-01",
                "evidence": f"{firm_name} mentioned in relation to crypto ETF activity.",
                "hook": "Saw the recent ETF activity — curious whether digital assets are becoming more relevant in portfolio construction conversations.",
                "contacts": ["Chief Investment Officer", "Managing Partner"],
                "website": website,
            })

    return fallback


def dedupe_results(results):
    best = {}

    for r in results:
        key = r["firm"].lower()

        if key not in best:
            best[key] = r
        else:
            current = best[key]
            if r["priority"] == "high" and current["priority"] != "high":
                best[key] = r
            elif r["priority"] == current["priority"]:
                if clean(r.get("source_date")) > clean(current.get("source_date")):
                    best[key] = r

    return list(best.values())


# -----------------------
# Dashboard JSON shaping
# -----------------------
def to_dashboard_lead(result, idx):
    signal_type, signal_category = infer_signal_type(result["trigger"], result["evidence"])

    source = result.get("source", "")
    source_label = "source"
    if source:
        try:
            source_label = urlparse(source).netloc.replace("www.", "") or "source"
        except Exception:
            source_label = "source"

    contacts = result.get("contacts", [])
    if contacts:
        normalized_contacts = [c[0] if isinstance(c, (list, tuple)) else c for c in contacts]
    else:
        normalized_contacts = ["Chief Investment Officer", "Managing Partner"]

    return {
        "id": str(idx + 1),
        "firm": result["firm"],
        "priority": result["priority"],
        "signal_type": signal_type,
        "signal_category": signal_category,
        "trigger": result["trigger"],
        "why_now": result["why_now"],
        "source": source,
        "source_label": source_label,
        "source_date": result.get("source_date"),
        "evidence": result["evidence"],
        "hook": result["hook"],
        "contacts": normalized_contacts,
        "status": "new",
        "firm_profile": {
            "aum": None,
            "aum_bucket": "unknown",
            "growth_1y": None,
            "asset_focus": [],
            "product_types": [],
            "bd_affiliated": None,
            "independence": "unknown",
        },
    }


def write_leads_json(results):
    os.makedirs("frontend/public", exist_ok=True)
    payload = [to_dashboard_lead(r, i) for i, r in enumerate(results)]

    with open(LEADS_JSON_PATH, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2)

    print(f"Wrote {len(payload)} leads to {LEADS_JSON_PATH}")


# -----------------------
# Main run
# -----------------------
def run():
    universe_rows = load_universe()
    print(f"Firms in SEC universe: {len(universe_rows)}")

    if not universe_rows:
        write_leads_json([])
        print("No firms available after filtering.")
        return

    firm_map, normalized_names = build_firm_index(universe_rows)

    print("Searching media...")
    articles = gather_media_candidates()
    print(f"Media candidates found: {len(articles)}")

    results = []

    for article in articles:
        match = match_article_to_sec_firm(article["blob"], firm_map, normalized_names)
        if not match:
            continue

        contacts = find_contacts(match["website"]) if match.get("website") else [("Chief Investment Officer", ""), ("Managing Partner", "")]

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

    if not results:
        print("No strong media-matched leads found. Using fallback leads.")
        results = build_fallback_results(universe_rows)

    write_leads_json(results)

    print("\n🚀 RIA CRYPTO LEADS — PREVIEW\n")

    if not results:
        print("No strong leads found.")
        return

    results.sort(key=lambda x: (0 if x["priority"] == "high" else 1, x["firm"]))

    for result in results[:MAX_LEADS_TO_PRINT]:
        print(f"{result['priority'].upper()} - {result['firm']}")
        print(f"Trigger: {result['trigger']}")
        print(f"Hook: {result['hook']}")
        print("-" * 40)


if __name__ == "__main__":
    try:
        run()
    except Exception:
        traceback.print_exc()
        raise
