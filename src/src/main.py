import os
import json
from urllib.parse import urlparse

import pandas as pd

UNIVERSE_PATH = "data/current_universe.csv"
LEADS_JSON_PATH = "frontend/public/leads.json"


# -----------------------
# Helpers
# -----------------------
def clean(value):
    if pd.isna(value):
        return ""
    return str(value).strip()


def ensure_url(url):
    url = clean(url)
    if not url:
        return ""
    if url.lower().startswith("http://") or url.lower().startswith("https://"):
        return url
    return "https://" + url


def choose_column(df, options):
    for col in options:
        if col in df.columns:
            return col
    return None


# -----------------------
# Load universe from SEC file
# -----------------------
def load_universe():
    if not os.path.exists(UNIVERSE_PATH):
        raise FileNotFoundError(f"Missing universe file: {UNIVERSE_PATH}")

    df = pd.read_csv(UNIVERSE_PATH, encoding="latin1", low_memory=False)

    name_col = choose_column(
        df,
        [
            "Primary Business Name",
            "Legal Name",
            "Firm Name",
            "Business Name",
        ],
    )

    website_col = choose_column(
        df,
        [
            "Website Address",
            "Website",
            "Web Address",
        ],
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
    out = out.drop_duplicates(subset=["firm"]).copy()

    return out.to_dict("records")


# -----------------------
# Dummy signal detection for MVP plumbing
# Replace later with real signal engine
# -----------------------
def detect_signal_from_news(firm_name, website=""):
    text = firm_name.lower()

    if any(word in text for word in ["capital", "invest", "asset", "wealth", "advis"]):
        return {
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
        }

    return None


# -----------------------
# Deduplicate
# -----------------------
def dedupe_results(results):
    seen = set()
    deduped = []

    for result in results:
        key = (result["firm"], result["trigger"])
        if key not in seen:
            seen.add(key)
            deduped.append(result)

    return deduped


# -----------------------
# Signal classification
# -----------------------
def infer_signal_type(trigger, evidence):
    text = f"{trigger} {evidence}".lower()

    if "bought" in text or "sold" in text or "etf" in text:
        return "etf_activity", "portfolio"

    if "demand" in text:
        return "demand_signal", "market_demand"

    if "event" in text or "conference" in text or "webinar" in text:
        return "event", "education"

    if "launch" in text or "product" in text or "offering" in text:
        return "product_launch", "offering"

    return "crypto_signal", "general"


# -----------------------
# Convert to dashboard format
# -----------------------
def to_dashboard_lead(result, idx):
    signal_type, signal_category = infer_signal_type(
        result["trigger"], result["evidence"]
    )

    source = result.get("source", "")
    source_label = "source"
    if source:
        try:
            source_label = urlparse(source).netloc.replace("www.", "") or "source"
        except Exception:
            source_label = "source"

    contacts = result.get("contacts", [])
    if not contacts:
        contacts = ["Chief Investment Officer", "Managing Partner"]

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
        "contacts": contacts,
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


# -----------------------
# Write JSON for frontend
# -----------------------
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
    universe = load_universe()
    print(f"Firms: {len(universe)}")

    results = []

    for row in universe[:200]:
        firm_name = row["firm"]
        website = row.get("website", "")
        print(f"Scanning: {firm_name}")

        signal = detect_signal_from_news(firm_name, website)

        if signal:
            results.append(signal)

    results = dedupe_results(results)

    write_leads_json(results)

    print("\n🚀 RIA CRYPTO LEADS — PREVIEW\n")

    if not results:
        print("No strong leads found.")
        return

    for result in results[:20]:
        print(f"{result['priority'].upper()} - {result['firm']}")
        print(f"Trigger: {result['trigger']}")
        print(f"Hook: {result['hook']}")
        print("-" * 40)


# -----------------------
# Entry point
# -----------------------
if __name__ == "__main__":
    run()
