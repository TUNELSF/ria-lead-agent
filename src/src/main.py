import os
import json
import pandas as pd
from urllib.parse import urlparse

UNIVERSE_PATH = "src/src/data/current_universe.csv"
LEADS_JSON_PATH = "frontend/public/leads.json"


# -----------------------
# Load universe
# -----------------------
def load_universe():
    df = pd.read_csv(UNIVERSE_PATH)
    return df["firm"].dropna().tolist()


# -----------------------
# Dummy signal detection (replace later with your real logic)
# -----------------------
def detect_signal_from_news(firm_name):
    # This is placeholder logic so system works end-to-end
    # Replace with your real scraping / media logic

    if "capital" in firm_name.lower():
        return {
            "firm": firm_name,
            "priority": "medium",
            "trigger": "Portfolio activity involving crypto-related instruments detected in recent media",
            "why_now": "Recent ETF or portfolio signal tied to digital assets relevance",
            "source": "https://example.com/article",
            "source_date": "2026-04-01",
            "evidence": f"{firm_name} mentioned in relation to crypto ETF activity",
            "hook": "Saw the recent ETF activity — curious whether digital assets are becoming more relevant in portfolio construction conversations.",
            "contacts": ["Chief Investment Officer", "Managing Partner"]
        }

    return None


# -----------------------
# Deduplicate
# -----------------------
def dedupe_results(results):
    seen = set()
    deduped = []

    for r in results:
        key = (r["firm"], r["trigger"])
        if key not in seen:
            seen.add(key)
            deduped.append(r)

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

    if "event" in text or "conference" in text:
        return "event", "education"

    if "launch" in text or "product" in text:
        return "product_launch", "offering"

    return "crypto_signal", "general"


# -----------------------
# Convert to dashboard format
# -----------------------
def to_dashboard_lead(result, idx):
    signal_type, signal_category = infer_signal_type(
        result["trigger"], result["evidence"]
    )

    return {
        "id": str(idx + 1),
        "firm": result["firm"],
        "priority": result["priority"],
        "signal_type": signal_type,
        "signal_category": signal_category,
        "trigger": result["trigger"],
        "why_now": result["why_now"],
        "source": result["source"],
        "source_label": urlparse(result["source"]).netloc.replace("www.", ""),
        "source_date": result["source_date"],
        "evidence": result["evidence"],
        "hook": result["hook"],
        "contacts": result["contacts"],
        "status": "new",
        "firm_profile": {
            "aum": None,
            "aum_bucket": "unknown",
            "growth_1y": None,
            "asset_focus": [],
            "product_types": [],
            "bd_affiliated": None,
            "independence": "unknown"
        }
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
# Main run function
# -----------------------
def run():
    universe = load_universe()
    print(f"Firms: {len(universe)}")

    results = []

    for firm_name in universe[:200]:  # limit for MVP speed
        print(f"Scanning: {firm_name}")

        signal = detect_signal_from_news(firm_name)

        if signal:
            results.append(signal)

    results = dedupe_results(results)

    write_leads_json(results)

    print("\n🚀 RIA CRYPTO LEADS — PREVIEW\n")

    if not results:
        print("No strong leads found.")
        return

    for r in results:
        print(f"{r['priority'].upper()} - {r['firm']}")
        print(f"Trigger: {r['trigger']}")
        print(f"Hook: {r['hook']}")
        print("-" * 40)


# -----------------------
# Entry point
# -----------------------
if __name__ == "__main__":
    run()
