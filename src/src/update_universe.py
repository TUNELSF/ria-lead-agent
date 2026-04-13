import os
import io
import zipfile
import requests
import pandas as pd
from bs4 import BeautifulSoup
from urllib.parse import urljoin

SEC_PAGE_URL = "https://www.sec.gov/data-research/sec-markets-data/information-about-registered-investment-advisers-exempt-reporting-advisers"
OUTPUT_PATH = "data/current_universe.csv"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36 your-email@example.com",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,text/csv,application/vnd.openxmlformats-officedocument.spreadsheetml.sheet,application/zip,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://www.sec.gov/",
    "Connection": "keep-alive",
}

session = requests.Session()
session.headers.update(HEADERS)

def fetch(url):
    r = session.get(url, timeout=30, allow_redirects=True)
    r.raise_for_status()
    return r

def find_latest_sec_zip():
    print("Fetching SEC listing page...")
    r = fetch(SEC_PAGE_URL)
    soup = BeautifulSoup(r.text, "html.parser")

    candidates = []
    for a in soup.find_all("a", href=True):
        text = a.get_text(" ", strip=True).lower()
        href = a["href"].strip()
        full_url = urljoin("https://www.sec.gov", href)

        if "registered investment advisers" in text and full_url.lower().endswith(".zip"):
            candidates.append(full_url)

    if not candidates:
        raise Exception("Could not find latest SEC ZIP link")

    latest = candidates[0]
    print("Using SEC ZIP:", latest)
    return latest

def read_zip_to_dataframe(zip_url):
    r = fetch(zip_url)
    z = zipfile.ZipFile(io.BytesIO(r.content))

    for filename in z.namelist():
        lower = filename.lower()

        if lower.endswith(".csv"):
            print("Reading CSV from ZIP:", filename)
            with z.open(filename) as f:
                return pd.read_csv(f, encoding="latin1", low_memory=False)

        if lower.endswith(".xlsx"):
            print("Reading XLSX from ZIP:", filename)
            with z.open(filename) as f:
                return pd.read_excel(f)

    raise Exception("No CSV or XLSX found inside SEC ZIP")

def main():
    os.makedirs("data", exist_ok=True)

    zip_url = find_latest_sec_zip()
    df = read_zip_to_dataframe(zip_url)

    print("Rows:", len(df))
    print("Saving to:", OUTPUT_PATH)

    df.to_csv(OUTPUT_PATH, index=False)
    print("Done.")

if __name__ == "__main__":
    main()
