# ============================================================
# thunderball_last_5_years.py
# ------------------------------------------------------------
# Downloads Thunderball results for the past 5 years (incl. current year)
# from lottery.co.uk archive pages and saves CSV + Excel into:
#   C:\Users\shonk\source\PythonCodes\Lottery\data\
#
# Install:
#   pip install requests beautifulsoup4 pandas openpyxl lxml
# Run:
#   python thunderball_last_5_years.py
# ============================================================

from __future__ import annotations

from pathlib import Path
from datetime import datetime
import re
import sys

import pandas as pd
import requests
from bs4 import BeautifulSoup


# -------------------------
# CONFIG
# -------------------------
BASE_DIR = Path(r"C:\Users\shonk\source\PythonCodes\Lottery")
OUT_DIR = BASE_DIR / "data"
OUT_DIR.mkdir(parents=True, exist_ok=True)

GAME = "thunderball"
ARCHIVE_URL = f"https://www.lottery.co.uk/{GAME}/results/archive-{{year}}"

# Example date format on site: "Wednesday 31st December 2025"
DATE_RE = re.compile(
    r"^(Monday|Tuesday|Wednesday|Thursday|Friday|Saturday|Sunday)\s+\d{{1,2}}(st|nd|rd|th)\s+\w+\s+{year}$",
    re.IGNORECASE,
)


def normalise_date(date_text: str) -> str:
    """
    'Wednesday 31st December 2025' -> '2025-12-31'
    """
    # remove st/nd/rd/th
    s2 = re.sub(r"(\d{1,2})(st|nd|rd|th)", r"\1", date_text.strip(), flags=re.IGNORECASE)
    dt = datetime.strptime(s2, "%A %d %B %Y")
    return dt.strftime("%Y-%m-%d")


def fetch_html(url: str) -> str:
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                      "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    }
    r = requests.get(url, headers=headers, timeout=45)
    r.raise_for_status()
    return r.text


def extract_year(year: int) -> pd.DataFrame:
    url = ARCHIVE_URL.format(year=year)
    html = fetch_html(url)
    soup = BeautifulSoup(html, "lxml")

    # Approach: grab all strings and parse sequences:
    # Date -> 5 main numbers -> Thunderball -> Winners
    tokens = [s.strip() for s in soup.stripped_strings if s and s.strip()]

    # de-duplicate consecutive duplicates but keep order
    cleaned = []
    prev = None
    for t in tokens:
        if t == prev:
            continue
        cleaned.append(t)
        prev = t

    rows = []
    i = 0
    n = len(cleaned)
    date_re = re.compile(DATE_RE.pattern.format(year=year), re.IGNORECASE)

    while i < n:
        t = cleaned[i]

        if date_re.match(t):
            draw_date_text = t

            nums = []
            winners = None

            j = i + 1
            while j < n and (len(nums) < 6 or winners is None):
                tj = cleaned[j]

                # main numbers / thunderball often appear as small integers
                if re.fullmatch(r"\d{1,2}", tj):
                    if len(nums) < 6:
                        nums.append(int(tj))
                # winners often appear with commas
                elif re.fullmatch(r"\d{1,3}(?:,\d{3})*", tj):
                    # could still be a ball number if <= 39, but winners usually bigger
                    val = int(tj.replace(",", ""))
                    if len(nums) < 6 and val <= 99:
                        nums.append(val)
                    else:
                        winners = val
                # sometimes numbers appear in one token: "15 18 20 23 27 8"
                elif re.fullmatch(r"(?:\d{1,2}\s+){5}\d{1,2}", tj):
                    parts = [int(x) for x in tj.split()]
                    for p in parts:
                        if len(nums) < 6:
                            nums.append(p)

                j += 1

            if len(nums) == 6:
                n1, n2, n3, n4, n5, tb = nums
                rows.append(
                    {
                        "DrawDateText": draw_date_text,
                        "DrawDate": normalise_date(draw_date_text),
                        "N1": n1,
                        "N2": n2,
                        "N3": n3,
                        "N4": n4,
                        "N5": n5,
                        "Thunderball": tb,
                        "Winners": winners,
                        "Source": url,
                    }
                )
                i = j
                continue

        i += 1

    if not rows:
        raise RuntimeError(
            f"No rows extracted for {year}. The site layout may have changed.\nURL: {url}"
        )

    df = pd.DataFrame(rows)

    # sort ascending by date
    df["DrawDate"] = pd.to_datetime(df["DrawDate"])
    df = df.sort_values("DrawDate").reset_index(drop=True)
    df["DrawDate"] = df["DrawDate"].dt.strftime("%Y-%m-%d")

    return df


def main() -> None:
    current_year = datetime.now().year
    years = list(range(current_year - 4, current_year + 1))  # past 5 years inclusive

    print(f"Saving to: {OUT_DIR}")
    print(f"Years: {years}")

    for y in years:
        try:
            df = extract_year(y)
            out_csv = OUT_DIR / f"Thunderball_{y}.csv"
            out_xlsx = OUT_DIR / f"Thunderball_{y}.xlsx"

            df.to_csv(out_csv, index=False, encoding="utf-8-sig")
            df.to_excel(out_xlsx, index=False)

            print(f"✅ {y}: {len(df)} rows -> {out_xlsx.name}")
        except Exception as e:
            print(f"❌ {y}: failed -> {e}")

    print("Done.")


if __name__ == "__main__":
    main()