import os
import json
import re
import requests
from bs4 import BeautifulSoup
import pandas as pd
from io import BytesIO

def clean_bank_name(name):
    if not name or pd.isna(name):
        return ""
    # Strip double spaces and standardize capitalization
    s = str(name).strip().upper()
    s = re.sub(r'\s+', ' ', s)
    # Map key variants to full stacked words
    if "HDFC" in s: return "HDFC BANK"
    if "STATE BANK OF INDIA" in s or "SBI" in s: return "STATE BANK OF INDIA"
    if "ICICI" in s: return "ICICI BANK"
    if "AXIS" in s: return "AXIS BANK"
    if "KOTAK" in s: return "KOTAK MAHINDRA BANK"
    if "IDFC" in s: return "IDFC FIRST BANK"
    if "RBL" in s: return "RBL BANK"
    if "YES" in s: return "YES BANK"
    if "FEDERAL" in s: return "FEDERAL BANK"
    if "AMERICAN EXPRESS" in s: return "AMERICAN EXPRESS"
    return s

def extract_month_year(text):
    months = ["january", "february", "march", "april", "may", "june", "july", "august", "september", "october", "november", "december"]
    text_lower = text.lower()
    found_m = None
    for m in months:
        if m in text_lower:
            found_m = m.capitalize()[:3] # Standardize to Jan, Feb, Mar...
            break
    year_match = re.search(r'\b(20\d{2})\b', text)
    if found_m and year_match:
        return f"{found_m} {year_match.group(1)}"
    return None

def scrape_rbi_rolling_window():
    print("🚀 Running RBI Rolling Data Ingestion Pipeline...")
    portal_url = "https://rbi.org.in/scripts/atmview.aspx"
    base_url = "https://rbi.org.in"
    
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    }
    
    try:
        r = requests.get(portal_url, headers=headers, timeout=30)
        soup = BeautifulSoup(r.text, 'html.parser')
    except Exception as e:
        print(f"❌ Portal inaccessible: {e}")
        return

    # Find all anchor Excel links matching structural card templates
    excel_anchors = []
    for a in soup.find_all('a', href=True):
        link_text = a.get_text()
        period_key = extract_month_year(link_text)
        if period_key and (".xls" in a['href'].lower() or "xlsx" in a['href'].lower()):
            excel_anchors.append({
                'period': period_key,
                'url': urljoin(base_url, a['href']) if not a['href'].startswith('http') else a['href']
            })

    # Deduplicate and pick the latest 12 entries chronologically
    seen_periods = set()
    unique_anchors = []
    for anchor in excel_anchors:
        if anchor['period'] not in seen_periods:
            seen_periods.add(anchor['period'])
            unique_anchors.append(anchor)
    
    # Sort or limit to modern 12 rolling tracks
    unique_anchors = unique_anchors[:12]
    print(f"📌 Found {len(unique_anchors)} rolling reporting cycles targeting extraction.")

    # Core Database Schema Initialization
    database = {"months": [], "history": {}}

    for item in reversed(unique_anchors): # Process chronological oldest to newest
        mo = item['period']
        print(f"   Downloading: {mo} -> {item['url']}")
        try:
            res = requests.get(item['url'], headers=headers, timeout=30)
            df = pd.read_excel(BytesIO(res.content), header=None)
            
            current_sector = "Unknown"
            records = []
            
            for idx, row in df.iterrows():
                if idx < 5 or len(row) < 15: continue
                c1 = str(row[1]).strip()
                c2 = str(row[2]).strip()
                
                if "Public Sector Banks" in c1: current_sector = "Public"
                elif "Private Sector Banks" in c1: current_sector = "Private"
                elif "Foreign Banks" in c1: current_sector = "Foreign"
                elif "Small Finance Banks" in c1: current_sector = "Small Finance"
                
                if c1.isdigit() and c2 != 'nan' and c2 != '':
                    def val(v):
                        if pd.isna(v): return 0.0
                        try: return float(str(v).replace(',', '').strip())
                        except: return 0.0

                    cards = int(val(row[9]))
                    pos_t = int(val(row[5]))
                    pos_vol = int(val(row[11]))
                    pos_val = float(val(row[12]))
                    online_vol = int(val(row[13]))
                    online_val = float(val(row[14]))
                    atm_vol = int(val(row[17]))
                    atm_val = float(val(row[18]))
                    
                    records.append({
                        "name": clean_bank_name(c2),
                        "category": current_sector,
                        "cards": cards,
                        "pos_terminals": pos_t,
                        "pos_vol": pos_vol,
                        "pos_val": pos_val,
                        "online_vol": online_vol,
                        "online_val": online_val,
                        "atm_vol": atm_vol,
                        "atm_val": atm_val
                    })
            
            if records:
                database["months"].append(mo)
                database["history"][mo] = records
                print(f"      ✅ Ingested {len(records)} banking profiles successfully.")
        except Exception as err:
            print(f"      ❌ Skip period processing error: {err}")

    # Enforce strict 12-month sliding window eviction constraint
    if len(database["months"]) > 12:
        evict_count = len(database["months"]) - 12
        print(f"🧹 Evicting {evict_count} obsolete archival periods from database array...")
        for old_mo in database["months"][:evict_count]:
            if old_mo in database["history"]:
                del database["history"][old_mo]
        database["months"] = database["months"][evict_count:]

    # Write out unified immutable JSON layer asset file
    with open('data.json', 'w') as out_file:
        json.dump(database, out_file, indent=2)
    print("🎉 Live data.json matrix sync finalized successfully.")

if __name__ == "__main__":
    scrape_rbi_rolling_window()