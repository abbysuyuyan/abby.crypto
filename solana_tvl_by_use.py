import requests, time, pandas as pd
from datetime import datetime, date, timezone
from google.colab import drive

drive.mount('/content/drive')

OUTPUT_PATH = "/content/drive/MyDrive/solana_defillama_protocol_tvl.csv"
REQUEST_PAUSE_S = 0.25
START_DAY = None

def fetch_json(url, retries=3, timeout=60):
    for i in range(retries):
        try:
            r = requests.get(url, timeout=timeout)
            r.raise_for_status()
            return r.json()
        except Exception:
            if i == retries - 1: raise
            time.sleep(1.0 + i)
    return None

def to_day(ts):
    return datetime.fromtimestamp(int(ts), tz=timezone.utc).date()

protocols = fetch_json("https://api.llama.fi/protocols")
prot_df = pd.DataFrame(protocols)

is_solana = prot_df["chains"].apply(lambda xs: isinstance(xs, list) and ("Solana" in xs))
prot_sol = prot_df[is_solana].copy()

rows = []
for _, row in prot_sol.iterrows():
    slug = row["slug"] if pd.notna(row.get("slug")) else row["name"]
    category = row.get("category", "Unknown")
    try:
        data = fetch_json(f"https://api.llama.fi/protocol/{slug}")
        series = (data or {}).get("chainTvls", {}).get("Solana", {}).get("tvl", None)
        if not series:
            time.sleep(REQUEST_PAUSE_S); continue
        df = pd.DataFrame(series)
        if df.empty: 
            time.sleep(REQUEST_PAUSE_S); continue
        df["day"] = df["date"].apply(to_day)
        if START_DAY:
            df = df[df["day"] >= START_DAY]
        df["protocol"] = row["name"]
        df["category"] = category
        df.rename(columns={"totalLiquidityUSD":"tvl_usd"}, inplace=True)
        rows.append(df[["day","protocol","category","tvl_usd"]])
    except Exception:
        pass
    finally:
        time.sleep(REQUEST_PAUSE_S)

prot_hist = (pd.concat(rows, ignore_index=True)
             if rows else pd.DataFrame(columns=["day","protocol","category","tvl_usd"]))

prot_hist = prot_hist.dropna(subset=["day","protocol","category","tvl_usd"])
prot_hist = prot_hist[prot_hist["tvl_usd"] >= 0].copy()

cat_hist = prot_hist.groupby(["day","category"], as_index=False)["tvl_usd"].sum()
cat_hist["protocol"] = "ALL_PROTOCOLS" 
full_hist = pd.concat([prot_hist, cat_hist], ignore_index=True)

full_hist.to_csv(OUTPUT_PATH, index=False)

print(f" 已存檔: {OUTPUT_PATH}")
print("Preview:")
print(full_hist.head(10))
