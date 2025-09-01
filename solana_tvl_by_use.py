import requests, pandas as pd
from datetime import datetime, timezone

from google.colab import drive
drive.mount('/content/drive')

def fetch_json(url):
    r = requests.get(url, timeout=60)
    r.raise_for_status()
    return r.json()

protocols = fetch_json("https://api.llama.fi/protocols")
prot_df = pd.DataFrame(protocols)
mask = prot_df["chains"].apply(lambda xs: "Solana" in xs if isinstance(xs, list) else False)
prot_sol = prot_df[mask].copy()

rows = []
for _, row in prot_sol.iterrows():
    slug = row["slug"] if pd.notna(row.get("slug")) else row["name"]
    cat  = row.get("category", "Unknown")
    try:
        hist = fetch_json(f"https://api.llama.fi/protocol/{slug}")
        chain_tvls = hist.get("chainTvls", {})
        sol_series = chain_tvls.get("Solana", {}).get("tvl", None)
        if not sol_series:
            continue
        df = pd.DataFrame(sol_series)
        df["day"] = pd.to_datetime(df["date"], unit="s", utc=True).dt.date
        df["protocol"] = row["name"]
        df["category"] = cat
        df = df.rename(columns={"totalLiquidityUSD": "tvl_usd"})
        rows.append(df[["day","protocol","category","tvl_usd"]])
    except Exception:
        continue

prot_hist = pd.concat(rows, ignore_index=True) if rows else pd.DataFrame(columns=["day","protocol","category","tvl_usd"])

use_by_day = prot_hist.groupby(["day","category"], as_index=False)["tvl_usd"].sum()

save_path = "/content/drive/MyDrive/solana_tvl_by_use.csv"
use_by_day.to_csv(save_path, index=False)

print(f"已存檔到: {save_path}")
