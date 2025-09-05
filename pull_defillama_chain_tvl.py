!pip install requests -q

import requests, csv, datetime, time
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from google.colab import drive

drive.mount('/content/drive')

URL_CHAINS = "https://api.llama.fi/v2/chains"
BASE = "https://api.llama.fi/v2/historicalChainTvl/{}"
OUT = "/content/defillama_chain_tvl_top30.csv"
OUT_DRIVE = "/content/drive/MyDrive/crypto_analysis/defillama_chain_tvl_top30.csv"

def make_session():
    s = requests.Session()
    s.headers.update({"User-Agent": "abby-dune-tvl-fetcher/1.0"})
    retry_kwargs = dict(total=3, connect=3, read=3, backoff_factor=1.0,
                        status_forcelist=[429, 500, 502, 503, 504],
                        raise_on_status=False)
    try:
        retry = Retry(**retry_kwargs, allowed_methods=frozenset(["GET"]))
    except TypeError:
        retry = Retry(**retry_kwargs, method_whitelist=frozenset(["GET"]))
    s.mount("https://", HTTPAdapter(max_retries=retry))
    return s

chains_data = requests.get(URL_CHAINS).json()
top_chains = sorted(chains_data, key=lambda x: x.get("tvl", 0), reverse=True)[:30]
CHAIN_NAMES = [c["name"].lower() for c in top_chains]
print("Top 30 chains by TVL:", CHAIN_NAMES)

session = make_session()
all_rows = []
for chain in CHAIN_NAMES:
    print(f"Fetching {chain} ...")
    try:
        data = session.get(BASE.format(chain), timeout=60).json()
        for p in data:
            ts, tvl = p.get("date"), p.get("tvl")
            if ts is None or tvl is None: continue
            day = datetime.datetime.utcfromtimestamp(int(ts)).date().isoformat()
            all_rows.append((day, chain, float(tvl)))
    except Exception as e:
        print(f"[WARN] {chain} failed: {e}")
    time.sleep(0.5)

dedup = {}
for d,c,v in all_rows:
    dedup[(d,c)] = v
rows = [(d,c,v) for (d,c),v in sorted(dedup.items())]

with open(OUT, "w", newline="", encoding="utf-8") as f:
    w = csv.writer(f)
    w.writerow(["day","chain","tvl_usd"])
    w.writerows(rows)
print(f"Saved {OUT} with {len(rows)} rows.")

!cp $OUT $OUT_DRIVE
print(f"Also copied to Google Drive: {OUT_DRIVE}")
