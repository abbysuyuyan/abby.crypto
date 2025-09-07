from google.colab import drive
import os

drive.mount('/content/drive')

output_folder = '/content/drive/My Drive/crypto_analysis'

if not os.path.exists(output_folder):
    os.makedirs(output_folder)
    print(f"Created folder: {output_folder}")
else:
    print(f"Folder already exists: {output_folder}")

import requests
import pandas as pd
import matplotlib.pyplot as plt
import time
from datetime import datetime

def fetch_coin_market_cap_data(coin_id, vs_currency='usd', days=180):
    """Fetches historical market cap data for a given cryptocurrency from CoinGecko API."""
    url = f"https://api.coingecko.com/api/v3/coins/{coin_id}/market_chart"
    params = {'vs_currency': vs_currency, 'days': days, 'interval': 'daily'}
    
    try:
        time.sleep(2.5) 
        response = requests.get(url, params=params)
        response.raise_for_status()
        data = response.json()
        
        market_caps = data['market_caps']
        df = pd.DataFrame(market_caps, columns=['timestamp', 'market_cap'])
        df['date'] = pd.to_datetime(df['timestamp'], unit='ms').dt.date
        
        return df[['date', 'market_cap']]
    
    except requests.exceptions.RequestException as e:
        print(f"Error fetching data for {coin_id}: {e}")
        return pd.DataFrame()

def analyze_crypto_market_revised():
    print("Fetching historical data for a select group of major coins...")
    
    coins_to_fetch = ['bitcoin', 'ethereum', 'solana', 'ripple', 'dogecoin']
    
    all_data = []
    for coin_id in coins_to_fetch:
        df = fetch_coin_market_cap_data(coin_id, days=180)
        if not df.empty:
            df['coin_id'] = coin_id
            all_data.append(df)
    
    if not all_data:
        print("Failed to fetch data for any coins. Aborting.")
        return

    full_df = pd.concat(all_data)
    
    pivoted_df = full_df.pivot_table(index='date', columns='coin_id', values='market_cap')
    pivoted_df = pivoted_df.fillna(0)
    
    if 'bitcoin' not in pivoted_df.columns:
        print("Error: Could not fetch Bitcoin data. Aborting.")
        return

    btc_market_cap = pivoted_df['bitcoin']
    
    altcoin_cols = [col for col in pivoted_df.columns if col not in ['bitcoin', 'ethereum']]
    altcoin_market_cap = pivoted_df[altcoin_cols].sum(axis=1)

    ratio_df = pd.DataFrame({
        'date': btc_market_cap.index,
        'altcoin_to_btc_ratio': altcoin_market_cap / btc_market_cap
    }).dropna()

    csv_filename = "altcoin_to_btc_ratio.csv"
    csv_path = os.path.join(output_folder, csv_filename)
    output_df = ratio_df.copy()
    output_df.rename(columns={'date': 'Date', 'altcoin_to_btc_ratio': 'AltcoinToBTCRatio'}, inplace=True)
    output_df.to_csv(csv_path, index=False)
    print(f"Data saved to {csv_path}")

    plt.figure(figsize=(12, 7))
    plt.plot(output_df['Date'], output_df['AltcoinToBTCRatio'])
    plt.title('Total3 Index', fontsize=16)
    plt.xlabel('Date', fontsize=12)
    plt.ylabel('Altcoin / BTC ', fontsize=12)
    plt.grid(True)
    plt.tight_layout()
    plot_filename = "altcoin_to_btc_ratio.png"
    plot_path = os.path.join(output_folder, plot_filename)
    plt.savefig(plot_path)
    print(f"Plot saved to {plot_path}")
    plt.show()

if __name__ == "__main__":
    analyze_crypto_market_revised()
