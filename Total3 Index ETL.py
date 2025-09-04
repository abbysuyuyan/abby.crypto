!pip install requests pandas matplotlib

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
from datetime import datetime

# Historical market cap
def fetch_coin_market_cap_data(coin_id, vs_currency='usd', days=180):
    """Fetches historical market cap for a single coin from CoinGecko API."""
    url = f"https://api.coingecko.com/api/v3/coins/{coin_id}/market_chart"
    params = {'vs_currency': vs_currency, 'days': days, 'interval': 'daily'}

    try:
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

# Global market cap data
def fetch_global_market_cap_data(vs_currency='usd', days=180):
    """Fetches global cryptocurrency market cap data from CoinGecko API."""
    url = f"https://api.coingecko.com/api/v3/global"

    try:
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()['data']

        return pd.DataFrame()

    except requests.exceptions.RequestException as e:
        print(f"Error fetching global data: {e}")
        return pd.DataFrame()

def analyze_crypto_market_revised():
    print("Fetching data for BTC, ETH, and all coins with market cap...")

    # top 250 coins
    url_markets = "https://api.coingecko.com/api/v3/coins/markets"
    params_markets = {
        'vs_currency': 'usd',
        'order': 'market_cap_desc',
        'per_page': 250,
        'page': 1,
        'sparkline': False,
        'price_change_percentage': '7d'
    }

    try:
        response_markets = requests.get(url_markets, params=params_markets)
        response_markets.raise_for_status()
        data_markets = response_markets.json()


        print("Fetching historical data for a select group of major coins...")

        coins_to_fetch = ['bitcoin', 'ethereum', 'solana', 'xrp', 'dogecoin', 'cardano', 'avalanche']

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

        # Null
        pivoted_df = pivoted_df.fillna(0)

        # Calculate market caps
        btc_market_cap = pivoted_df['bitcoin']
        altcoin_market_cap = pivoted_df.drop(columns=['bitcoin', 'ethereum']).sum(axis=1)

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

        # 畫圖
        plt.figure(figsize=(12, 7))
        plt.plot(output_df['Date'], output_df['AltcoinToBTCRatio'])
        plt.title('Total3 Index', fontsize=16)
        plt.xlabel('Time', fontsize=12)
        plt.ylabel('Altcoin / BTC ', fontsize=12)
        plt.grid(True)
        plt.tight_layout()
        plot_filename = "altcoin_to_btc_ratio.png"
        plot_path = os.path.join(output_folder, plot_filename)
        plt.savefig(plot_path)
        print(f"Plot saved to {plot_path}")
        plt.show()

    except requests.exceptions.RequestException as e:
        print(f"An error occurred during market data fetching: {e}")
        return pd.DataFrame()

if __name__ == "__main__":
    analyze_crypto_market_revised()
