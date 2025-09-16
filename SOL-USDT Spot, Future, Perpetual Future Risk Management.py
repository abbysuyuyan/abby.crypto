import os
import sys
import time
import json
import base64
import sqlite3
import requests
import logging
import threading
import smtplib
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

print("🔄 Initializing SOL Risk Monitor...")
time.sleep(1)

class Config:
    """系統配置"""
    MONITOR_INTERVAL = 300  # 5分鐘
    DEPTH_STD_THRESHOLD = 1.0
    VRP_MIN_THRESHOLD = 0.01
    SPREAD_MAX_BPS = 20
    EMAIL_COOLDOWN = 1800  # 30分鐘
    
    DB_PATH = "/content/drive/MyDrive/crypto_analysis/sol_risk.db"
    LOG_PATH = "/content/drive/MyDrive/crypto_analysis/sol_risk.log"
    CONFIG_PATH = "/content/drive/MyDrive/crypto_analysis/config.json"
    REPORT_PATH = "/content/drive/MyDrive/crypto_analysis/"

class AutoSetup:  
    @staticmethod
    def mount_drive():
        try:
            from google.colab import drive
            if not os.path.exists('/content/drive'):
                print("📁 Mounting Google Drive...")
                drive.mount('/content/drive', force_remount=False)
                print("✅ Google Drive mounted successfully!")
            else:
                print("✅ Google Drive already mounted")
            return True
        except Exception as e:
            print(f"❌ Failed to mount Drive: {e}")
            return False
    
    @staticmethod
    def load_or_create_config():
        """載入或創建配置文件"""
        os.makedirs(os.path.dirname(Config.CONFIG_PATH), exist_ok=True)
        
        if os.path.exists(Config.CONFIG_PATH):
            try:
                with open(Config.CONFIG_PATH, 'r') as f:
                    config = json.load(f)
                print("✅ Loaded existing configuration")
                
                if 'sender_password_encoded' in config:
                    config['sender_password'] = base64.b64decode(
                        config['sender_password_encoded'].encode()
                    ).decode()
                    
                return config
            except Exception as e:
                print(f"⚠️ Error loading config: {e}")
        
        print("\n🔧 First-time setup required:")
        config = {
            'sender_email': input("Enter Gmail account: "),
            'sender_password': input("Enter Gmail app password: "),
            'recipients': input("Enter recipient emails (comma separated): ").split(',')
        }
        
        config['recipients'] = [email.strip() for email in config['recipients']]
        
        save_config = config.copy()
        save_config['sender_password_encoded'] = base64.b64encode(
            config['sender_password'].encode()
        ).decode()
        del save_config['sender_password']
        
        with open(Config.CONFIG_PATH, 'w') as f:
            json.dump(save_config, f, indent=2)
        
        print("✅ Configuration saved for future use")
        return config

class DataCollector:    
    def __init__(self, logger):
        self.logger = logger
        self.session = self._create_session()
        
    def _create_session(self):
        """創建session"""
        session = requests.Session()
        session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Accept': 'application/json'
        })
        return session
    
    def fetch_with_retry(self, url: str, params: Dict = None, name: str = "API", retries: int = 3) -> Optional[Dict]:
        """重試請求"""
        for attempt in range(retries):
            try:
                response = self.session.get(url, params=params, timeout=10)
                if response.status_code == 200:
                    return response.json()
                self.logger.warning(f"{name} returned status {response.status_code}")
            except Exception as e:
                if attempt < retries - 1:
                    time.sleep(2 * (attempt + 1))
                else:
                    self.logger.error(f"{name} failed: {e}")
        return None
    
    def fetch_orderbook_any_source(self) -> Optional[Dict]:
        """從任何可用源獲取訂單簿"""
        sources = [
            {
                'name': 'KuCoin',
                'url': 'https://api.kucoin.com/api/v1/market/orderbook/level2_100',
                'params': {'symbol': 'SOL-USDT'},
                'parser': lambda d: d.get('data') if d.get('code') == '200000' else None
            },
            {
                'name': 'Gate.io',
                'url': 'https://api.gateio.ws/api/v4/spot/order_book',
                'params': {'currency_pair': 'SOL_USDT', 'limit': 100},
                'parser': lambda d: d if 'bids' in d and 'asks' in d else None
            },
            {
                'name': 'MEXC',
                'url': 'https://api.mexc.com/api/v3/depth',
                'params': {'symbol': 'SOLUSDT', 'limit': 100},
                'parser': lambda d: d if 'bids' in d and 'asks' in d else None
            }
        ]
        
        for source in sources:
            self.logger.info(f"Trying {source['name']}...")
            data = self.fetch_with_retry(source['url'], source['params'], source['name'])
            
            if data:
                parsed = source['parser'](data)
                if parsed:
                    self.logger.info(f"✅ Successfully fetched from {source['name']}")
                    return {'source': source['name'], 'data': parsed}
        
        self.logger.error("❌ All orderbook sources failed")
        return None
    
    def fetch_price_data(self) -> Optional[Dict]:
        """獲取價格數據"""
        try:
            url = "https://api.coingecko.com/api/v3/simple/price"
            params = {
                'ids': 'solana',
                'vs_currencies': 'usd',
                'include_24hr_vol': 'true',
                'include_24hr_change': 'true'
            }
            
            data = self.fetch_with_retry(url, params, "CoinGecko")
            if data and 'solana' in data:
                return {
                    'price': data['solana']['usd'],
                    'volume_24h': data['solana'].get('usd_24h_vol', 0),
                    'change_24h': data['solana'].get('usd_24h_change', 0)
                }
        except Exception as e:
            self.logger.error(f"Price data error: {e}")
        return None

# ==================== 監控系統 ====================
class SOLMonitor:    
    def __init__(self, email_config):
        self.setup_logging()
        self.email_config = email_config
        self.collector = DataCollector(self.logger)
        self.running = False
        self.init_database()
        
    def setup_logging(self):
        """設置日誌"""
        os.makedirs(os.path.dirname(Config.LOG_PATH), exist_ok=True)
        
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(Config.LOG_PATH),
                logging.StreamHandler()
            ]
        )
        self.logger = logging.getLogger(__name__)
    
    def init_database(self):
        """初始化數據庫"""
        os.makedirs(os.path.dirname(Config.DB_PATH), exist_ok=True)
        
        conn = sqlite3.connect(Config.DB_PATH)
        cursor = conn.cursor()
        
        # 創建表格
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS market_data (
            timestamp INTEGER PRIMARY KEY,
            price REAL,
            volume_24h REAL,
            change_24h REAL,
            bid_depth REAL,
            ask_depth REAL,
            total_depth REAL,
            spread_bps REAL,
            source TEXT
        )""")
        
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS alerts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp INTEGER,
            alert_type TEXT,
            message TEXT,
            value REAL,
            threshold REAL,
            email_sent INTEGER DEFAULT 0
        )""")
        
        # 創建索引
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_market_timestamp ON market_data(timestamp DESC)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_alerts_timestamp ON alerts(timestamp DESC)")
        
        conn.commit()
        conn.close()
        self.logger.info("Database initialized")
    
    def calculate_market_depth(self, orderbook_data: Dict) -> Optional[Dict]:
        """計算市場深度"""
        try:
            data = orderbook_data['data']
            source = orderbook_data['source']
            
            bids = [(float(b[0]), float(b[1])) for b in data.get('bids', [])[:50]]
            asks = [(float(a[0]), float(a[1])) for a in data.get('asks', [])[:50]]
            
            if not bids or not asks:
                return None
            
            best_bid = bids[0][0]
            best_ask = asks[0][0]
            mid_price = (best_bid + best_ask) / 2
            
            # 1%深度
            bid_threshold = mid_price * 0.99
            ask_threshold = mid_price * 1.01
            
            bid_depth = sum(p * s for p, s in bids if p >= bid_threshold)
            ask_depth = sum(p * s for p, s in asks if p <= ask_threshold)
            
            spread_bps = ((best_ask - best_bid) / mid_price) * 10000
            
            return {
                'price': mid_price,
                'bid_depth': bid_depth,
                'ask_depth': ask_depth,
                'total_depth': bid_depth + ask_depth,
                'spread_bps': spread_bps,
                'source': source
            }
        except Exception as e:
            self.logger.error(f"Depth calculation error: {e}")
            return None
    
    def check_alerts(self, depth_data: Dict):
        try:
            conn = sqlite3.connect(Config.DB_PATH)
            cutoff = int(time.time()) - 30 * 24 * 3600
            query = "SELECT total_depth FROM market_data WHERE timestamp > ?"
            df = pd.read_sql_query(query, conn, params=(cutoff,))
            
            if len(df) >= 10: 
                # 計算統計值
                mean = df['total_depth'].mean()
                std = df['total_depth'].std()
                threshold = mean - Config.DEPTH_STD_THRESHOLD * std
                current = depth_data['total_depth']
                
                self.logger.info(f"Depth - Current: ${current:,.0f}, Mean: ${mean:,.0f}, Std: ${std:,.0f}, Threshold: ${threshold:,.0f}")
                
                # 深度警報
                if current < threshold:
                    self.send_alert(
                        'DEPTH_DECLINE',
                        f'Market depth ${current:,.0f} below threshold ${threshold:,.0f}',
                        current,
                        threshold
                    )
            else:
                self.logger.info(f"Not enough data for statistics (only {len(df)} records)")
            
            # 價差警報
            if depth_data.get('spread_bps', 0) > Config.SPREAD_MAX_BPS:
                self.send_alert(
                    'WIDE_SPREAD',
                    f'Spread {depth_data["spread_bps"]:.2f} bps exceeds {Config.SPREAD_MAX_BPS} bps',
                    depth_data['spread_bps'],
                    Config.SPREAD_MAX_BPS
                )
            
            # VRP
            if 'change_24h' in depth_data and df.shape[0] > 2:
                # 歷史波動率
                price_query = "SELECT price FROM market_data WHERE timestamp > ? ORDER BY timestamp"
                price_df = pd.read_sql_query(price_query, conn, params=(cutoff,))
                
                if len(price_df) > 2:
                    prices = price_df['price'].values
                    returns = np.diff(np.log(prices))
                    
                    if len(returns) > 0:
                        realized_vol = np.std(returns) * np.sqrt(365 * 24 * 12)  # 年化
                        implied_vol = abs(depth_data.get('change_24h', 0)) / 100 * np.sqrt(365)
                        vrp = implied_vol - realized_vol
                        
                        self.logger.info(f"VRP - IV: {implied_vol*100:.2f}%, RV: {realized_vol*100:.2f}%, VRP: {vrp*100:.2f}%")
                        
                        if vrp < Config.VRP_MIN_THRESHOLD:
                            self.send_alert(
                                'LOW_VRP',
                                f'VRP {vrp*100:.2f}% below {Config.VRP_MIN_THRESHOLD*100}% threshold',
                                vrp,
                                Config.VRP_MIN_THRESHOLD
                            )
            
            conn.close()
            
        except Exception as e:
            self.logger.error(f"Alert check error: {e}")
    
    def send_alert(self, alert_type: str, message: str, value: float, threshold: float):
        """發送警報"""
        try:
            conn = sqlite3.connect(Config.DB_PATH)
            cursor = conn.cursor()
            
            cursor.execute("""
            SELECT MAX(timestamp) FROM alerts 
            WHERE alert_type = ? AND email_sent = 1
            """, (alert_type,))
            
            last_sent = cursor.fetchone()[0]
            current_time = int(time.time())
            
            cursor.execute("""
            INSERT INTO alerts (timestamp, alert_type, message, value, threshold, email_sent)
            VALUES (?, ?, ?, ?, ?, ?)
            """, (current_time, alert_type, message, value, threshold, 0))
            conn.commit()
            
            if last_sent and (current_time - last_sent) < Config.EMAIL_COOLDOWN:
                self.logger.info(f"Alert {alert_type} in cooldown period")
                conn.close()
                return
            
            msg = MIMEMultipart()
            msg['Subject'] = f"🚨 SOL Alert: {alert_type}"
            msg['From'] = self.email_config['sender_email']
            msg['To'] = ', '.join(self.email_config['recipients'])
            
            html = f"""
            <html>
            <body style="font-family: Arial; padding: 20px;">
                <h2 style="color: #e74c3c;">🚨 SOL Risk Alert</h2>
                <div style="background: #f8f9fa; padding: 15px; border-radius: 5px;">
                    <p><strong>Type:</strong> {alert_type.replace('_', ' ')}</p>
                    <p><strong>Message:</strong> {message}</p>
                    <p><strong>Current Value:</strong> {value:,.2f}</p>
                    <p><strong>Threshold:</strong> {threshold:,.2f}</p>
                    <p><strong>Time:</strong> {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} UTC+8</p>
                </div>
                <div style="margin-top: 20px;">
                    <h3>Recommended Actions:</h3>
                    <ul>
                        <li>Reduce leverage to below 10x</li>
                        <li>Use limit orders instead of market orders</li>
                        <li>Monitor liquidation levels closely</li>
                    </ul>
                </div>
            </body>
            </html>
            """
            
            msg.attach(MIMEText(html, 'html'))
            
            server = smtplib.SMTP('smtp.gmail.com', 587)
            server.starttls()
            server.login(self.email_config['sender_email'], self.email_config['sender_password'])
            server.send_message(msg)
            server.quit()
            
            cursor.execute("""
            UPDATE alerts SET email_sent = 1 
            WHERE timestamp = ? AND alert_type = ?
            """, (current_time, alert_type))
            conn.commit()
            conn.close()
            
            self.logger.info(f"✅ Alert email sent: {alert_type}")
            
        except Exception as e:
            self.logger.error(f"Alert error: {e}")
    
    def run_cycle(self):
        """執行監控循環"""
        try:
            self.logger.info("=" * 50)
            self.logger.info("Starting monitoring cycle...")
            
            orderbook = self.collector.fetch_orderbook_any_source()
            if not orderbook:
                self.logger.warning("No orderbook data available")
                return
            
            depth_data = self.calculate_market_depth(orderbook)
            if not depth_data:
                self.logger.warning("Failed to calculate market depth")
                return
            
            price_data = self.collector.fetch_price_data()
            if price_data:
                depth_data['volume_24h'] = price_data.get('volume_24h', 0)
                depth_data['change_24h'] = price_data.get('change_24h', 0)
            
            conn = sqlite3.connect(Config.DB_PATH)
            cursor = conn.cursor()
            
            cursor.execute("""
            INSERT INTO market_data 
            (timestamp, price, volume_24h, change_24h, bid_depth, ask_depth, total_depth, spread_bps, source)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                int(time.time()),
                depth_data['price'],
                depth_data.get('volume_24h', 0),
                depth_data.get('change_24h', 0),
                depth_data['bid_depth'],
                depth_data['ask_depth'],
                depth_data['total_depth'],
                depth_data['spread_bps'],
                depth_data['source']
            ))
            
            conn.commit()
            conn.close()
            
            self.check_alerts(depth_data)
            
            self.logger.info(f"✅ Cycle complete")
            self.logger.info(f"   Price: ${depth_data['price']:.2f}")
            self.logger.info(f"   Depth: ${depth_data['total_depth']:,.0f}")
            self.logger.info(f"   Spread: {depth_data['spread_bps']:.2f} bps")
            self.logger.info(f"   Source: {depth_data['source']}")
            
        except Exception as e:
            self.logger.error(f"Cycle error: {e}", exc_info=True)
    
    def start(self):
        """啟動監控"""
        self.running = True
        
        def loop():
            while self.running:
                self.run_cycle()
                time.sleep(Config.MONITOR_INTERVAL)
        
        thread = threading.Thread(target=loop, daemon=True)
        thread.start()
        
        self.logger.info("Monitor started")
        
        try:
            from IPython.display import Javascript, display
            display(Javascript('''
            setInterval(() => {
                console.log("Keep alive");
            }, 60000);
            '''))
        except:
            pass
    
    def stop(self):
        self.running = False
        self.logger.info("Monitor stopped")

def main():
    print("=" * 60)
    print("SOL Risk Monitor - Fixed Version")
    print("=" * 60)
    
    if not AutoSetup.mount_drive():
        print("❌ Cannot proceed without Google Drive")
        return
    
    config = AutoSetup.load_or_create_config()
    
    monitor = SOLMonitor(config)
    monitor.start()
    
    print("\n✅ System Started Successfully!")
    print(f"📊 Monitor interval: {Config.MONITOR_INTERVAL//60} minutes")
    print(f"📧 Alert cooldown: {Config.EMAIL_COOLDOWN//60} minutes")
    print(f"💾 Database: {Config.DB_PATH}")
    print(f"📝 Logs: {Config.LOG_PATH}")
    print("\n⚠️ Risk Thresholds:")
    print(f"   - Market Depth: < Mean - {Config.DEPTH_STD_THRESHOLD} std")
    print(f"   - VRP: < {Config.VRP_MIN_THRESHOLD*100}%")
    print(f"   - Spread: > {Config.SPREAD_MAX_BPS} bps")
    print("\n📌 Monitor is running in background...")
    print("Press Ctrl+C to stop\n")
    
    try:
        while True:
            time.sleep(60)
            remaining = Config.MONITOR_INTERVAL - (time.time() % Config.MONITOR_INTERVAL)
            print(f"[{datetime.now().strftime('%H:%M:%S')}] Running... Next check in {remaining:.0f}s")
    except KeyboardInterrupt:
        print("\n\n🛑 Stopping monitor...")
        monitor.stop()
        print("✅ Monitor stopped successfully")

if __name__ == "__main__":
    main()
