from google.colab import drive
drive.mount('/content/drive', force_remount=True)

import numpy as np
import pandas as pd
import yfinance as yf
import json
import os
from datetime import datetime
import matplotlib.pyplot as plt
from scipy import stats
import warnings
warnings.filterwarnings('ignore')

class SOLVaRCalculator:
    def __init__(self, portfolio_value=100000, num_simulations=50000):
        self.portfolio_value = portfolio_value
        self.num_simulations = num_simulations
        self.ticker = "SOL-USD"
        self.save_dir = "/content/drive/MyDrive/crypto_analysis"
        
        if not os.path.exists(self.save_dir):
            os.makedirs(self.save_dir)
        
        np.random.seed(42)
    
    def fetch_and_calculate(self):
        sol = yf.Ticker(self.ticker)
        df = sol.history(period="2y", interval="1d")[['Close']]
        df.columns = ['price']
        df = df.dropna()
        
        returns = np.log(df['price'] / df['price'].shift(1)).dropna()
        
        mu = returns.mean()
        sigma = returns.std()
        
        random_returns = np.random.normal(mu, sigma, self.num_simulations)
        portfolio_changes = self.portfolio_value * (np.exp(random_returns) - 1)
        
        losses = -portfolio_changes
        var_95 = np.percentile(losses, 95)
        var_99 = np.percentile(losses, 99)
        
        cvar_95 = np.mean(losses[losses >= var_95])
        cvar_99 = np.mean(losses[losses >= var_99])
        
        prob_loss = (np.sum(portfolio_changes < 0) / self.num_simulations) * 100
        
        return {
            'df': df,
            'returns': returns,
            'portfolio_changes': portfolio_changes,
            'stats': {
                'mean_return': mu,
                'std_return': sigma,
                'annual_return': mu * 252,
                'annual_vol': sigma * np.sqrt(252),
                'sharpe': (mu * 252) / (sigma * np.sqrt(252)),
                'observations': len(returns)
            },
            'var': {
                '95': {'value': var_95, 'pct': (var_95/self.portfolio_value)*100, 'cvar': cvar_95},
                '99': {'value': var_99, 'pct': (var_99/self.portfolio_value)*100, 'cvar': cvar_99},
                'prob_loss': prob_loss
            }
        }
    
    def plot_and_save(self, data):
        fig, axes = plt.subplots(2, 3, figsize=(15, 10))
        
        axes[0,0].plot(data['df'].index, data['df']['price'], 'b-', linewidth=1)
        axes[0,0].set_title('SOL-USD Price History')
        axes[0,0].set_xlabel('Date')
        axes[0,0].set_ylabel('Price (USD)')
        axes[0,0].grid(True, alpha=0.3)
        
        axes[0,1].hist(data['returns'], bins=50, alpha=0.7, color='g', edgecolor='black')
        axes[0,1].set_title('Daily Returns Distribution')
        axes[0,1].set_xlabel('Returns')
        axes[0,1].grid(True, alpha=0.3)
        
        axes[0,2].hist(data['portfolio_changes'], bins=100, alpha=0.7, color='orange', edgecolor='black')
        axes[0,2].axvline(0, color='r', linestyle='--', linewidth=2, label='Break-even')
        axes[0,2].axvline(-data['var']['95']['value'], color='b', linestyle='--', label='95% VaR')
        axes[0,2].axvline(-data['var']['99']['value'], color='purple', linestyle='--', label='99% VaR')
        axes[0,2].set_title(f'Monte Carlo ({self.num_simulations:,} simulations)')
        axes[0,2].set_xlabel('Portfolio Change (USD)')
        axes[0,2].legend()
        axes[0,2].grid(True, alpha=0.3)
        
        sorted_changes = np.sort(data['portfolio_changes'])
        cumulative = np.arange(1, len(sorted_changes) + 1) / len(sorted_changes)
        axes[1,0].plot(sorted_changes, cumulative, 'b-', linewidth=2)
        axes[1,0].axhline(0.05, color='orange', linestyle='--', alpha=0.7, label='5%')
        axes[1,0].axhline(0.01, color='r', linestyle='--', alpha=0.7, label='1%')
        axes[1,0].set_title('Cumulative Distribution')
        axes[1,0].set_xlabel('Portfolio Change (USD)')
        axes[1,0].legend()
        axes[1,0].grid(True, alpha=0.3)
        
        stats.probplot(data['returns'], dist="norm", plot=axes[1,1])
        axes[1,1].set_title('Q-Q Plot')
        axes[1,1].grid(True, alpha=0.3)
        
        axes[1,2].axis('off')
        summary = f"""VaR Analysis Results
{'='*25}
Portfolio: ${self.portfolio_value:,.0f}

95% VaR: ${data['var']['95']['value']:,.2f}
        ({data['var']['95']['pct']:.2f}%)
95% CVaR: ${data['var']['95']['cvar']:,.2f}

99% VaR: ${data['var']['99']['value']:,.2f}
        ({data['var']['99']['pct']:.2f}%)
99% CVaR: ${data['var']['99']['cvar']:,.2f}

Loss Probability: {data['var']['prob_loss']:.1f}%"""
        
        axes[1,2].text(0.1, 0.5, summary, fontsize=11, family='monospace', verticalalignment='center')
        
        plt.tight_layout()
        
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        fig_path = f'{self.save_dir}/SOL_VaR_{timestamp}.png'
        plt.savefig(fig_path, dpi=100, bbox_inches='tight')
        plt.show()
        
        save_data = {
            'portfolio_value': self.portfolio_value,
            'simulations': self.num_simulations,
            'timestamp': timestamp,
            'statistics': {k: float(v) for k, v in data['stats'].items()},
            'var_95': {k: float(v) for k, v in data['var']['95'].items()},
            'var_99': {k: float(v) for k, v in data['var']['99'].items()},
            'probability_of_loss': float(data['var']['prob_loss'])
        }
        
        json_path = f'{self.save_dir}/SOL_VaR_{timestamp}.json'
        with open(json_path, 'w') as f:
            json.dump(save_data, f, indent=2)
        
        report = f"""
SOL VaR 分析報告
================
時間: {timestamp}
投資組合: ${self.portfolio_value:,.2f}

【統計數據】
• 日收益率: {data['stats']['mean_return']*100:.3f}%
• 日波動率: {data['stats']['std_return']*100:.3f}%
• 年化收益: {data['stats']['annual_return']*100:.2f}%
• 年化波動: {data['stats']['annual_vol']*100:.2f}%
• 夏普比率: {data['stats']['sharpe']:.3f}

【VaR 結果】
95% 信賴水準:
• VaR: ${data['var']['95']['value']:,.2f} ({data['var']['95']['pct']:.2f}%)
• CVaR: ${data['var']['95']['cvar']:,.2f}

99% 信賴水準:
• VaR: ${data['var']['99']['value']:,.2f} ({data['var']['99']['pct']:.2f}%)
• CVaR: ${data['var']['99']['cvar']:,.2f}

損失機率: {data['var']['prob_loss']:.1f}%
"""
        
        report_path = f'{self.save_dir}/SOL_VaR_{timestamp}.txt'
        with open(report_path, 'w', encoding='utf-8') as f:
            f.write(report)
        
        print(report)
        print(f"\n檔案已儲存至: {self.save_dir}/")
        
        return fig_path, json_path, report_path
    
    def run(self):
        data = self.fetch_and_calculate()
        return self.plot_and_save(data)

calculator = SOLVaRCalculator(portfolio_value=100000, num_simulations=50000)
fig_path, json_path, report_path = calculator.run()
