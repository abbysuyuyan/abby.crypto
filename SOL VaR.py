from google.colab import drive
drive.mount('/content/drive')

import numpy as np
import pandas as pd
import yfinance as yf
import json
import os
from datetime import datetime, timedelta
import matplotlib.pyplot as plt
import warnings
warnings.filterwarnings('ignore')

plt.rcParams['font.sans-serif'] = ['DejaVu Sans']
plt.rcParams['axes.unicode_minus'] = False

class SOLVaRCalculator: 
    def __init__(self, portfolio_value=100000, num_simulations=50000):

        self.portfolio_value = portfolio_value
        self.num_simulations = num_simulations
        self.ticker = "SOL-USD"
        self.save_dir = "/content/drive/MyDrive/crypto_analysis"
        
        if not os.path.exists(self.save_dir):
            os.makedirs(self.save_dir)
            print(f"✅ 已創建儲存目錄: {self.save_dir}")
        
        np.random.seed(42)
        
        print("="*60)
        print("🚀 SOL 幣 VaR 計算器已初始化")
        print(f"📊 投資組合價值: ${portfolio_value:,.2f}")
        print(f"🎯 模擬次數: {num_simulations:,}")
        print(f"📈 數據來源: Yahoo Finance ({self.ticker})")
        print(f"💾 儲存路徑: {self.save_dir}")
        print("="*60)
    
    def fetch_price_data(self):
        print("\n 正在從 Yahoo Finance 下載數據")
        
        try:
            sol = yf.Ticker(self.ticker)
            
            df = sol.history(period="2y", interval="1d")
            
            if df.empty:
                raise ValueError("無法獲取數據")
            
            df = df[['Close']].rename(columns={'Close': 'price'})
            
            df = df.dropna()
            df = df[df['price'] > 0]
            
            print(f"✅ 成功獲取 {len(df)} 個交易日的數據")
            print(f"📅 數據期間: {df.index[0].strftime('%Y-%m-%d')} 至 {df.index[-1].strftime('%Y-%m-%d')}")
            print(f"💰 當前價格: ${df['price'].iloc[-1]:.2f}")
            print(f"📈 價格範圍: ${df['price'].min():.2f} - ${df['price'].max():.2f}")
            
            return df
            
        except Exception as e:
            print(f"❌ 獲取數據失敗: {str(e)}")
            return None
    
    def calculate_statistics(self, df):
        print("\n🧮 計算統計參數...")
        
        returns = np.log(df['price'] / df['price'].shift(1)).dropna()
        
        mean_return = returns.mean()
        std_return = returns.std()
        
        annual_mean = mean_return * 252
        annual_vol = std_return * np.sqrt(252)
        
        skewness = returns.skew()
        kurtosis = returns.kurtosis()
        sharpe = annual_mean / annual_vol if annual_vol != 0 else 0
        
        cumulative = (1 + returns).cumprod()
        running_max = cumulative.expanding().max()
        drawdown = (cumulative - running_max) / running_max
        max_drawdown = drawdown.min()
        
        stats = {
            'returns': returns,
            'mean_return': mean_return,
            'std_return': std_return,
            'annual_mean': annual_mean,
            'annual_vol': annual_vol,
            'skewness': skewness,
            'kurtosis': kurtosis,
            'sharpe_ratio': sharpe,
            'max_drawdown': max_drawdown,
            'num_obs': len(returns)
        }
        
        print(f"📊 統計結果:")
        print(f"  • 日平均收益率: {mean_return*100:.3f}%")
        print(f"  • 日波動率: {std_return*100:.3f}%")
        print(f"  • 年化收益率: {annual_mean*100:.2f}%")
        print(f"  • 年化波動率: {annual_vol*100:.2f}%")
        print(f"  • 夏普比率: {sharpe:.3f}")
        print(f"  • 最大回撤: {max_drawdown*100:.2f}%")
        print(f"  • 偏度: {skewness:.3f}")
        print(f"  • 峰度: {kurtosis:.3f}")
        
        return stats
    
    def monte_carlo_simulation(self, stats):
        print(f"\n🎰 執行蒙地卡羅模擬 ({self.num_simulations:,} 次)...")
        
        mu = stats['mean_return']
        sigma = stats['std_return']
        
        np.random.seed(42)
        random_returns = np.random.normal(mu, sigma, self.num_simulations)
        
        portfolio_values = self.portfolio_value * np.exp(random_returns)
        portfolio_changes = portfolio_values - self.portfolio_value
        
        results = {
            'portfolio_values': portfolio_values,
            'portfolio_changes': portfolio_changes,
            'mean_change': np.mean(portfolio_changes),
            'std_change': np.std(portfolio_changes),
            'worst_loss': np.min(portfolio_changes),
            'best_gain': np.max(portfolio_changes)
        }
        
        print(f"✅ 模擬完成")
        print(f"  • 平均損益: ${results['mean_change']:,.2f}")
        print(f"  • 標準差: ${results['std_change']:,.2f}")
        print(f"  • 最大損失: ${results['worst_loss']:,.2f}")
        print(f"  • 最大收益: ${results['best_gain']:,.2f}")
        
        return results
    
    def calculate_var(self, simulation_results):
        print("\n📊 計算風險價值 (VaR)...")
        
        portfolio_changes = simulation_results['portfolio_changes']
        losses = -portfolio_changes  
        
        var_results = {}
        
        for confidence_level in [0.95, 0.99]:
            var_value = np.percentile(losses, confidence_level * 100)
            
            tail_losses = losses[losses >= var_value]
            cvar_value = np.mean(tail_losses) if len(tail_losses) > 0 else var_value
            
            var_pct = (var_value / self.portfolio_value) * 100
            cvar_pct = (cvar_value / self.portfolio_value) * 100
            
            var_results[f'{int(confidence_level*100)}%'] = {
                'var_value': var_value,
                'var_percentage': var_pct,
                'cvar_value': cvar_value,
                'cvar_percentage': cvar_pct
            }
        
        prob_loss = (np.sum(portfolio_changes < 0) / self.num_simulations) * 100
        
        print("\n" + "="*60)
        print("🎯 風險價值 (VaR) 計算結果:")
        print("="*60)
        
        print(f"\n📉 95% 信賴水準下的單日 VaR 為 ${var_results['95%']['var_value']:,.2f}")
        print(f"   佔投資組合的 {var_results['95%']['var_percentage']:.2f}%")
        print(f"   CVaR (期望短缺): ${var_results['95%']['cvar_value']:,.2f}")
        
        print(f"\n📉 99% 信賴水準下的單日 VaR 為 ${var_results['99%']['var_value']:,.2f}")
        print(f"   佔投資組合的 {var_results['99%']['var_percentage']:.2f}%")
        print(f"   CVaR (期望短缺): ${var_results['99%']['cvar_value']:,.2f}")
        
        print(f"\n📊 損失機率: {prob_loss:.1f}%")
        print("="*60)
        
        var_results['probability_of_loss'] = prob_loss
        
        return var_results
    
    def plot_results(self, df, stats, simulation_results, var_results):
        print("\n📈 生成分析圖表...")
        
        fig = plt.figure(figsize=(15, 10))
        
        ax1 = plt.subplot(2, 3, 1)
        ax1.plot(df.index, df['price'], linewidth=1, color='blue')
        ax1.set_title('SOL-USD Price History')
        ax1.set_xlabel('Date')
        ax1.set_ylabel('Price (USD)')
        ax1.grid(True, alpha=0.3)
        
        ax2 = plt.subplot(2, 3, 2)
        ax2.hist(stats['returns'], bins=50, alpha=0.7, color='green', edgecolor='black')
        ax2.set_title('Daily Returns Distribution')
        ax2.set_xlabel('Daily Returns')
        ax2.set_ylabel('Frequency')
        ax2.grid(True, alpha=0.3)
        
        ax3 = plt.subplot(2, 3, 3)
        ax3.hist(simulation_results['portfolio_changes'], bins=100, 
                alpha=0.7, color='orange', edgecolor='black')
        ax3.axvline(0, color='red', linestyle='--', linewidth=2, label='Break-even')
        ax3.axvline(-var_results['95%']['var_value'], color='blue', 
                   linestyle='--', label='95% VaR')
        ax3.axvline(-var_results['99%']['var_value'], color='purple', 
                   linestyle='--', label='99% VaR')
        ax3.set_title(f'Monte Carlo Simulation ({self.num_simulations:,} runs)')
        ax3.set_xlabel('Portfolio Change (USD)')
        ax3.set_ylabel('Frequency')
        ax3.legend()
        ax3.grid(True, alpha=0.3)
        
        ax4 = plt.subplot(2, 3, 4)
        sorted_changes = np.sort(simulation_results['portfolio_changes'])
        cumulative = np.arange(1, len(sorted_changes) + 1) / len(sorted_changes)
        ax4.plot(sorted_changes, cumulative, linewidth=2, color='blue')
        ax4.axhline(0.05, color='orange', linestyle='--', alpha=0.7, label='5%')
        ax4.axhline(0.01, color='red', linestyle='--', alpha=0.7, label='1%')
        ax4.set_title('Cumulative Distribution')
        ax4.set_xlabel('Portfolio Change (USD)')
        ax4.set_ylabel('Cumulative Probability')
        ax4.legend()
        ax4.grid(True, alpha=0.3)
        
        ax5 = plt.subplot(2, 3, 5)
        from scipy import stats as scipy_stats
        scipy_stats.probplot(stats['returns'], dist="norm", plot=ax5)
        ax5.set_title('Q-Q Plot')
        ax5.grid(True, alpha=0.3)
        
        ax6 = plt.subplot(2, 3, 6)
        ax6.axis('off')
        
        summary_text = f"""
        VaR Analysis Summary
        {'='*30}
        Portfolio Value: ${self.portfolio_value:,.0f}
        Data Source: Yahoo Finance
        Period: 2 years
        Simulations: {self.num_simulations:,}
        
        95% VaR: ${var_results['95%']['var_value']:,.2f}
        99% VaR: ${var_results['99%']['var_value']:,.2f}
        
        95% CVaR: ${var_results['95%']['cvar_value']:,.2f}
        99% CVaR: ${var_results['99%']['cvar_value']:,.2f}
        
        Loss Probability: {var_results['probability_of_loss']:.1f}%
        """
        
        ax6.text(0.1, 0.5, summary_text, fontsize=10, 
                family='monospace', verticalalignment='center')
        
        plt.tight_layout()
        
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        fig_path = f'{self.save_dir}/SOL_VaR_Charts_{timestamp}.png'
        plt.savefig(fig_path, dpi=100, bbox_inches='tight')
        print(f"✅ 圖表已儲存至: {fig_path}")
        
        plt.show()
        
        return fig_path
    
    def save_results(self, stats, var_results, simulation_results):
        print(f"\n💾 儲存結果到 Google Drive...")
        
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        
        save_data = {
            'analysis_info': {
                'ticker': self.ticker,
                'data_source': 'Yahoo Finance',
                'portfolio_value': self.portfolio_value,
                'num_simulations': self.num_simulations,
                'calculation_time': timestamp,
                'save_directory': self.save_dir
            },
            'statistics': {
                'mean_daily_return': float(stats['mean_return']),
                'std_daily_return': float(stats['std_return']),
                'annual_return': float(stats['annual_mean']),
                'annual_volatility': float(stats['annual_vol']),
                'sharpe_ratio': float(stats['sharpe_ratio']),
                'max_drawdown': float(stats['max_drawdown']),
                'skewness': float(stats['skewness']),
                'kurtosis': float(stats['kurtosis']),
                'observations': int(stats['num_obs'])
            },
            'var_results': {
                '95%_VaR': float(var_results['95%']['var_value']),
                '95%_VaR_pct': float(var_results['95%']['var_percentage']),
                '95%_CVaR': float(var_results['95%']['cvar_value']),
                '99%_VaR': float(var_results['99%']['var_value']),
                '99%_VaR_pct': float(var_results['99%']['var_percentage']),
                '99%_CVaR': float(var_results['99%']['cvar_value']),
                'probability_of_loss': float(var_results['probability_of_loss'])
            },
            'simulation_summary': {
                'mean_change': float(simulation_results['mean_change']),
                'std_change': float(simulation_results['std_change']),
                'worst_loss': float(simulation_results['worst_loss']),
                'best_gain': float(simulation_results['best_gain'])
            }
        }
        
        json_path = f'{self.save_dir}/SOL_VaR_Results_{timestamp}.json'
        with open(json_path, 'w', encoding='utf-8') as f:
            json.dump(save_data, f, ensure_ascii=False, indent=2)
        
        print(f"✅ JSON 結果已儲存至: {json_path}")
        
        report = self.generate_report(stats, var_results, simulation_results)
        report_path = f'{self.save_dir}/SOL_VaR_Report_{timestamp}.txt'
        with open(report_path, 'w', encoding='utf-8') as f:
            f.write(report)
        
        print(f"✅ 文字報告已儲存至: {report_path}")
        
        return json_path, report_path
    
    def generate_report(self, stats, var_results, simulation_results):
        report = f"""
{'='*70}
                SOL 幣風險價值 (VaR) 分析報告
                   蒙地卡羅模擬法計算結果
{'='*70}

生成時間: {datetime.now().strftime('%Y年%m月%d日 %H:%M:%S')}
數據來源: Yahoo Finance (SOL-USD)

【投資組合資訊】
• 投資組合價值: ${self.portfolio_value:,.2f} USD
• 持有期間: 1 天
• 模擬次數: {self.num_simulations:,} 次

【市場數據統計】
• 觀測期間: {stats['num_obs']} 個交易日
• 日平均收益率: {stats['mean_return']*100:.3f}%
• 日波動率: {stats['std_return']*100:.3f}%
• 年化收益率: {stats['annual_mean']*100:.2f}%
• 年化波動率: {stats['annual_vol']*100:.2f}%
• 夏普比率: {stats['sharpe_ratio']:.3f}
• 最大回撤: {stats['max_drawdown']*100:.2f}%
• 偏度: {stats['skewness']:.3f}
• 峰度: {stats['kurtosis']:.3f}

【風險價值 (VaR) 結果】

>>> 95% 信賴水準 <<<
• VaR 值: ${var_results['95%']['var_value']:,.2f}
• 佔投資組合: {var_results['95%']['var_percentage']:.2f}%
• CVaR (期望短缺): ${var_results['95%']['cvar_value']:,.2f}
• 解釋: 有 95% 的信心，一天內的損失不會超過 ${var_results['95%']['var_value']:,.2f}

>>> 99% 信賴水準 <<<
• VaR 值: ${var_results['99%']['var_value']:,.2f}
• 佔投資組合: {var_results['99%']['var_percentage']:.2f}%
• CVaR (期望短缺): ${var_results['99%']['cvar_value']:,.2f}
• 解釋: 有 99% 的信心，一天內的損失不會超過 ${var_results['99%']['var_value']:,.2f}

【模擬結果摘要】
• 平均損益: ${simulation_results['mean_change']:,.2f}
• 標準差: ${simulation_results['std_change']:,.2f}
• 最壞情境損失: ${simulation_results['worst_loss']:,.2f}
• 最佳情境收益: ${simulation_results['best_gain']:,.2f}
• 發生損失的機率: {var_results['probability_of_loss']:.1f}%

【風險提醒】
• SOL 為高波動性加密貨幣資產
• VaR 基於歷史數據和常態分布假設
• 實際損失可能超過 VaR 預測值
• 建議配合其他風險管理工具使用

{'='*70}
報告結束 - 儲存於 Google Drive: /My Drive/crypto_analysis/
{'='*70}
"""
        return report
    
    def run_analysis(self):
        print("\n🔄 開始執行 SOL 幣 VaR 分析...\n")
        
        try:
            df = self.fetch_price_data()
            if df is None:
                raise ValueError("無法獲取價格數據")
            
            stats = self.calculate_statistics(df)
            
            simulation_results = self.monte_carlo_simulation(stats)
            
            var_results = self.calculate_var(simulation_results)
            
            fig_path = self.plot_results(df, stats, simulation_results, var_results)
            
            json_path, report_path = self.save_results(stats, var_results, simulation_results)
            
            report = self.generate_report(stats, var_results, simulation_results)
            print("\n" + report)
            
            print("\n✅ 分析完成！所有檔案已儲存至 Google Drive")
            print(f"📁 儲存位置: {self.save_dir}")
            
            return {
                'success': True,
                'stats': stats,
                'var_results': var_results,
                'simulation_results': simulation_results,
                'files': {
                    'json': json_path,
                    'report': report_path,
                    'chart': fig_path
                }
            }
            
        except Exception as e:
            print(f"\n❌ 分析失敗: {str(e)}")
            return {'success': False, 'error': str(e)}

if __name__ == "__main__":
    calculator = SOLVaRCalculator(
        portfolio_value=100000,  
        num_simulations=50000    
    )
    
    results = calculator.run_analysis()
    
    if results['success']:
        print("\n🎉 分析完成")
    else:
        print(f"\n分析失敗: {results.get('error', 'Unknown error')}")
