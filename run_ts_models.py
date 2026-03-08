import pandas as pd
import numpy as np
import os
import time
import json
import warnings
import matplotlib.pyplot as plt
from statsmodels.tsa.arima.model import ARIMA
from statsmodels.tsa.vector_ar.var_model import VAR
from statsmodels.tsa.regime_switching.markov_autoregression import MarkovAutoregression
from arch import arch_model

warnings.filterwarnings('ignore')

def calculate_metrics(y_true, y_pred):
    # filter NaNs
    mask = ~np.isnan(y_true) & ~np.isnan(y_pred)
    if not np.any(mask):
        return np.nan, np.nan
    mse = np.mean((y_true[mask] - y_pred[mask])**2)
    rmse = np.sqrt(mse)
    mae = np.mean(np.abs(y_true[mask] - y_pred[mask]))
    return rmse, mae

def fit_models(data_path, output_metrics_path, output_plot_path):
    print(f"Loading data from {data_path}...")
    df = pd.read_csv(data_path)
    df = df.sort_values('time_bucket')
    
    # Using total_bytes_per_sec
    # Scale down by 1e6 to make it MB/s so optimization algorithms don't fail due to massive variance
    ts_data = df['total_bytes_per_sec'].values / 10**6
    ts_data_var = df[['tx_bytes_per_sec', 'rx_bytes_per_sec']].values / 10**6
    
    # Split train/test (80/20)
    split_idx = int(len(ts_data) * 0.8)
    train_uni, test_uni = ts_data[:split_idx], ts_data[split_idx:]
    train_var, test_var = ts_data_var[:split_idx], ts_data_var[split_idx:]
    
    print(f"Total time steps: {len(ts_data)}")
    print(f"Train size: {len(train_uni)}, Test size: {len(test_uni)}")
    
    results = []
    plot_data = {'Actual': test_uni}
    
    def record_result(name, aic, bic, llf, rmse, mae, ftime, preds=None):
        results.append({
            'Model': name, 'AIC': aic, 'BIC': bic, 'Log-Likelihood': llf,
            'RMSE (MB/s)': rmse, 'MAE (MB/s)': mae, 'Fit Time (s)': ftime
        })
        if preds is not None:
            plot_data[name] = preds

    # 1. ARMA(1,1)
    print("\nFitting ARMA(1,0,1)...")
    t0 = time.time()
    try:
        mod = ARIMA(train_uni, order=(1,0,1)).fit()
        preds = mod.forecast(steps=len(test_uni))
        rmse, mae = calculate_metrics(test_uni, preds)
        record_result('ARMA(1,1)', mod.aic, mod.bic, mod.llf, rmse, mae, time.time()-t0, preds)
    except Exception as e: print(f"ARMA failed: {e}")

    # 2. ARIMA(1,1,1)
    print("\nFitting ARIMA(1,1,1)...")
    t0 = time.time()
    try:
        mod = ARIMA(train_uni, order=(1,1,1)).fit()
        preds = mod.forecast(steps=len(test_uni))
        rmse, mae = calculate_metrics(test_uni, preds)
        record_result('ARIMA(1,1,1)', mod.aic, mod.bic, mod.llf, rmse, mae, time.time()-t0, preds)
    except Exception as e: print(f"ARIMA failed: {e}")

    # 3. SARIMA
    print("\nFitting SARIMA(1,1,1)x(1,0,0,10)...")
    t0 = time.time()
    try:
        mod = ARIMA(train_uni, order=(1,1,1), seasonal_order=(1,0,0,10)).fit()
        preds = mod.forecast(steps=len(test_uni))
        rmse, mae = calculate_metrics(test_uni, preds)
        record_result('SARIMA', mod.aic, mod.bic, mod.llf, rmse, mae, time.time()-t0, preds)
    except Exception as e: print(f"SARIMA failed: {e}")

    # 4. ARCH(1)
    print("\nFitting ARCH(1)...")
    t0 = time.time()
    try:
        mod = arch_model(train_uni, p=1, q=0, rescale=True).fit(disp='off')
        record_result('ARCH(1)', mod.aic, mod.bic, mod.loglikelihood, np.nan, np.nan, time.time()-t0)
    except Exception as e: print(f"ARCH failed: {e}")

    # 5. GARCH(1,1)
    print("\nFitting GARCH(1,1)...")
    t0 = time.time()
    try:
        mod = arch_model(train_uni, p=1, q=1, rescale=True).fit(disp='off')
        record_result('GARCH(1,1)', mod.aic, mod.bic, mod.loglikelihood, np.nan, np.nan, time.time()-t0)
    except Exception as e: print(f"GARCH failed: {e}")

    # 6. VAR
    print("\nFitting VAR...")
    t0 = time.time()
    try:
        mod = VAR(train_var).fit(maxlags=2, ic='aic')
        preds_var = mod.forecast(train_var[-mod.k_ar:], steps=len(test_var))
        preds_total = preds_var[:, 0] + preds_var[:, 1]
        rmse, mae = calculate_metrics(test_uni, preds_total)
        record_result('VAR', mod.aic, mod.bic, mod.llf, rmse, mae, time.time()-t0, preds_total)
    except Exception as e: print(f"VAR failed: {e}")

    # 7. TAR (Markov Regime Switching)
    print("\nFitting TAR (Markov Regime Switching AR)...")
    t0 = time.time()
    try:
        mod = MarkovAutoregression(train_uni, k_regimes=2, order=1, switching_ar=True).fit(disp=False)
        record_result('TAR', mod.aic, mod.bic, mod.llf, np.nan, np.nan, time.time()-t0)
    except Exception as e: print(f"TAR failed: {e}")

    # Save
    results_df = pd.DataFrame(results)
    print("\n=== Model Comparison Metrics ===")
    print(results_df.to_markdown(index=False))
    results_df.to_csv(output_metrics_path, index=False)
    print(f"\nSaved metrics to {output_metrics_path}")

    # Plot
    print("\nGenerating plots...")
    os.makedirs(os.path.dirname(output_plot_path), exist_ok=True)
    plt.figure(figsize=(14, 7))
    time_axis = range(len(test_uni))
    
    plt.plot(time_axis, plot_data['Actual'], label='Actual (Test Set)', color='black', linewidth=2)
    
    cmap = plt.get_cmap('tab10')
    color_idx = 0
    for mod_name, preds in plot_data.items():
        if mod_name != 'Actual':
            plt.plot(time_axis, preds, label=f'{mod_name} Forecast', color=cmap(color_idx), linestyle='--')
            color_idx += 1
            
    plt.title("Time Series Models: Forecast vs Actual Test Set (Bandwidth MB/s)")
    plt.xlabel("Time Steps (Seconds in Test Window)")
    plt.ylabel("Bandwidth (MB / Sec)")
    plt.legend()
    plt.grid(True, alpha=0.3)
    plt.tight_layout()
    plt.savefig(output_plot_path, dpi=300)
    print(f"Saved plot to {output_plot_path}")

if __name__ == "__main__":
    data_path = r"data\aggregated_ts_bandwidth.csv"
    output_metrics = r"ts_results_metrics.csv"
    output_plot = r"plots\ts_comparative_plots.png"
    fit_models(data_path, output_metrics, output_plot)
