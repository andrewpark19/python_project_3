import numpy as np
import pandas as pd
from sklearn.linear_model import LinearRegression
from scipy.optimize import brute
import json

from trading_strategy import calc_spread, exec_trade


def downside_beta(ret: pd.Series, mkt_ret: pd.Series):

    idx = ret.index.intersection(mkt_ret.loc[mkt_ret < 0].index)
    return ret.loc[idx].corr(mkt_ret.loc[idx])


def sharpe_ratio(ret: pd.Series, rf_ret: pd.Series = None,
                 ann_factor: int = 252):

    if rf_ret is not None:
        ret = (ret - rf_ret).loc[ret.index.intersection(rf_ret.index)]
    return ret.mean() / ret.std() * np.sqrt(ann_factor)


def sortino_ratio(ret: pd.Series, rf_ret: pd.Series = None,
                  ann_factor: int = 252):

    if rf_ret is not None:
        ret = (ret - rf_ret).loc[ret.index.intersection(rf_ret.index)]
    return ret.mean() / ret.loc[ret < 0].std() * np.sqrt(ann_factor)


def eval_return(ret: pd.Series,
                df_ff: pd.DataFrame,
                ann_calendar_days: float = 365.25,
                ann_trading_days: float = 252):

    daily_ret = ret.diff().dropna()
    T = (ret.index.max() - ret.index.min()).days * ann_calendar_days

    df_ff = df_ff / ann_trading_days  # Use daily data for FF & our returns
    mkt_ret = df_ff["Mkt-RF"] + df_ff["RF"]
    ols = LinearRegression().fit(df_ff.loc[daily_ret.index], daily_ret)

    return {
        "return": ret.iloc[-1] / T,

        # We assume risk-free rate = 0 since our return are too small to
        # compare with RF in Fama-French time-series
        "sharpe": sharpe_ratio(daily_ret, ann_factor=ann_trading_days),
        "sortino": sortino_ratio(daily_ret, ann_factor=ann_trading_days),

        # Fama-French Regression
        "alpha": ols.intercept_ * ann_trading_days,
        "beta": ols.coef_,

        # Correlations
        "downside_beta": downside_beta(daily_ret, mkt_ret),
    }


def translate_j_g(m: int, j: float, g: float, df_price: pd.DataFrame
                  ):
    spread = calc_spread(df_price, m)
    return spread.abs().quantile(j), spread.abs().quantile(g)

def calculate_annual_summary(df, rf_rate_df):
    rf_rate_df = rf_rate_df.reindex(df.index).fillna(method='ffill')  # Forward fill any missing rf values
    df['Year'] = df.index.year  # Extract years from the index

    summary = []

    for year in df['Year'].unique():
        yearly_data = df[df['Year'] == year]
        rf_yearly = rf_rate_df[rf_rate_df.index.year == year]
        
        annual_return = (1 + yearly_data['daily_return']).prod() - 1
        annual_volatility = yearly_data['daily_return'].std() * np.sqrt(252)
        
        avg_rf_rate = rf_yearly.mean().iloc[0] 
        excess_return = annual_return - avg_rf_rate
        sharpe_ratio = excess_return / annual_volatility if annual_volatility != 0 else np.nan
        
        cumulative_return = (1 + yearly_data['daily_return']).cumprod()
        max_drawdown = (cumulative_return / cumulative_return.cummax() - 1).min()

        summary.append({
            'Year': year,
            '% Return': f"{annual_return * 100:.3f}%",
            '% Volatility': f"{annual_volatility * 100:.3f}%",
            'Sharpe Ratio': f"{sharpe_ratio:.2f}",
            'Max Drawdown (%)': f"{max_drawdown * 100:.3f}%"
        })

    total_return = (1 + df['daily_return']).prod() - 1
    #print(total_return)
    total_volatility = df['daily_return'].std() * np.sqrt(252)
    #avg_rf_total = rf_rate_df.mean().iloc[0]  
    #print(avg_rf_total)
    total_sharpe_ratio = (total_return) / total_volatility if total_volatility != 0 else np.nan
    total_max_drawdown = (cumulative_return / cumulative_return.cummax() - 1).min()
    
    summary.append({
        'Year': 'Total',
        '% Return': f"{total_return * 100:.3f}%",
        '% Volatility': f"{total_volatility * 100:.3f}%",
        'Sharpe Ratio': f"{total_sharpe_ratio:.3f}",
        'Max Drawdown (%)': f"{total_max_drawdown * 100:.3f}%"
    })

    summary_df = pd.DataFrame(summary)
    summary_df.set_index('Year', inplace=True)
    return summary_df
