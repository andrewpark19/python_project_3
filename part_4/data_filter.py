import pandas as pd
import numpy as np
import warnings
warnings.filterwarnings('ignore')
import quandl
from datetime import datetime
from statsmodels.tsa.stattools import coint
from config import * 

def industry_filter(df):
    # Filter only active and common asset tickers
    df = df[df['active_ticker_flag'] != 'N']
    print('Filter inactive tickers. Remaining tickers:', df.shape[0])

    df = df[df['asset_type'] == 'COM']
    print('Filter based on asset type. Remaining tickers:', df.shape[0])
    
    # Ensure sector code is available
    df = df[~df['zacks_x_sector_code'].isna()]

    # Return filtered ticker list and sector data for grouping
    ticker_list = df['ticker'].tolist()
    industry_map = df[['ticker', 'zacks_x_sector_desc']].set_index('ticker').to_dict()['zacks_x_sector_desc']
    return ticker_list, industry_map



def get_adj_close_data(ticker_list, industry_map, start, end):
    industry_data = {}
    
    for ticker in ticker_list:
        try:
            df = quandl.get_table('QUOTEMEDIA/PRICES', ticker=ticker, date={'gte': start, 'lte': end})
            df = df.sort_values(by='date').set_index('date')
            if df.empty: 
                continue 

            adj_close = df[['adj_close']]
            volume = df['volume'].mean()

            if volume < 100000: # liquidity check
                continue 
 
            if len(adj_close) != 252: # check whether there are non missing value left 
                continue
    
            industry = industry_map[ticker]

            if industry not in industry_data:
                industry_data[industry] = pd.DataFrame()
            industry_data[industry][ticker] = adj_close['adj_close']
        except Exception as e:
            print(f"Error fetching adjusted close data for {ticker}: {e}")

    # Combine individual industry data into a single dictionary
    industry_dfs = {industry: df for industry, df in industry_data.items()}
    return industry_dfs


def download_data(ticker_list,start,end): 

    dates = pd.date_range(start, end).strftime("%Y-%m-%d")
    df = quandl.get_table('QUOTEMEDIA/PRICES',
                                date=','.join(dates.tolist()),
                                ticker=','.join(ticker_list))
    df = df.set_index(["date", "ticker"])[
        ["adj_close", "volume"]].unstack()
    return df


def cointegration_test(stock1_prices, stock2_prices, significance_level=0.05):

    coint_t, p_value, critical_values = coint(stock1_prices, stock2_prices)
    
    # Determine if the stocks are cointegrated based on the p-value
    if p_value < significance_level:
        conclusion = "The stocks are cointegrated"
    else:
        conclusion = "The stocks are not cointegrated"

    result = {
        "Test Statistic": coint_t,
        "P-Value": p_value,
        "Critical Values": critical_values,
        "Conclusion": conclusion
    }
    
    return result


def download_ff_data(start_date: str | datetime = START_DATE,
                     end_date: str | datetime = END_DATE):
    """
    Download Fama-French data and save to pickle file for future rerun

    Parameters
    ----------
    start_date: get price data from start_date to end_date (both inclusive)
    end_date: get price data from start_date to end_date (both inclusive)

    Returns
    -------
    pd.DataFrame of required price data for analysis
    """

    ff_link = ("https://mba.tuck.dartmouth.edu/pages/faculty/ken.french/"
               "ftp/F-F_Research_Data_Factors_daily_CSV.zip")
    ff = pd.read_csv(ff_link, compression='zip', header=None, sep='\t',
                     lineterminator='\r')
    ff = (ff.iloc[4:, 0].str.strip("\n").str.split(",", expand=True)
          .dropna(how="any"))
    df = pd.DataFrame(
        ff.iloc[1:, 1:].values,
        index=pd.to_datetime(ff.iloc[1:, 0], format="%Y%m%d"),
        columns=ff.iloc[0, 1:].to_list()
    )
    return df.loc[start_date:end_date].apply(pd.to_numeric)
