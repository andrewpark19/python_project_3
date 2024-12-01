from functools import partial

import numpy as np
import pandas as pd

# from part_4.config.config import STOCK_HIGH, STOCK_LOW, START_TRADING_DATE
from config import *

def calc_spread(df_price: pd.DataFrame, m: int,
                use_log: bool = False) -> pd.Series:
    
    if use_log:
        ret = np.log(df_price / df_price.shift(m))
    else:
        ret = df_price.pct_change(m)

    return (ret[STOCK_HIGH] - ret[STOCK_LOW]).loc[START_TRADING_DATE:].dropna()


def calc_position_by_spread(
        df_price: pd.DataFrame,
        spread: pd.Series,
        j: float, g: float,
        dollar_size: pd.Series,
) -> pd.DataFrame:

    position = pd.DataFrame(index=spread.index,
                            columns=[STOCK_HIGH, STOCK_LOW])

    spread_select = partial(
        np.select,
        condlist=[
            spread > g,
            (spread < j) & (spread > -j),
            spread < -g,
        ],
        default=np.nan
    )

    matched_ds = dollar_size.loc[spread.index]
    high_shares = matched_ds // df_price.loc[spread.index, STOCK_HIGH]
    low_shares = matched_ds // df_price.loc[spread.index, STOCK_LOW]

    position.loc[:, STOCK_HIGH] = spread_select(
        choicelist=[-high_shares, 0, high_shares])
    position.loc[:, STOCK_LOW] = spread_select(
        choicelist=[low_shares, 0, -low_shares])

    # close all position in the end
    position.loc[position.index.max()] = 0

    return position


class CalcTrade:

    _gross_traded_cash = 0
    _net_traded_cash = 0

    def __init__(self, position: pd.DataFrame, df_price: pd.DataFrame,
                 cost: float = 0, s: float = 1, init_cap: float = 0):

        self._position = position
        self._df_price = df_price.loc[position.index]
        self._s = s
        self._cost = cost
        self._init_cap = init_cap

        self._next_valid_date = position.index.min()
        self.__zero_position = pd.Series([0, 0], index=position.columns)
        self._open_position = self.__zero_position.copy()

    def iter_dates(self):

        trade = {}
        for date, pos in self._position.iterrows():
            price = self._df_price.loc[date]
            open_pnl = self._open_pnl(price)
            pos.rename(None, inplace=True)

            # 1. Stop loss happened in the same calendar month
            if date < self._next_valid_date:
                trade[date] = self._keep_position()

            # 2. Has open position + Stop loss event occur -> close to 0
            elif open_pnl < -self._s:
                trade[date] = self._make_trade(price, self.__zero_position)

                # after stop loss, no new position until next calendar month
                self._next_valid_date = date + pd.offsets.MonthBegin()

            # 3. Keep current position if no ideal position for the date (e.g. spread within band)
            elif pos.isnull().all():
                trade[date] = self._keep_position()

            # 4. No open position + ideal position != 0 -> enter new position
            elif (self._open_position == self.__zero_position).all():
                trade[date] = self._make_trade(price, pos)
                open_pnl = 0    # on adjust position PnL = 0

            # 5. Has open position + ideal position = 0 / flip -> adjust position
            elif ((pos == self.__zero_position).all() or
                  ((pos > 0) != (self._open_position > 0)).all()):
                trade[date] = self._make_trade(price, pos)
                open_pnl = 0    # on adjust position PnL = 0

            else:
                trade[date] = self._keep_position()

            trade[date]["open_pnl"] = open_pnl

        return self._final_df(trade)

    def _final_df(self, trade: dict[pd.Series | float]) -> pd.DataFrame:

        raw_df = pd.DataFrame(trade).T

        # Combine input df + output actual position / trade data
        position_df = pd.DataFrame(raw_df["position"].to_dict()).T
        trade_df = pd.DataFrame(raw_df["trade"].to_dict()).T
        df = pd.concat([
            self._df_price, self._position,
            position_df, position_df * self._df_price,
            trade_df, -trade_df * self._df_price,
            raw_df.drop(columns=["position", "trade"])
        ], keys=["price", "raw_position", "position", "position_dollar",
                 "trade", "trade_dollar", "summary"], axis=1)

        # Calculate balance and returns
        df.loc[:, ("balance", "cash")] = (df["trade_dollar"].sum(axis=1) -
                                          df[("summary", "trading_cost")]
                                          ).cumsum() + self._init_cap
        df.loc[:, ("balance", "position")] = df["position_dollar"].sum(axis=1)
        df.loc[:, ("balance", "total")] = df["balance"].sum(axis=1)
        df.loc[:, ("balance", "total_ret")] = \
            df[("balance", "total")] / self._init_cap - 1

        return df.apply(pd.to_numeric)

    def _open_pnl(self, price: pd.Series) -> float:

        if self._gross_traded_cash > 0:
            return ((self._open_position * price).sum() -
                    self._net_traded_cash) / self._gross_traded_cash
        else:
            return np.nan

    def _make_trade(self, price: pd.Series, pos: pd.Series
                    ) -> dict[pd.Series | float]:
        
        self._net_traded_cash = (pos * price).sum()
        self._gross_traded_cash = (pos * price).abs().sum()
        trade = (pos - self._open_position)

        details = {
            "position": pos,
            "trade": trade,
            "net_traded_cash": self._net_traded_cash,
            "gross_traded_cash": self._gross_traded_cash,
            "trading_cost": (trade * price).abs().sum() * self._cost
        }
        self._open_position = pos
        return details

    def _keep_position(self) -> dict[pd.Series | float]:

        return {
            "position": self._open_position,
            "trade": self.__zero_position,
            "net_traded_cash": self._net_traded_cash,
            "gross_traded_cash": self._gross_traded_cash,
            "trading_cost": 0
        }


def exec_trade(
        K: float,
        df_price: pd.DataFrame,
        m: int,
        j: float,
        g: float,
        s: float,
        dollar_size: pd.Series,
        cost: float = 0,
        return_only: bool = True,
) -> pd.Series | pd.DataFrame:

    spread = calc_spread(df_price, m)
    position = calc_position_by_spread(df_price, spread,
                                       j=spread.abs().quantile(j),
                                       g=spread.abs().quantile(g),
                                       dollar_size=dollar_size)
    trade = CalcTrade(df_price=df_price, position=position,
                      s=s, cost=cost, init_cap=K).iter_dates()

    if return_only:
        return trade[("balance", "total_ret")]
    else:
        return trade
