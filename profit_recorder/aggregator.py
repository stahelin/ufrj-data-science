"""Agregação de negócios em barras de preço e em fluxo por corretora.

Duas implementações que devem produzir o mesmo resultado:

* ``LiveAggregator``: incremental, usada durante o pregão (barra a barra).
* ``aggregate_frame``: vetorizada (pandas), usada para reconstruir o dia a
  partir dos negócios brutos deduplicados na compactação e no backfill.

Todas as colunas numéricas de ``broker_flow`` são aditivas, então linhas
repetidas para a mesma (barra, corretora) — por exemplo, um negócio que
chegou atrasado — podem ser somadas na leitura sem perda de exatidão.

Atribuição por corretora em cada negócio de ``qty`` contratos:
  * corretora compradora: ``buy_qty += qty``; se o negócio foi agressão
    compradora (TTradeType=2), ela foi a agressora: ``buy_aggr_qty += qty``.
  * corretora vendedora: ``sell_qty += qty``; se foi agressão vendedora
    (TTradeType=3): ``sell_aggr_qty += qty``.
Negócios marcados como edição (TC_IS_EDIT) não entram nas agregações; ficam
apenas no arquivo bruto.
"""

from __future__ import annotations

from datetime import datetime, timedelta

import pandas as pd

from .model import AGGRESSOR_BUY, AGGRESSOR_SELL, Trade

BAR_COLUMNS = [
    "date", "ticker", "bar_start",
    "open", "high", "low", "close",
    "qty", "notional", "n_trades", "aggr_buy_qty", "aggr_sell_qty",
]

FLOW_COLUMNS = [
    "date", "ticker", "bar_start", "agent_id",
    "buy_qty", "sell_qty", "net_qty",
    "buy_aggr_qty", "sell_aggr_qty", "net_aggr_qty",
    "buy_notional", "sell_notional",
    "n_buy", "n_sell",
]

_FLOW_SUM_COLUMNS = [
    "buy_qty", "sell_qty", "buy_aggr_qty", "sell_aggr_qty",
    "buy_notional", "sell_notional", "n_buy", "n_sell",
]

_BAR_INT = ["qty", "n_trades", "aggr_buy_qty", "aggr_sell_qty"]
_FLOW_INT = ["agent_id", "buy_qty", "sell_qty", "net_qty", "buy_aggr_qty",
             "sell_aggr_qty", "net_aggr_qty", "n_buy", "n_sell"]


def floor_ts(ts: datetime, interval_s: int) -> datetime:
    secs = ts.hour * 3600 + ts.minute * 60 + ts.second
    secs -= secs % interval_s
    return ts.replace(hour=secs // 3600, minute=secs % 3600 // 60,
                      second=secs % 60, microsecond=0)


def _finish_bars(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame(columns=BAR_COLUMNS)
    df = df[BAR_COLUMNS].copy()
    df["bar_start"] = pd.to_datetime(df["bar_start"])
    df[_BAR_INT] = df[_BAR_INT].astype("int64")
    return df.sort_values(["ticker", "bar_start"]).reset_index(drop=True)


def _finish_flow(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame(columns=FLOW_COLUMNS)
    df = df.copy()
    df["net_qty"] = df["buy_qty"] - df["sell_qty"]
    df["net_aggr_qty"] = df["buy_aggr_qty"] - df["sell_aggr_qty"]
    df = df[FLOW_COLUMNS]
    df["bar_start"] = pd.to_datetime(df["bar_start"])
    df[_FLOW_INT] = df[_FLOW_INT].astype("int64")
    return df.sort_values(["ticker", "bar_start", "agent_id"]).reset_index(drop=True)


class LiveAggregator:
    """Acumula negócios em memória e libera barras já fechadas."""

    def __init__(self, interval_s: int = 60):
        self.interval_s = interval_s
        self._bars: dict[tuple[str, datetime], list] = {}
        self._flow: dict[tuple[str, datetime, int], list] = {}
        self._latest: dict[str, datetime] = {}
        self.skipped_edits = 0

    def add(self, t: Trade) -> None:
        if t.is_edit:
            self.skipped_edits += 1
            return
        b = floor_ts(t.ts, self.interval_s)
        notional = t.price * t.qty
        aggr_buy = t.qty if t.trade_type == AGGRESSOR_BUY else 0
        aggr_sell = t.qty if t.trade_type == AGGRESSOR_SELL else 0

        bar = self._bars.get((t.ticker, b))
        if bar is None:
            # open, high, low, close, qty, notional, n, aggr_buy, aggr_sell
            self._bars[(t.ticker, b)] = [t.price, t.price, t.price, t.price,
                                         t.qty, notional, 1, aggr_buy, aggr_sell]
        else:
            bar[1] = max(bar[1], t.price)
            bar[2] = min(bar[2], t.price)
            bar[3] = t.price
            bar[4] += t.qty
            bar[5] += notional
            bar[6] += 1
            bar[7] += aggr_buy
            bar[8] += aggr_sell

        # ordem igual a _FLOW_SUM_COLUMNS
        buy = self._flow.setdefault((t.ticker, b, t.buy_agent), [0, 0, 0, 0, 0.0, 0.0, 0, 0])
        buy[0] += t.qty
        buy[2] += aggr_buy
        buy[4] += notional
        buy[6] += 1
        sell = self._flow.setdefault((t.ticker, b, t.sell_agent), [0, 0, 0, 0, 0.0, 0.0, 0, 0])
        sell[1] += t.qty
        sell[3] += aggr_sell
        sell[5] += notional
        sell[7] += 1

        prev = self._latest.get(t.ticker)
        if prev is None or b > prev:
            self._latest[t.ticker] = b

    def _pop(self, keep) -> tuple[pd.DataFrame, pd.DataFrame]:
        bar_rows, flow_rows = [], []
        for key in [k for k in self._bars if not keep(k[0], k[1])]:
            ticker, b = key
            o, h, lo, c, q, n_, cnt, ab, as_ = self._bars.pop(key)
            bar_rows.append([b.date().isoformat(), ticker, b, o, h, lo, c, q, n_, cnt, ab, as_])
        for key in [k for k in self._flow if not keep(k[0], k[1])]:
            ticker, b, agent = key
            flow_rows.append([b.date().isoformat(), ticker, b, agent, *self._flow.pop(key)])
        bars = pd.DataFrame(bar_rows, columns=BAR_COLUMNS)
        flow = pd.DataFrame(flow_rows, columns=["date", "ticker", "bar_start", "agent_id", *_FLOW_SUM_COLUMNS])
        return _finish_bars(bars), _finish_flow(flow)

    def pop_closed(self, grace_bars: int = 1) -> tuple[pd.DataFrame, pd.DataFrame]:
        """Libera barras anteriores à barra mais recente menos ``grace_bars``."""
        grace = timedelta(seconds=self.interval_s * grace_bars)

        def keep(ticker, b):
            latest = self._latest.get(ticker)
            return latest is not None and b >= latest - grace

        return self._pop(keep)

    def pop_all(self) -> tuple[pd.DataFrame, pd.DataFrame]:
        return self._pop(lambda ticker, b: False)


def aggregate_frame(trades: pd.DataFrame, interval_s: int = 60) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Versão vetorizada; ``trades`` no formato de ``trades_to_frame``."""
    df = trades[~trades["is_edit"].astype(bool)]
    if df.empty:
        return _finish_bars(pd.DataFrame()), _finish_flow(pd.DataFrame())
    df = df.sort_values(["ts", "trade_number"], kind="stable").copy()
    df["bar_start"] = df["ts"].dt.floor(f"{interval_s}s")
    df["notional"] = df["price"] * df["qty"]
    df["aggr_buy_qty"] = df["qty"].where(df["trade_type"] == AGGRESSOR_BUY, 0)
    df["aggr_sell_qty"] = df["qty"].where(df["trade_type"] == AGGRESSOR_SELL, 0)

    keys = ["ticker", "bar_start"]
    g = df.groupby(keys, sort=True)
    bars = g.agg(open=("price", "first"), high=("price", "max"), low=("price", "min"),
                 close=("price", "last"), qty=("qty", "sum"), notional=("notional", "sum"),
                 n_trades=("price", "size"), aggr_buy_qty=("aggr_buy_qty", "sum"),
                 aggr_sell_qty=("aggr_sell_qty", "sum")).reset_index()
    bars["date"] = bars["bar_start"].dt.date.map(lambda d: d.isoformat())

    zeros = pd.Series(0, index=df.index)
    buy = pd.DataFrame({
        "ticker": df["ticker"], "bar_start": df["bar_start"], "agent_id": df["buy_agent"],
        "buy_qty": df["qty"], "sell_qty": zeros, "buy_aggr_qty": df["aggr_buy_qty"],
        "sell_aggr_qty": zeros, "buy_notional": df["notional"], "sell_notional": 0.0,
        "n_buy": 1, "n_sell": zeros,
    })
    sell = pd.DataFrame({
        "ticker": df["ticker"], "bar_start": df["bar_start"], "agent_id": df["sell_agent"],
        "buy_qty": zeros, "sell_qty": df["qty"], "buy_aggr_qty": zeros,
        "sell_aggr_qty": df["aggr_sell_qty"], "buy_notional": 0.0, "sell_notional": df["notional"],
        "n_buy": zeros, "n_sell": 1,
    })
    flow = (pd.concat([buy, sell], ignore_index=True)
            .groupby(["ticker", "bar_start", "agent_id"], sort=True)[_FLOW_SUM_COLUMNS]
            .sum().reset_index())
    flow["date"] = flow["bar_start"].dt.date.map(lambda d: d.isoformat())
    return _finish_bars(bars), _finish_flow(flow)


def combine_flow(flow: pd.DataFrame) -> pd.DataFrame:
    """Soma linhas repetidas de (ticker, barra, corretora)."""
    if flow.empty:
        return _finish_flow(flow)
    out = (flow.groupby(["date", "ticker", "bar_start", "agent_id"], sort=True)[_FLOW_SUM_COLUMNS]
           .sum().reset_index())
    return _finish_flow(out)


def combine_bars(bars: pd.DataFrame) -> pd.DataFrame:
    """Combina pedaços da mesma barra (usado só quando não há negócios brutos)."""
    if bars.empty:
        return _finish_bars(bars)
    out = (bars.groupby(["date", "ticker", "bar_start"], sort=True)
           .agg(open=("open", "first"), high=("high", "max"), low=("low", "min"),
                close=("close", "last"), qty=("qty", "sum"), notional=("notional", "sum"),
                n_trades=("n_trades", "sum"), aggr_buy_qty=("aggr_buy_qty", "sum"),
                aggr_sell_qty=("aggr_sell_qty", "sum"))
           .reset_index())
    return _finish_bars(out)
