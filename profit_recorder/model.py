"""Tipos de dados compartilhados (independentes da DLL, testáveis em qualquer SO)."""

from __future__ import annotations

from datetime import datetime
from typing import Iterable, NamedTuple

import pandas as pd

# Valores de TTradeType (manual ProfitDLL, seção TNewTradeCallback)
AGGRESSOR_BUY = 2   # negócio originado por agressão compradora
AGGRESSOR_SELL = 3  # negócio originado por agressão vendedora
AUCTION = 4


class Trade(NamedTuple):
    ticker: str
    ts: datetime          # horário local da B3, como entregue pela DLL
    trade_number: int     # identificador único do negócio no pregão
    price: float
    qty: int
    buy_agent: int        # ID da corretora compradora
    sell_agent: int       # ID da corretora vendedora
    trade_type: int       # TTradeType
    is_edit: bool = False


TRADE_COLUMNS = ["date", *Trade._fields]

TRADE_DTYPES = {
    "date": "string",
    "ticker": "string",
    "trade_number": "int64",
    "price": "float64",
    "qty": "int64",
    "buy_agent": "int64",
    "sell_agent": "int64",
    "trade_type": "int64",
    "is_edit": "bool",
}


def trades_to_frame(trades: Iterable[Trade]) -> pd.DataFrame:
    df = pd.DataFrame(list(trades), columns=list(Trade._fields))
    df.insert(0, "date", df["ts"].map(lambda t: t.date().isoformat()) if len(df) else [])
    df["ts"] = pd.to_datetime(df["ts"])
    return df.astype(TRADE_DTYPES)
