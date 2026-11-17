"""Armazenamento em Parquet particionado por tipo/data/ativo.

Layout::

    <data_dir>/
      trades/date=2026-11-17/ticker=WINFUT/part-101500-ab12cd.parquet   (durante o dia)
      trades/date=2026-11-17/ticker=WINFUT/day.parquet                  (após compactar)
      bars/...            barras OHLC + agressão total
      broker_flow/...     fluxo por corretora por barra
      daily/...           último snapshot diário (ajuste, contratos em aberto...)
      agents.csv          ID -> nome da corretora

Google Drive: aponte ``data_dir`` para uma pasta do Google Drive para
Computador (ex.: ``G:/Meu Drive/profit_data``). Cada arquivo é escrito por
completo numa pasta local de *staging* e só depois movido para o destino, de
modo que o Drive nunca sincroniza um arquivo pela metade.
"""

from __future__ import annotations

import logging
import os
import shutil
import tempfile
import uuid
from datetime import datetime
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from .aggregator import aggregate_frame, combine_bars, combine_flow

log = logging.getLogger(__name__)

KINDS = ("trades", "bars", "broker_flow", "daily")
DAY_FILE = "day.parquet"


class ParquetStore:
    def __init__(self, data_dir: str | os.PathLike, staging_dir: str | os.PathLike | None = None):
        self.data_dir = Path(data_dir)
        self.data_dir.mkdir(parents=True, exist_ok=True)
        if staging_dir is None:
            staging_dir = Path(tempfile.gettempdir()) / "profit_recorder_staging"
        self.staging_dir = Path(staging_dir)
        self.staging_dir.mkdir(parents=True, exist_ok=True)

    # ---------------------------------------------------------------- paths
    def partition(self, kind: str, date: str, ticker: str) -> Path:
        if kind not in KINDS:
            raise ValueError(f"tipo desconhecido: {kind}")
        return self.data_dir / kind / f"date={date}" / f"ticker={ticker}"

    def files(self, kind: str, date: str, ticker: str) -> list[Path]:
        p = self.partition(kind, date, ticker)
        return sorted(p.glob("*.parquet")) if p.exists() else []

    def has_day(self, kind: str, date: str, ticker: str) -> bool:
        return (self.partition(kind, date, ticker) / DAY_FILE).exists()

    def dates(self, kind: str) -> list[str]:
        root = self.data_dir / kind
        if not root.exists():
            return []
        return sorted(p.name.split("=", 1)[1] for p in root.glob("date=*") if p.is_dir())

    def tickers(self, kind: str, date: str) -> list[str]:
        root = self.data_dir / kind / f"date={date}"
        if not root.exists():
            return []
        return sorted(p.name.split("=", 1)[1] for p in root.glob("ticker=*") if p.is_dir())

    # ---------------------------------------------------------------- write
    def _stage(self, df: pd.DataFrame) -> Path:
        tmp = self.staging_dir / f"{uuid.uuid4().hex}.parquet"
        table = pa.Table.from_pandas(df.reset_index(drop=True), preserve_index=False)
        pq.write_table(table, tmp, compression="zstd")
        return tmp

    @staticmethod
    def _publish(tmp: Path, final: Path) -> None:
        final.parent.mkdir(parents=True, exist_ok=True)
        try:
            os.replace(tmp, final)          # mesmo volume: atômico
        except OSError:
            shutil.move(str(tmp), str(final))  # volumes diferentes (ex.: drive G:)

    def write(self, kind: str, df: pd.DataFrame) -> list[Path]:
        """Grava um novo arquivo ``part-*`` por (data, ativo) presente em ``df``."""
        written = []
        if df is None or df.empty:
            return written
        stamp = datetime.now().strftime("%H%M%S")
        for (date, ticker), g in df.groupby(["date", "ticker"], sort=True):
            final = self.partition(kind, date, ticker) / f"part-{stamp}-{uuid.uuid4().hex[:6]}.parquet"
            self._publish(self._stage(g), final)
            written.append(final)
        return written

    def replace_day(self, kind: str, date: str, ticker: str, df: pd.DataFrame) -> Path:
        """Substitui toda a partição por um único ``day.parquet``."""
        tmp = self._stage(df)
        for f in self.files(kind, date, ticker):
            f.unlink()
        final = self.partition(kind, date, ticker) / DAY_FILE
        self._publish(tmp, final)
        return final

    # ----------------------------------------------------------------- read
    def read(self, kind: str, date: str, ticker: str | None = None) -> pd.DataFrame:
        tickers = [ticker] if ticker else self.tickers(kind, date)
        frames = [pd.read_parquet(f) for t in tickers for f in self.files(kind, date, t)]
        frames = [f for f in frames if not f.empty]
        if not frames:
            return pd.DataFrame()
        return pd.concat(frames, ignore_index=True)

    # ------------------------------------------------------------- compact
    def compact(self, date: str, ticker: str, interval_s: int = 60) -> dict:
        """Junta os ``part-*`` do dia em ``day.parquet``.

        Com negócios brutos disponíveis, deduplica-os e recalcula barras e
        fluxo por corretora a partir deles (resultado exato, independente de
        reinícios ou negócios atrasados). Sem eles, apenas soma as partes.
        """
        stats = {"date": date, "ticker": ticker}
        trades = self.read("trades", date, ticker)
        if not trades.empty:
            before = len(trades)
            trades = (trades.drop_duplicates()
                      .sort_values(["ts", "trade_number"], kind="stable")
                      .reset_index(drop=True))
            stats["trades"] = len(trades)
            stats["duplicates_removed"] = before - len(trades)
            self.replace_day("trades", date, ticker, trades)
            bars, flow = aggregate_frame(trades, interval_s)
        else:
            bars = combine_bars(self.read("bars", date, ticker))
            flow = combine_flow(self.read("broker_flow", date, ticker))
        if not bars.empty:
            self.replace_day("bars", date, ticker, bars)
        if not flow.empty:
            self.replace_day("broker_flow", date, ticker, flow)
        stats["bars"] = len(bars)
        stats["flow_rows"] = len(flow)
        log.info("compactado %s", stats)
        return stats

    def compact_all(self, interval_s: int = 60, date: str | None = None) -> list[dict]:
        out = []
        dates = [date] if date else sorted(set(self.dates("trades")) | set(self.dates("broker_flow")))
        for d in dates:
            tickers = sorted(set(self.tickers("trades", d)) | set(self.tickers("broker_flow", d)))
            for t in tickers:
                parts = [f for k in ("trades", "bars", "broker_flow") for f in self.files(k, d, t)
                         if f.name != DAY_FILE]
                if parts:
                    out.append(self.compact(d, t, interval_s))
        return out
