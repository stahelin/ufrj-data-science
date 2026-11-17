"""Fila entre os callbacks da DLL e a gravação em disco.

Os callbacks chamam ``put_trade``/``put_daily`` (apenas ``put_nowait`` numa
``queue.Queue``). Uma thread consumidora agrega e, a cada
``flush_interval_s``, grava barras fechadas, fluxo por corretora e negócios
brutos no ``ParquetStore``.
"""

from __future__ import annotations

import logging
import queue
import threading
import time

import pandas as pd

from .aggregator import LiveAggregator
from .model import Trade, trades_to_frame
from .storage import ParquetStore

log = logging.getLogger(__name__)


class Pipeline:
    def __init__(self, store: ParquetStore, interval_s: int = 60, save_raw: bool = True,
                 flush_interval_s: float = 600, max_queue: int = 5_000_000):
        self.store = store
        self.interval_s = interval_s
        self.save_raw = save_raw
        self.flush_interval_s = flush_interval_s
        self.queue: queue.Queue = queue.Queue(maxsize=max_queue)
        self.agg = LiveAggregator(interval_s)
        self._raw: list[Trade] = []
        self._daily: dict[str, dict] = {}
        self._agents: set[int] = set()
        self._agents_lock = threading.Lock()
        self.dropped = 0
        self.trades_seen = 0
        self.tickers_seen: set[str] = set()

    # ---------------------------------------------- chamados pelos callbacks
    def put_trade(self, t: Trade) -> None:
        try:
            self.queue.put_nowait(("t", t))
        except queue.Full:
            self.dropped += 1

    def put_daily(self, d: dict) -> None:
        try:
            self.queue.put_nowait(("d", d))
        except queue.Full:
            self.dropped += 1

    # ------------------------------------------------------------ consumidor
    def process(self, item) -> None:
        kind, payload = item
        if kind == "t":
            self.trades_seen += 1
            if payload.ticker not in self.tickers_seen:
                self.tickers_seen.add(payload.ticker)
                log.info("primeiro negócio recebido para %s", payload.ticker)
            self.agg.add(payload)
            if self.save_raw:
                self._raw.append(payload)
            with self._agents_lock:
                self._agents.add(payload.buy_agent)
                self._agents.add(payload.sell_agent)
        elif kind == "d":
            self._daily[payload["ticker"]] = payload

    def drain(self) -> int:
        n = 0
        while True:
            try:
                item = self.queue.get_nowait()
            except queue.Empty:
                return n
            self.process(item)
            n += 1

    def flush(self, final: bool = False) -> None:
        bars, flow = self.agg.pop_all() if final else self.agg.pop_closed()
        if self._raw:
            raw, self._raw = self._raw, []
            self.store.write("trades", trades_to_frame(raw))
        self.store.write("bars", bars)
        self.store.write("broker_flow", flow)
        for ticker, d in list(self._daily.items()):
            date = d["received_at"].date().isoformat()
            df = pd.DataFrame([{**d, "date": date}])
            self.store.replace_day("daily", date, ticker, df)
        log.info("flush: %d negócios recebidos, %d descartados (fila cheia), %d edições ignoradas, "
                 "%d na fila", self.trades_seen, self.dropped, self.agg.skipped_edits,
                 self.queue.qsize())

    def agents_seen(self) -> set[int]:
        with self._agents_lock:
            return set(self._agents)

    def run(self, stop: threading.Event) -> None:
        next_flush = time.monotonic() + self.flush_interval_s
        while not stop.is_set():
            try:
                self.process(self.queue.get(timeout=0.5))
            except queue.Empty:
                pass
            if time.monotonic() >= next_flush:
                self.drain()
                self.flush()
                next_flush = time.monotonic() + self.flush_interval_s
        self.drain()
        self.flush(final=True)
