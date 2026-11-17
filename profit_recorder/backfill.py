"""Recuperação do histórico de negócios disponível na DLL.

Limites do manual (GetHistoryTrades):
  * a data inicial não pode ser anterior a 30 dias (NL_HISTORY_PERIOD_LIMIT);
  * para WIN/WDO o intervalo de cada requisição deve ser menor que 10 dias.
Por isso pedimos um pregão por requisição, dos últimos ``max_age_days`` dias.
"""

from __future__ import annotations

import logging
import threading
import time
from datetime import date, datetime, time as dtime, timedelta
from typing import Callable

from .agents import AgentRegistry
from .model import Trade, trades_to_frame
from .storage import ParquetStore

log = logging.getLogger(__name__)

MAX_AGE_DAYS = 29  # margem de 1 dia em relação ao limite de 30


def plan_dates(today: date, max_age_days: int = MAX_AGE_DAYS,
               exists: Callable[[date], bool] = lambda d: False) -> list[date]:
    """Dias úteis (seg–sex) de ``today - max_age_days`` até ontem, sem os já gravados.

    Feriados não são conhecidos aqui: a requisição simplesmente volta vazia.
    """
    max_age_days = min(max_age_days, MAX_AGE_DAYS)
    days = [today - timedelta(days=i) for i in range(max_age_days, 0, -1)]
    return [d for d in days if d.weekday() < 5 and not exists(d)]


class HistoryCollector:
    """Recebe os negócios do callback de histórico (thread da DLL)."""

    def __init__(self):
        self._lock = threading.Lock()
        self._trades: list[Trade] = []
        self.last_received = 0.0

    def __call__(self, t: Trade) -> None:
        with self._lock:
            self._trades.append(t)
        self.last_received = time.monotonic()

    def take(self) -> list[Trade]:
        with self._lock:
            out, self._trades = self._trades, []
        return out


def run_backfill(bridge, collector: HistoryCollector, store: ParquetStore,
                 registry: AgentRegistry, tickers: list[str], exchange: str,
                 session_start: dtime, session_end: dtime, interval_s: int = 60,
                 max_age_days: int = MAX_AGE_DAYS, timeout_s: float = 300,
                 today: date | None = None) -> list[dict]:
    today = today or date.today()
    report = []
    for ticker in tickers:
        days = plan_dates(today, max_age_days,
                          exists=lambda d: store.has_day("trades", d.isoformat(), ticker))
        log.info("%s: %d pregões para baixar", ticker, len(days))
        for d in days:
            collector.take()
            start, end = datetime.combine(d, session_start), datetime.combine(d, session_end)
            ret = bridge.request_history(ticker, exchange, start, end)
            if ret != 0:
                log.warning("%s %s: GetHistoryTrades retornou %s", ticker, d, ret)
                report.append({"ticker": ticker, "date": d.isoformat(), "status": f"erro {ret}"})
                continue
            finished = bridge.history_done.wait(timeout_s)
            time.sleep(0.5)  # deixa o último pacote ser entregue
            trades = collector.take()
            if not trades:
                status = "vazio (feriado?)" if finished else "timeout sem dados"
                log.info("%s %s: %s", ticker, d, status)
                report.append({"ticker": ticker, "date": d.isoformat(), "status": status})
                continue
            df = trades_to_frame(trades)
            df["ticker"] = ticker  # mesmo nome usado na gravação ao vivo
            df = df[df["date"] == d.isoformat()]
            store.write("trades", df)
            stats = store.compact(d.isoformat(), ticker, interval_s)
            registry.resolve(set(df["buy_agent"]) | set(df["sell_agent"]), bridge.agent_name)
            stats["status"] = "ok" if finished else "ok (sem TC_LAST_PACKET, pode estar incompleto)"
            report.append(stats)
    return report
