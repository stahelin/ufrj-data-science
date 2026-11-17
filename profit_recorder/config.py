"""Leitura da configuração (TOML). Credenciais nunca ficam no arquivo."""

from __future__ import annotations

import getpass
import os
import tomllib
from dataclasses import dataclass, field
from datetime import time
from pathlib import Path


@dataclass
class Config:
    dll_path: str = "ProfitDLL.dll"
    tickers: list[str] = field(default_factory=lambda: ["WINFUT", "WDOFUT"])
    exchange: str = "F"
    session_start: time = time(8, 55)
    session_end: time = time(18, 35)
    data_dir: str = "profit_data"
    staging_dir: str | None = None
    save_raw_trades: bool = True
    flush_interval_s: int = 600
    bar_interval_s: int = 60
    watchlist: list[str] = field(default_factory=list)
    opening_window_end: time = time(9, 30)


def _t(s: str) -> time:
    h, m = s.split(":")
    return time(int(h), int(m))


def load_config(path: str | os.PathLike) -> Config:
    raw = tomllib.loads(Path(path).read_text(encoding="utf-8"))
    p, m, s, b = (raw.get(k, {}) for k in ("profit", "market", "storage", "brokers"))
    cfg = Config()
    cfg.dll_path = p.get("dll_path", cfg.dll_path)
    cfg.tickers = list(m.get("tickers", cfg.tickers))
    cfg.exchange = m.get("exchange", cfg.exchange)
    if "session_start" in m:
        cfg.session_start = _t(m["session_start"])
    if "session_end" in m:
        cfg.session_end = _t(m["session_end"])
    cfg.data_dir = s.get("data_dir", cfg.data_dir)
    cfg.staging_dir = s.get("staging_dir", cfg.staging_dir)
    cfg.save_raw_trades = bool(s.get("save_raw_trades", cfg.save_raw_trades))
    cfg.flush_interval_s = int(s.get("flush_interval_s", cfg.flush_interval_s))
    cfg.bar_interval_s = int(s.get("bar_interval_s", cfg.bar_interval_s))
    if 86400 % cfg.bar_interval_s:
        raise ValueError("bar_interval_s precisa dividir o dia (ex.: 15, 30, 60, 300)")
    cfg.watchlist = list(b.get("watchlist", cfg.watchlist))
    if "opening_window_end" in b:
        cfg.opening_window_end = _t(b["opening_window_end"])
    return cfg


def credentials() -> tuple[str, str, str]:
    """Lê PROFIT_KEY / PROFIT_USER / PROFIT_PASSWORD ou pergunta no terminal."""
    key = os.environ.get("PROFIT_KEY") or input("Chave de acesso: ")
    user = os.environ.get("PROFIT_USER") or input("Usuário: ")
    password = os.environ.get("PROFIT_PASSWORD") or getpass.getpass("Senha: ")
    return key, user, password
