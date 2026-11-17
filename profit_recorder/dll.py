"""Ponte mínima com a ProfitDLL (somente Windows, Python 64 bits).

Regras do manual respeitadas aqui:
  * todos os callbacks rodam na ``ConnectorThread`` da DLL, em fila única: o
    callback só traduz o dado (``TranslateTrade``, função acessória) e o
    repassa; nada de disco, banco ou print por negócio;
  * funções de requisição (subscribe, histórico, nomes de agentes) nunca são
    chamadas de dentro de um callback;
  * referências aos callbacks ficam guardadas no objeto para não serem
    coletadas pelo GC enquanto a DLL as usa.

As estruturas abaixo seguem o manual (v4.0.0.43) e o exemplo Python oficial.
"""

from __future__ import annotations

import ctypes
import logging
import sys
import threading
from ctypes import (POINTER, Structure, byref, c_double, c_int, c_int32, c_longlong,
                    c_size_t, c_ubyte, c_uint, c_ushort, c_void_p, c_wchar_p, cast,
                    create_unicode_buffer)
from datetime import datetime
from typing import Callable

from .model import Trade

log = logging.getLogger(__name__)

NL_OK = 0
TC_IS_EDIT = 1
TC_LAST_PACKET = 2

# nConnStateType / resultados relevantes de TStateCallback
CONN_LOGIN, CONN_BROKER, CONN_MARKET, CONN_ACTIVATION = 0, 1, 2, 3
MARKET_CONNECTED_STATES = {4, 5, 6}


class SystemTime(Structure):
    _fields_ = [("wYear", c_ushort), ("wMonth", c_ushort), ("wDayOfWeek", c_ushort),
                ("wDay", c_ushort), ("wHour", c_ushort), ("wMinute", c_ushort),
                ("wSecond", c_ushort), ("wMilliseconds", c_ushort)]


class TConnectorTrade(Structure):
    _fields_ = [("Version", c_ubyte), ("TradeDate", SystemTime), ("TradeNumber", c_uint),
                ("Price", c_double), ("Quantity", c_longlong), ("Volume", c_double),
                ("BuyAgent", c_int), ("SellAgent", c_int), ("TradeType", c_ubyte)]


class TConnectorAssetIdentifierSafe(Structure):
    # Ticker/Exchange como c_void_p evitam conversões automáticas de string
    # pelo ctypes dentro do callback (mesma abordagem do exemplo oficial).
    _fields_ = [("Version", c_ubyte), ("Ticker", c_void_p), ("Exchange", c_void_p),
                ("FeedType", c_ubyte)]


class TAssetID(Structure):
    _fields_ = [("ticker", c_wchar_p), ("bolsa", c_wchar_p), ("feed", c_int)]


def _st_to_dt(st: SystemTime) -> datetime:
    return datetime(st.wYear, st.wMonth, st.wDay, st.wHour, st.wMinute, st.wSecond,
                    st.wMilliseconds * 1000)


class ProfitBridge:
    """Encapsula login, assinatura de negócios, histórico e nomes de agentes."""

    def __init__(self, dll_path: str,
                 on_trade: Callable[[Trade], None],
                 on_history_trade: Callable[[Trade], None],
                 on_daily: Callable[[dict], None] | None = None):
        if sys.platform != "win32":
            raise RuntimeError("A ProfitDLL só roda no Windows (Python 64 bits).")
        self._on_trade = on_trade
        self._on_history_trade = on_history_trade
        self._on_daily = on_daily
        self.market_connected = threading.Event()
        self.activated = threading.Event()
        self.history_done = threading.Event()
        self.history_progress = 0
        self.callback_errors = 0
        self._dll = ctypes.WinDLL(dll_path)
        self._build_callbacks()
        self._declare_functions()

    # ----------------------------------------------------------- callbacks
    def _build_callbacks(self) -> None:
        W = ctypes.WINFUNCTYPE
        self.TradeCallback = W(None, TConnectorAssetIdentifierSafe, c_size_t, c_uint)

        dll = self  # captura para os closures

        @W(None, c_int32, c_int32)
        def state_cb(conn_type, result):
            if conn_type == CONN_MARKET:
                if result in MARKET_CONNECTED_STATES:
                    dll.market_connected.set()
                else:
                    dll.market_connected.clear()
            elif conn_type == CONN_ACTIVATION:
                if result == 0:
                    dll.activated.set()
                else:
                    dll.activated.clear()
            elif conn_type == CONN_LOGIN and result != 0:
                log.warning("Login retornou %s", result)

        def _translate(asset, p_trade, flags):
            trade = TConnectorTrade(Version=0)
            if dll._dll.TranslateTrade(p_trade, byref(trade)) != NL_OK:
                return None
            ticker = cast(asset.Ticker, c_wchar_p).value if asset.Ticker else ""
            return Trade(ticker=ticker, ts=_st_to_dt(trade.TradeDate),
                         trade_number=int(trade.TradeNumber), price=float(trade.Price),
                         qty=int(trade.Quantity), buy_agent=int(trade.BuyAgent),
                         sell_agent=int(trade.SellAgent), trade_type=int(trade.TradeType),
                         is_edit=bool(flags & TC_IS_EDIT))

        @self.TradeCallback
        def trade_cb(asset, p_trade, flags):
            try:
                t = _translate(asset, p_trade, flags)
                if t is not None:
                    dll._on_trade(t)
            except Exception:  # noqa: BLE001 - exceção nunca deve voltar para a DLL
                dll.callback_errors += 1

        @self.TradeCallback
        def history_cb(asset, p_trade, flags):
            try:
                t = _translate(asset, p_trade, 0)
                if t is not None:
                    dll._on_history_trade(t)
            except Exception:  # noqa: BLE001
                dll.callback_errors += 1
            finally:
                if flags & TC_LAST_PACKET:
                    dll.history_done.set()

        @W(None, TAssetID, c_wchar_p, *([c_double] * 10), *([c_int] * 7))
        def daily_cb(asset, date, o, h, lo, c, vol, ajuste, max_lim, min_lim, vol_buy, vol_sell,
                     qtd, negocios, contratos_abertos, qtd_buy, qtd_sell, neg_buy, neg_sell):
            if dll._on_daily is None:
                return
            try:
                dll._on_daily({
                    "ticker": asset.ticker, "session": date, "open": o, "high": h, "low": lo,
                    "close": c, "volume": vol, "ajuste": ajuste, "max_limit": max_lim,
                    "min_limit": min_lim, "volume_buyer": vol_buy, "volume_seller": vol_sell,
                    "qty": qtd, "n_trades": negocios, "open_interest": contratos_abertos,
                    "qty_buyer": qtd_buy, "qty_seller": qtd_sell,
                    "n_trades_buyer": neg_buy, "n_trades_seller": neg_sell,
                    "received_at": datetime.now(),
                })
            except Exception:  # noqa: BLE001
                dll.callback_errors += 1

        @W(None, TAssetID, c_int)
        def progress_cb(asset, progress):
            dll.history_progress = int(progress)

        @W(None, TAssetID, c_double, c_int, c_int)
        def tinybook_cb(asset, price, qty, side):
            return

        self._callbacks = dict(state=state_cb, trade=trade_cb, history=history_cb,
                               daily=daily_cb, progress=progress_cb, tinybook=tinybook_cb)

    def _declare_functions(self) -> None:
        d = self._dll
        d.TranslateTrade.argtypes = [c_size_t, POINTER(TConnectorTrade)]
        d.TranslateTrade.restype = c_int
        d.SetTradeCallbackV2.argtypes = [self.TradeCallback]
        d.SetTradeCallbackV2.restype = c_int
        d.SetHistoryTradeCallbackV2.argtypes = [self.TradeCallback]
        d.SetHistoryTradeCallbackV2.restype = c_int
        d.SubscribeTicker.argtypes = [c_wchar_p, c_wchar_p]
        d.SubscribeTicker.restype = c_int
        d.UnsubscribeTicker.argtypes = [c_wchar_p, c_wchar_p]
        d.UnsubscribeTicker.restype = c_int
        d.GetHistoryTrades.argtypes = [c_wchar_p, c_wchar_p, c_wchar_p, c_wchar_p]
        d.GetHistoryTrades.restype = c_int
        d.GetAgentNameLength.argtypes = [c_int, c_int]
        d.GetAgentNameLength.restype = c_int
        d.GetAgentName.argtypes = [c_int, c_int, c_wchar_p, c_int]
        d.GetAgentName.restype = c_int

    # -------------------------------------------------------------- public
    def login(self, key: str, user: str, password: str, timeout_s: float = 120) -> None:
        cb = self._callbacks
        # Mesma ordem de parâmetros do exemplo oficial (somente Market Data).
        result = self._dll.DLLInitializeMarketLogin(
            c_wchar_p(key), c_wchar_p(user), c_wchar_p(password),
            cb["state"], None, cb["daily"], None, None, None, cb["progress"], cb["tinybook"])
        if result != NL_OK:
            raise RuntimeError(f"DLLInitializeMarketLogin retornou {result}")
        self._dll.SetTradeCallbackV2(cb["trade"])
        self._dll.SetHistoryTradeCallbackV2(cb["history"])
        if not self.market_connected.wait(timeout_s):
            raise TimeoutError("Market Data não conectou dentro do tempo limite")
        log.info("Market Data conectado (ativação: %s)", self.activated.is_set())

    def subscribe(self, ticker: str, exchange: str) -> int:
        return self._dll.SubscribeTicker(ticker, exchange)

    def unsubscribe(self, ticker: str, exchange: str) -> int:
        return self._dll.UnsubscribeTicker(ticker, exchange)

    def request_history(self, ticker: str, exchange: str, start: datetime, end: datetime) -> int:
        self.history_done.clear()
        self.history_progress = 0
        fmt = "%d/%m/%Y %H:%M:%S"
        return self._dll.GetHistoryTrades(ticker, exchange, start.strftime(fmt), end.strftime(fmt))

    def agent_name(self, agent_id: int) -> tuple[str, str]:
        def _get(short: int) -> str:
            n = self._dll.GetAgentNameLength(int(agent_id), short)
            if n <= 0:
                return ""
            buf = create_unicode_buffer(n + 1)
            self._dll.GetAgentName(n + 1, int(agent_id), buf, short)
            return buf.value
        return _get(0), _get(1)

    def finalize(self) -> None:
        try:
            self._dll.DLLFinalize()
        except Exception:  # noqa: BLE001
            log.exception("DLLFinalize falhou")
