"""Linha de comando.

    python -m profit_recorder live     --config config.toml   # grava o pregão
    python -m profit_recorder backfill --config config.toml   # últimos ~30 dias
    python -m profit_recorder compact  --config config.toml   # junta partes (sem DLL)
"""

from __future__ import annotations

import argparse
import logging
import signal
import threading
from datetime import date, datetime
from pathlib import Path

from .agents import AgentRegistry
from .config import Config, credentials, load_config
from .storage import ParquetStore

log = logging.getLogger("profit_recorder")


def _store(cfg: Config) -> ParquetStore:
    return ParquetStore(cfg.data_dir, cfg.staging_dir)


def _registry(cfg: Config) -> AgentRegistry:
    return AgentRegistry(Path(cfg.data_dir) / "agents.csv")


def cmd_live(cfg: Config, args) -> None:
    from .dll import ProfitBridge
    from .pipeline import Pipeline

    store, registry = _store(cfg), _registry(cfg)
    pipe = Pipeline(store, cfg.bar_interval_s, cfg.save_raw_trades, cfg.flush_interval_s)
    bridge = ProfitBridge(cfg.dll_path, on_trade=pipe.put_trade,
                          on_history_trade=lambda t: None, on_daily=pipe.put_daily)
    stop = threading.Event()
    signal.signal(signal.SIGINT, lambda *_: stop.set())

    bridge.login(*credentials())
    consumer = threading.Thread(target=pipe.run, args=(stop,), name="consumer", daemon=True)
    consumer.start()
    for t in cfg.tickers:
        log.info("SubscribeTicker %s/%s -> %s", t, cfg.exchange, bridge.subscribe(t, cfg.exchange))

    end = datetime.combine(date.today(), cfg.session_end)
    try:
        while not stop.is_set():
            # nomes das corretoras: thread principal, fora dos callbacks
            registry.resolve(pipe.agents_seen(), bridge.agent_name)
            if not args.no_auto_stop and datetime.now() >= end:
                log.info("fim do pregão configurado (%s), encerrando", cfg.session_end)
                break
            stop.wait(30)
    finally:
        for t in cfg.tickers:
            bridge.unsubscribe(t, cfg.exchange)
        stop.set()
        consumer.join(timeout=120)
        registry.resolve(pipe.agents_seen(), bridge.agent_name)  # antes do DLLFinalize
        bridge.finalize()
        for s in store.compact_all(cfg.bar_interval_s, date=date.today().isoformat()):
            log.info("%s", s)
        if pipe.dropped:
            log.warning("%d mensagens descartadas por fila cheia", pipe.dropped)
        if bridge.callback_errors:
            log.warning("%d erros dentro de callbacks", bridge.callback_errors)


def cmd_backfill(cfg: Config, args) -> None:
    from .backfill import HistoryCollector, run_backfill
    from .dll import ProfitBridge

    collector = HistoryCollector()
    bridge = ProfitBridge(cfg.dll_path, on_trade=lambda t: None, on_history_trade=collector)
    bridge.login(*credentials())
    try:
        report = run_backfill(bridge, collector, _store(cfg), _registry(cfg),
                              tickers=args.ticker or cfg.tickers, exchange=cfg.exchange,
                              session_start=cfg.session_start, session_end=cfg.session_end,
                              interval_s=cfg.bar_interval_s, max_age_days=args.days)
    finally:
        bridge.finalize()
    for r in report:
        log.info("%s", r)


def cmd_compact(cfg: Config, args) -> None:
    for s in _store(cfg).compact_all(cfg.bar_interval_s, date=args.date):
        log.info("%s", s)


def cmd_report(cfg: Config, args) -> None:
    import pandas as pd

    from .analysis import day_report, parse_time, plot_day

    store, registry = _store(cfg), _registry(cfg)
    dates = store.dates("broker_flow")
    d = args.date or (dates[-1] if dates else None)
    if d is None:
        raise SystemExit("nenhum dado em broker_flow ainda")
    end = parse_time(args.opening_end) if args.opening_end else cfg.opening_window_end
    for ticker in args.ticker or cfg.tickers:
        rep, prices = day_report(store, registry, d, ticker, opening_end=end, top=args.top)
        with pd.option_context("display.width", 200, "display.max_columns", 20):
            print(f"\n=== {ticker} {d} | preços: {prices}")
            print(rep.to_string(index=False))
        if args.plot:
            out = Path(args.plot).with_name(f"{Path(args.plot).stem}_{ticker}_{d}.png")
            print("gráfico:", plot_day(store, registry, d, ticker, cfg.watchlist, str(out)))


def cmd_study(cfg: Config, args) -> None:
    import pandas as pd

    from .analysis import early_flow_study, parse_time

    store, registry = _store(cfg), _registry(cfg)
    end = parse_time(args.opening_end) if args.opening_end else cfg.opening_window_end
    for ticker in args.ticker or cfg.tickers:
        daily, summary = early_flow_study(store, registry, ticker, cfg.watchlist, opening_end=end)
        print(f"\n=== {ticker}: saldo de agressão até {end:%H:%M} vs retorno até o fechamento")
        if summary.empty:
            print("sem dados suficientes")
            continue
        with pd.option_context("display.width", 200, "display.max_columns", 20,
                               "display.float_format", "{:.4f}".format):
            print(summary.to_string(index=False))
        n = summary["dias"].max()
        if n < 60:
            print(f"ATENÇÃO: só {n} pregões. Abaixo de ~60 dias qualquer correlação é ruído; "
                  "não tome decisões com isso ainda.")
        if args.csv:
            daily.to_csv(Path(args.csv).with_name(f"{Path(args.csv).stem}_{ticker}.csv"), index=False)


def main(argv=None) -> None:
    ap = argparse.ArgumentParser(prog="profit_recorder")
    ap.add_argument("--config", default="config.toml")
    ap.add_argument("-v", "--verbose", action="store_true")
    sub = ap.add_subparsers(dest="cmd", required=True)
    live = sub.add_parser("live", help="grava o pregão em tempo real")
    live.add_argument("--no-auto-stop", action="store_true",
                      help="não encerrar no horário session_end")
    bf = sub.add_parser("backfill", help="baixa os últimos ~30 dias de negócios")
    bf.add_argument("--days", type=int, default=29)
    bf.add_argument("--ticker", action="append",
                    help="ativo específico (pode repetir); padrão: tickers da config")
    cp = sub.add_parser("compact", help="junta as partes do dia em day.parquet")
    cp.add_argument("--date", help="AAAA-MM-DD (padrão: todas as datas com partes)")
    rp = sub.add_parser("report", help="ranking de corretoras na abertura e no dia")
    rp.add_argument("--date", help="AAAA-MM-DD (padrão: último dia gravado)")
    rp.add_argument("--ticker", action="append")
    rp.add_argument("--top", type=int, default=15)
    rp.add_argument("--opening-end", help="HH:MM, fim da janela de abertura")
    rp.add_argument("--plot", help="caminho base do PNG (ex.: fluxo.png)")
    st = sub.add_parser("study", help="o fluxo da abertura antecipa o resto do dia?")
    st.add_argument("--ticker", action="append")
    st.add_argument("--opening-end", help="HH:MM, fim da janela de abertura")
    st.add_argument("--csv", help="salva a tabela diária (ex.: estudo.csv)")
    args = ap.parse_args(argv)

    logging.basicConfig(level=logging.DEBUG if args.verbose else logging.INFO,
                        format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    cfg = load_config(args.config) if Path(args.config).exists() else Config()
    {"live": cmd_live, "backfill": cmd_backfill, "compact": cmd_compact,
     "report": cmd_report, "study": cmd_study}[args.cmd](cfg, args)


if __name__ == "__main__":
    main()
