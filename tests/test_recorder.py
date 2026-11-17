import random
import threading
from datetime import date, datetime, time, timedelta

import pandas as pd
import pytest

from profit_recorder.agents import AgentRegistry
from profit_recorder.aggregator import LiveAggregator, aggregate_frame, combine_flow, floor_ts
from profit_recorder.analysis import day_report, early_flow_study
from profit_recorder.backfill import HistoryCollector, plan_dates, run_backfill
from profit_recorder.config import load_config
from profit_recorder.model import AGGRESSOR_BUY, AGGRESSOR_SELL, Trade, trades_to_frame
from profit_recorder.pipeline import Pipeline
from profit_recorder.storage import ParquetStore

MORGAN, UBS, XP = 40, 8, 3


def make_trades(day=date(2026, 11, 16), n=3000, seed=1, ticker="WINFUT"):
    rng = random.Random(seed)
    t0 = datetime.combine(day, time(9, 0))
    price, out = 130000.0, []
    for i in range(n):
        price += rng.choice([-5, 0, 5])
        out.append(Trade(ticker, t0 + timedelta(milliseconds=i * 3000 + rng.randint(0, 999)), i + 1,
                         price, rng.randint(1, 20), rng.choice([MORGAN, UBS, XP, 72, 120]),
                         rng.choice([MORGAN, UBS, XP, 72, 120]),
                         rng.choice([AGGRESSOR_BUY, AGGRESSOR_SELL, 1]), False))
    return out


def test_floor_ts():
    assert floor_ts(datetime(2026, 1, 2, 9, 7, 59, 999), 60) == datetime(2026, 1, 2, 9, 7)
    assert floor_ts(datetime(2026, 1, 2, 9, 7, 59), 300) == datetime(2026, 1, 2, 9, 5)


def test_aggression_attribution():
    ts = datetime(2026, 11, 16, 9, 0, 1)
    agg = LiveAggregator(60)
    agg.add(Trade("WINFUT", ts, 1, 100.0, 10, MORGAN, XP, AGGRESSOR_BUY))   # Morgan agrediu comprando
    agg.add(Trade("WINFUT", ts, 2, 100.0, 4, XP, MORGAN, AGGRESSOR_BUY))    # XP agrediu comprando
    agg.add(Trade("WINFUT", ts, 3, 99.0, 3, UBS, MORGAN, AGGRESSOR_SELL))   # Morgan agrediu vendendo
    agg.add(Trade("WINFUT", ts, 4, 99.0, 50, UBS, MORGAN, AGGRESSOR_SELL, is_edit=True))
    bars, flow = agg.pop_all()
    m = flow.set_index("agent_id").loc[MORGAN]
    assert (m.buy_qty, m.sell_qty, m.net_qty) == (10, 7, 3)
    assert (m.buy_aggr_qty, m.sell_aggr_qty, m.net_aggr_qty) == (10, 3, 7)
    u = flow.set_index("agent_id").loc[UBS]
    assert (u.buy_qty, u.buy_aggr_qty, u.net_aggr_qty) == (3, 0, 0)  # comprou passivamente
    assert agg.skipped_edits == 1
    assert bars.iloc[0][["open", "high", "low", "close", "qty"]].tolist() == [100.0, 100.0, 99.0, 99.0, 17]
    # soma de agressão compradora = soma das agressões líquidas positivas do lado comprador
    assert flow["buy_aggr_qty"].sum() == bars["aggr_buy_qty"].sum() == 14


def test_live_equals_vectorized():
    trades = make_trades()
    agg = LiveAggregator(60)
    live_bars, live_flow = [], []
    for i, t in enumerate(trades):
        agg.add(t)
        if i % 500 == 0:
            b, f = agg.pop_closed()
            live_bars.append(b)
            live_flow.append(f)
    b, f = agg.pop_all()
    live_bars = pd.concat(live_bars + [b], ignore_index=True)
    live_flow = combine_flow(pd.concat(live_flow + [f], ignore_index=True))
    vb, vf = aggregate_frame(trades_to_frame(trades), 60)
    pd.testing.assert_frame_equal(live_bars.reset_index(drop=True), vb, check_dtype=False)
    pd.testing.assert_frame_equal(live_flow, vf, check_dtype=False)


def test_pop_closed_keeps_recent_bars():
    agg = LiveAggregator(60)
    base = datetime(2026, 11, 16, 9, 0)
    for m in range(5):
        agg.add(Trade("WINFUT", base + timedelta(minutes=m, seconds=5), m, 1.0, 1, 1, 2, 2))
    bars, _ = agg.pop_closed(grace_bars=1)
    assert bars["bar_start"].dt.minute.tolist() == [0, 1, 2]
    bars, _ = agg.pop_all()
    assert bars["bar_start"].dt.minute.tolist() == [3, 4]


def test_store_compact_dedupes_and_rebuilds(tmp_path):
    store = ParquetStore(tmp_path / "data", tmp_path / "staging")
    trades = make_trades(n=600)
    df = trades_to_frame(trades)
    store.write("trades", df.iloc[:400])
    store.write("trades", df.iloc[300:])  # 100 negócios duplicados (ex.: reinício)
    stats = store.compact("2026-11-16", "WINFUT")
    assert stats["trades"] == 600 and stats["duplicates_removed"] == 100
    assert [f.name for f in store.files("trades", "2026-11-16", "WINFUT")] == ["day.parquet"]
    vb, vf = aggregate_frame(df)
    pd.testing.assert_frame_equal(store.read("broker_flow", "2026-11-16", "WINFUT"), vf, check_dtype=False)
    pd.testing.assert_frame_equal(store.read("bars", "2026-11-16", "WINFUT"), vb, check_dtype=False)
    assert not list((tmp_path / "staging").iterdir())


def test_pipeline_flush_and_compact(tmp_path):
    store = ParquetStore(tmp_path / "data", tmp_path / "staging")
    pipe = Pipeline(store, 60, save_raw=True, flush_interval_s=0.05)
    stop = threading.Event()
    th = threading.Thread(target=pipe.run, args=(stop,))
    th.start()
    trades = make_trades(n=1000)
    for t in trades:
        pipe.put_trade(t)
    pipe.put_daily({"ticker": "WINFUT", "ajuste": 130100.0, "open_interest": 123,
                    "received_at": datetime(2026, 11, 16, 18, 0)})
    stop.set()
    th.join(10)
    assert pipe.trades_seen == 1000 and pipe.dropped == 0
    assert {MORGAN, UBS, XP} <= pipe.agents_seen()
    store.compact_all(60)
    _, vf = aggregate_frame(trades_to_frame(trades))
    pd.testing.assert_frame_equal(store.read("broker_flow", "2026-11-16", "WINFUT"), vf, check_dtype=False)
    assert store.read("daily", "2026-11-16", "WINFUT")["ajuste"].iloc[0] == 130100.0


def test_pipeline_without_raw_still_exact(tmp_path):
    store = ParquetStore(tmp_path / "data", tmp_path / "staging")
    pipe = Pipeline(store, 60, save_raw=False)
    trades = make_trades(n=800)
    for i, t in enumerate(trades):
        pipe.process(("t", t))
        if i % 200 == 0:
            pipe.flush()
    pipe.flush(final=True)
    store.compact_all(60)
    _, vf = aggregate_frame(trades_to_frame(trades))
    pd.testing.assert_frame_equal(store.read("broker_flow", "2026-11-16", "WINFUT"), vf, check_dtype=False)


def test_plan_dates_respects_limits():
    today = date(2026, 11, 17)  # terça
    days = plan_dates(today, 40)
    assert days[0] == today - timedelta(days=29) or days[0] > today - timedelta(days=29)
    assert days[-1] == date(2026, 11, 16)
    assert all(d.weekday() < 5 for d in days)
    assert today not in days
    skip = date(2026, 11, 13)
    assert skip not in plan_dates(today, exists=lambda d: d == skip)


class FakeBridge:
    def __init__(self, collector, data):
        self.collector, self.data = collector, data
        self.history_done = threading.Event()
        self.requests = []

    def request_history(self, ticker, exchange, start, end):
        self.requests.append((ticker, start, end))
        assert end - start < timedelta(days=10)
        self.history_done.clear()
        trades = self.data.get(start.date(), [])
        def deliver():
            for t in trades:
                self.collector(t._replace(ticker="WINZ26"))
            self.history_done.set()
        threading.Thread(target=deliver).start()
        return 0

    def agent_name(self, agent_id):
        return {MORGAN: ("MORGAN STANLEY CTVM", "Morgan"), UBS: ("UBS BRASIL CCTVM", "UBS")}.get(
            agent_id, (f"Corretora {agent_id}", ""))


def test_backfill_with_fake_dll(tmp_path):
    store = ParquetStore(tmp_path / "data", tmp_path / "staging")
    reg = AgentRegistry(tmp_path / "data" / "agents.csv")
    collector = HistoryCollector()
    d1, d2 = date(2026, 11, 13), date(2026, 11, 16)
    bridge = FakeBridge(collector, {d1: make_trades(d1, 300, 2), d2: make_trades(d2, 300, 3)})
    report = run_backfill(bridge, collector, store, reg, ["WINFUT"], "F", time(8, 55), time(18, 35),
                          max_age_days=5, timeout_s=5, today=date(2026, 11, 17))
    ok = [r for r in report if r.get("status") == "ok"]
    assert {r["date"] for r in ok} == {d1.isoformat(), d2.isoformat()}
    assert store.has_day("broker_flow", d2.isoformat(), "WINFUT")
    assert reg.label(MORGAN) == "Morgan"
    assert AgentRegistry(tmp_path / "data" / "agents.csv").match(["morgan"])["morgan"] == [MORGAN]
    # segunda execução não baixa de novo os dias já gravados
    bridge.requests.clear()
    run_backfill(bridge, collector, store, reg, ["WINFUT"], "F", time(8, 55), time(18, 35),
                 max_age_days=5, timeout_s=1, today=date(2026, 11, 17))
    assert {r[1].date() for r in bridge.requests}.isdisjoint({d1, d2})


def test_study_detects_planted_signal(tmp_path):
    """Dias sintéticos onde a agressão do 'Morgan' na abertura antecipa o resto do dia."""
    store = ParquetStore(tmp_path / "data", tmp_path / "staging")
    reg = AgentRegistry(tmp_path / "agents.csv")
    reg.resolve([MORGAN, UBS, XP], FakeBridge(None, {}).agent_name)
    rng = random.Random(7)
    day = date(2026, 6, 1)
    for _ in range(40):
        while day.weekday() >= 5:
            day += timedelta(days=1)
        direction = rng.choice([-1, 1])
        t0, price, trades, n = datetime.combine(day, time(9, 0)), 130000.0, [], 0
        for minute in range(0, 120):
            for _k in range(5):
                n += 1
                ts = t0 + timedelta(minutes=minute, seconds=_k * 10)
                if minute < 30:  # abertura: Morgan agride no sentido do dia, UBS aleatório
                    buyer, seller = (MORGAN, XP) if direction > 0 else (XP, MORGAN)
                    tt = AGGRESSOR_BUY if direction > 0 else AGGRESSOR_SELL
                    if _k % 2:
                        buyer, seller = (UBS, XP) if rng.random() < .5 else (XP, UBS)
                        tt = AGGRESSOR_BUY if buyer == UBS else AGGRESSOR_SELL
                    price += rng.choice([-5, 5])
                else:
                    buyer, seller, tt = XP, 72, rng.choice([2, 3])
                    price += direction * 5 + rng.choice([-5, 0, 5])
                trades.append(Trade("WINFUT", ts, n, price, 5, buyer, seller, tt))
        store.write("trades", trades_to_frame(trades))
        store.compact(day.isoformat(), "WINFUT")
        day += timedelta(days=1)

    daily, summary = early_flow_study(store, reg, "WINFUT", ["Morgan", "UBS"], opening_end=time(9, 30))
    s = summary.set_index("grupo")
    assert s.loc["Morgan", "dias"] == 40
    assert s.loc["Morgan", "acerto_direcao"] == 1.0
    assert s.loc["Morgan", "corr_spearman"] > 0.8
    assert abs(s.loc["UBS", "corr_spearman"]) < s.loc["Morgan", "corr_spearman"]

    rep, prices = day_report(store, reg, daily["date"].iloc[0], "WINFUT")
    assert rep.iloc[0]["corretora"] in {"Morgan", "Corretora 3"}
    assert set(prices) == {"abertura", "em_0930", "fechamento", "pct_vol_sem_corretora"}
    assert prices["pct_vol_sem_corretora"] == 0.0


def test_load_config(tmp_path):
    p = tmp_path / "c.toml"
    p.write_text('[market]\ntickers=["WDOFUT"]\nsession_end="18:00"\n'
                 '[storage]\ndata_dir="x"\nbar_interval_s=30\n[brokers]\nwatchlist=["UBS"]\n',
                 encoding="utf-8")
    cfg = load_config(p)
    assert cfg.tickers == ["WDOFUT"] and cfg.session_end == time(18, 0)
    assert cfg.bar_interval_s == 30 and cfg.watchlist == ["UBS"]
    p.write_text("[storage]\nbar_interval_s=7\n", encoding="utf-8")
    with pytest.raises(ValueError):
        load_config(p)


def test_cli_compact_report_study(tmp_path, capsys):
    from profit_recorder.__main__ import main

    data = tmp_path / "data"
    cfg = tmp_path / "config.toml"
    cfg.write_text(f'[market]\ntickers=["WINFUT"]\n[storage]\ndata_dir="{data.as_posix()}"\n'
                   f'staging_dir="{(tmp_path / "stg").as_posix()}"\n'
                   '[brokers]\nwatchlist=["Morgan","UBS"]\nopening_window_end="09:30"\n',
                   encoding="utf-8")
    store = ParquetStore(data, tmp_path / "stg")
    reg = AgentRegistry(data / "agents.csv")
    reg.resolve([MORGAN, UBS, XP, 72, 120], FakeBridge(None, {}).agent_name)
    for i, d in enumerate([date(2026, 11, 12), date(2026, 11, 13), date(2026, 11, 16)]):
        store.write("trades", trades_to_frame(make_trades(d, 2000, i)))

    main(["--config", str(cfg), "compact"])
    assert store.has_day("broker_flow", "2026-11-16", "WINFUT")
    main(["--config", str(cfg), "report", "--plot", str(tmp_path / "fluxo.png")])
    main(["--config", str(cfg), "study", "--csv", str(tmp_path / "estudo.csv")])
    out = capsys.readouterr().out
    assert "Morgan" in out and "ATENÇÃO" in out
    assert (tmp_path / "fluxo_WINFUT_2026-11-16.png").stat().st_size > 10_000
    assert len(pd.read_csv(tmp_path / "estudo_WINFUT.csv")) == 6
