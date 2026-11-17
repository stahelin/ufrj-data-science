"""Análises sobre o fluxo por corretora gravado.

* ``day_report``: quem mais comprou/vendeu (e agrediu) na abertura e no dia.
* ``early_flow_study``: hipótese "seguir os grandes" — o saldo de agressão
  de uma corretora na janela de abertura antecipa o retorno do resto do dia?

O saldo de agressão (``net_aggr_qty``) é o que cada corretora tomou de
liquidez comprando menos o que tomou vendendo: mede iniciativa, não só
participação. ``net_qty`` inclui também as ordens passivas que foram executadas.
"""

from __future__ import annotations

from datetime import datetime, time

import numpy as np
import pandas as pd

from .agents import AgentRegistry
from .storage import ParquetStore

_SUMS = ["buy_qty", "sell_qty", "net_qty", "buy_aggr_qty", "sell_aggr_qty", "net_aggr_qty"]


def _in_window(df: pd.DataFrame, start: time | None, end: time | None) -> pd.DataFrame:
    t = df["bar_start"].dt.time
    mask = pd.Series(True, index=df.index)
    if start is not None:
        mask &= t >= start
    if end is not None:
        mask &= t < end
    return df[mask]


def window_summary(flow: pd.DataFrame, start: time | None = None, end: time | None = None) -> pd.DataFrame:
    w = _in_window(flow, start, end)
    if w.empty:
        return pd.DataFrame(columns=["agent_id", *_SUMS])
    out = w.groupby("agent_id")[_SUMS].sum().reset_index()
    return out.sort_values("net_aggr_qty", key=np.abs, ascending=False).reset_index(drop=True)


def day_report(store: ParquetStore, registry: AgentRegistry, date: str, ticker: str,
               opening_start: time = time(9, 0), opening_end: time = time(9, 30),
               top: int = 15) -> tuple[pd.DataFrame, dict]:
    flow = store.read("broker_flow", date, ticker)
    bars = store.read("bars", date, ticker).sort_values("bar_start")
    if flow.empty:
        raise FileNotFoundError(f"sem broker_flow para {ticker} em {date}")
    opening = window_summary(flow, opening_start, opening_end)
    day = window_summary(flow)
    rep = (opening[["agent_id", "net_aggr_qty", "net_qty"]]
           .rename(columns={"net_aggr_qty": "aggr_abertura", "net_qty": "saldo_abertura"})
           .merge(day[["agent_id", "net_aggr_qty", "net_qty", "buy_qty", "sell_qty"]]
                  .rename(columns={"net_aggr_qty": "aggr_dia", "net_qty": "saldo_dia",
                                   "buy_qty": "compra_dia", "sell_qty": "venda_dia"}),
                  on="agent_id", how="outer").fillna(0))
    rep.insert(1, "corretora", rep["agent_id"].map(registry.label))
    rep = rep.sort_values("aggr_abertura", key=np.abs, ascending=False).head(top).reset_index(drop=True)

    prices = {}
    if not bars.empty:
        prices["abertura"] = float(bars["open"].iloc[0])
        before = _in_window(bars, None, opening_end)
        if not before.empty:
            prices[f"em_{opening_end:%H%M}"] = float(before["close"].iloc[-1])
        prices["fechamento"] = float(bars["close"].iloc[-1])
    # fatia do volume cujo lado comprador não tem corretora identificada
    unknown = day["agent_id"].map(lambda i: i <= 0 or registry.label(i).startswith("#"))
    total = day["buy_qty"].sum()
    prices["pct_vol_sem_corretora"] = round(100 * day.loc[unknown, "buy_qty"].sum() / total, 2) if total else 0.0
    return rep, prices


def early_flow_study(store: ParquetStore, registry: AgentRegistry, ticker: str,
                     patterns: list[str], opening_start: time = time(9, 0),
                     opening_end: time = time(9, 30)) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Retorna (tabela diária, resumo por grupo de corretoras).

    Para cada dia e cada padrão da watchlist (ex.: "Morgan" soma todos os IDs
    cujo nome contém Morgan): saldo de agressão na janela de abertura e o
    retorno do preço entre o fim da janela e o fechamento.
    """
    groups = registry.match(patterns)
    rows = []
    for date in store.dates("broker_flow"):
        flow = store.read("broker_flow", date, ticker)
        bars = store.read("bars", date, ticker)
        if flow.empty or bars.empty:
            continue
        bars = bars.sort_values("bar_start")
        before = _in_window(bars, None, opening_end)
        after = _in_window(bars, opening_end, None)
        if before.empty or after.empty:
            continue
        p_entry, p_close = float(before["close"].iloc[-1]), float(after["close"].iloc[-1])
        opening = _in_window(flow, opening_start, opening_end)
        for name, ids in groups.items():
            sel = opening[opening["agent_id"].isin(ids)]
            rows.append({"date": date, "grupo": name,
                         "aggr_abertura": int(sel["net_aggr_qty"].sum()),
                         "saldo_abertura": int(sel["net_qty"].sum()),
                         "ret_resto_dia": p_close / p_entry - 1.0})
    daily = pd.DataFrame(rows)
    if daily.empty:
        return daily, pd.DataFrame()

    summary = []
    for name, g in daily.groupby("grupo"):
        active = g[g["aggr_abertura"] != 0]
        same_sign = np.sign(active["aggr_abertura"]) == np.sign(active["ret_resto_dia"])
        summary.append({
            "grupo": name,
            "ids": ",".join(map(str, groups[name])) or "(nenhum ID encontrado)",
            "dias": len(g),
            "dias_ativos": len(active),
            "corr_pearson": g["aggr_abertura"].corr(g["ret_resto_dia"]) if len(g) > 2 else np.nan,
            # Spearman = Pearson dos ranks (evita depender do scipy)
            "corr_spearman": (g["aggr_abertura"].rank().corr(g["ret_resto_dia"].rank())
                              if len(g) > 2 else np.nan),
            "acerto_direcao": same_sign.mean() if len(active) else np.nan,
            "ret_medio_quando_compra": active.loc[active["aggr_abertura"] > 0, "ret_resto_dia"].mean(),
            "ret_medio_quando_vende": active.loc[active["aggr_abertura"] < 0, "ret_resto_dia"].mean(),
        })
    return daily, pd.DataFrame(summary)


def plot_day(store: ParquetStore, registry: AgentRegistry, date: str, ticker: str,
             patterns: list[str], out_path: str) -> str:
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    flow = store.read("broker_flow", date, ticker)
    bars = store.read("bars", date, ticker).sort_values("bar_start")
    fig, (ax_p, ax_f) = plt.subplots(2, 1, figsize=(12, 7), sharex=True,
                                     gridspec_kw={"height_ratios": [1, 1.4]})
    ax_p.plot(bars["bar_start"], bars["close"], color="black", lw=1)
    ax_p.set_ylabel(f"{ticker} (fechamento da barra)")
    for name, ids in registry.match(patterns).items():
        sel = flow[flow["agent_id"].isin(ids)].groupby("bar_start")["net_aggr_qty"].sum()
        if not sel.empty:
            ax_f.plot(sel.index, sel.cumsum(), label=name, lw=1.4)
    ax_f.axhline(0, color="grey", lw=0.6)
    ax_f.set_ylabel("saldo de agressão acumulado (contratos)")
    ax_f.legend(loc="upper left", fontsize=8)
    fig.suptitle(f"Fluxo por corretora — {ticker} {date}")
    fig.autofmt_xdate()
    fig.tight_layout()
    fig.savefig(out_path, dpi=120)
    plt.close(fig)
    return out_path


def parse_time(s: str) -> time:
    return datetime.strptime(s, "%H:%M").time()
