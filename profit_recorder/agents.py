"""Cadastro de corretoras (agentes): ID numérico da B3 -> nome.

Os nomes vêm de ``GetAgentName`` da DLL, chamada fora dos callbacks (na
thread principal), e ficam guardados em ``agents.csv`` junto aos dados.
"""

from __future__ import annotations

import csv
import os
import tempfile
from pathlib import Path
from typing import Callable, Iterable

import pandas as pd

Resolver = Callable[[int], tuple[str, str]]  # id -> (nome completo, nome curto)


class AgentRegistry:
    def __init__(self, path: str | os.PathLike):
        self.path = Path(path)
        self.names: dict[int, tuple[str, str]] = {}
        if self.path.exists():
            with self.path.open(newline="", encoding="utf-8") as fh:
                for row in csv.DictReader(fh):
                    self.names[int(row["agent_id"])] = (row["name"], row["short_name"])

    def missing(self, ids: Iterable[int]) -> set[int]:
        return {int(i) for i in ids} - self.names.keys()

    def resolve(self, ids: Iterable[int], resolver: Resolver) -> int:
        """Busca nomes ainda desconhecidos; retorna quantos foram adicionados."""
        added = 0
        for agent_id in sorted(self.missing(ids)):
            try:
                name, short = resolver(agent_id)
            except Exception:  # noqa: BLE001 - um ID ruim não deve parar a gravação
                name, short = "", ""
            self.names[agent_id] = (name or "", short or "")
            added += 1
        if added:
            self.save()
        return added

    def save(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        fd, tmp = tempfile.mkstemp(suffix=".csv")
        with os.fdopen(fd, "w", newline="", encoding="utf-8") as fh:
            w = csv.writer(fh)
            w.writerow(["agent_id", "name", "short_name"])
            for agent_id in sorted(self.names):
                w.writerow([agent_id, *self.names[agent_id]])
        try:
            os.replace(tmp, self.path)
        except OSError:
            import shutil
            shutil.move(tmp, self.path)

    def label(self, agent_id: int) -> str:
        name, short = self.names.get(int(agent_id), ("", ""))
        return short or name or f"#{agent_id}"

    def frame(self) -> pd.DataFrame:
        return pd.DataFrame(
            [(k, *v) for k, v in sorted(self.names.items())],
            columns=["agent_id", "name", "short_name"],
        )

    def match(self, patterns: Iterable[str]) -> dict[str, list[int]]:
        """Mapeia cada padrão (ex.: 'Morgan') para os IDs cujo nome o contém."""
        out: dict[str, list[int]] = {}
        for pat in patterns:
            p = pat.casefold()
            out[pat] = sorted(i for i, (n, s) in self.names.items()
                              if p in n.casefold() or p in s.casefold())
        return out
