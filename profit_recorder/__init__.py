"""Gravador de fluxo por corretora (WIN/WDO) usando a ProfitDLL da Nelogica.

O foco é o comportamento das corretoras (agentes) ao longo do pregão:
quanto cada uma comprou/vendeu e quanto disso foi agressão, em barras de
tempo (padrão 1 minuto). Os negócios brutos são guardados apenas como fonte
para recalcular as agregações de forma exata.
"""

__version__ = "0.1.0"
