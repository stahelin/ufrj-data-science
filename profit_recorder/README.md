# profit_recorder — fluxo por corretora em WIN/WDO

Gravador de dados que usa a **ProfitDLL** (Nelogica) para registrar, minuto a
minuto, **quanto cada corretora comprou, vendeu e agrediu** no mini índice e no
mini dólar. O objetivo é estudar se o comportamento de corretoras grandes
(Morgan Stanley, UBS, Goldman, J.P. Morgan…) na abertura antecipa o resto do
pregão.

## O que é gravado

| Pasta | Conteúdo | Uso |
|---|---|---|
| `broker_flow/` | por minuto × corretora: `buy_qty`, `sell_qty`, `net_qty`, `buy_aggr_qty`, `sell_aggr_qty`, `net_aggr_qty`, volumes financeiros, nº de negócios | **o dado principal** |
| `bars/` | OHLC de 1 min, volume, agressão compradora/vendedora total | preço para medir retornos |
| `daily/` | ajuste, contratos em aberto, volume comprador/vendedor do dia | contexto |
| `trades/` | negócios brutos (preço, qtd, corretora compradora/vendedora, tipo) | fonte para recalcular `broker_flow` exatamente; não é analisado diretamente |
| `agents.csv` | ID da corretora → nome | legenda |

**Saldo de agressão** (`net_aggr_qty`) = contratos que a corretora comprou
*agredindo* menos os que vendeu *agredindo*. Mede iniciativa (quem foi ao
mercado), diferente do saldo total (`net_qty`), que inclui ordens passivas
executadas.

## Instalação (Windows, Python 3.11+ 64 bits)

```bat
pip install -r requirements.txt
copy config.example.toml config.toml   &REM edite dll_path e data_dir
setx PROFIT_KEY "sua-chave"
setx PROFIT_USER "seu-usuario"
setx PROFIT_PASSWORD "sua-senha"
```

Só é preciso o `ProfitDLL.dll` (64 bits). Os arquivos de exemplo da Nelogica
(`main.py`, `profit_dll.py`, `profitTypes.py`) não são usados; as estruturas
necessárias estão em `profit_recorder/dll.py`.

## Uso

```bat
:: 1) uma vez: baixa os últimos ~30 dias (limite da DLL), um pregão por requisição
python -m profit_recorder backfill

:: 2) todo dia útil (agende run_recorder.bat às 08:45 no Agendador de Tarefas)
python -m profit_recorder live

:: 3) análise — não precisa da DLL, roda em qualquer máquina com acesso ao Drive
python -m profit_recorder report --plot fluxo.png        :: ranking do último dia
python -m profit_recorder report --date 2026-11-16 --opening-end 10:00
python -m profit_recorder study --csv estudo.csv          :: abertura → resto do dia
```

O modo `live` encerra sozinho em `session_end`, compacta o dia em
`day.parquet` (deduplica negócios e recalcula o fluxo por corretora a partir
deles) e grava os nomes das corretoras novas.

## Google Drive

Use o **Google Drive para Computador** e aponte `data_dir` para uma pasta dele
(ex.: `G:/Meu Drive/profit_data`). Cada arquivo é escrito por inteiro em
`staging_dir` (local) e só depois movido para o Drive, então o Drive nunca
sincroniza arquivo pela metade. Grava-se um arquivo novo a cada
`flush_interval_s` (padrão 10 min); se o PC cair, perde-se no máximo esse
intervalo, que pode ser recuperado no dia seguinte com `backfill`.

Espaço estimado: dezenas de MB por pregão para WIN + WDO (o bruto domina). Com
`save_raw_trades = false` fica bem menor, mas reinícios no meio do dia podem
gerar contagem dupla que não dá para corrigir.

## Limites conhecidos

* **Histórico**: a DLL só fornece negócios dos últimos 30 dias, e para WIN/WDO
  cada requisição deve cobrir menos de 10 dias. Por isso é essencial manter o
  `live` rodando todos os dias para acumular amostra.
* **Contrato**: `WINFUT`/`WDOFUT` seguem o vencimento vigente; no backfill de
  dias antes de uma rolagem, use `--ticker WINZ26` etc., se necessário.
* **Edições** de negócios (`TC_IS_EDIT`) são guardadas no bruto, mas não
  entram nas agregações.
* **Corretora ≠ cliente**: o fluxo de uma corretora mistura muitos clientes
  (estrangeiros, fundos, HFTs que usam a corretora como acesso). Verifique se
  há negócios com corretora não identificada; a proporção aparece no
  `report`.
* **Teste de hipótese**: `study` mostra correlação e taxa de acerto, mas com
  menos de ~60 pregões isso é ruído. Antes de operar, valide fora da amostra e
  com custos.

## Testes

```bash
python -m pytest -q tests
```

Os testes rodam em qualquer SO: usam dados sintéticos e uma DLL falsa para o
backfill.
