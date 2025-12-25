Pipeline de ponta a ponta com Python + DuckDB: coleta (CSV), tratamento, modelagem e consultas SQL complexas com foco em performance.

## Stack
- Python 3.11+
- DuckDB (warehouse em arquivo local)
- CSVs de exemplo em `data/raw`

## Estrutura
- `src/pipeline.py`: ETL (ingestao + transformacao) e CLI para rodar consultas
- `sql/complex_queries.sql`: consultas analiticas nomeadas
- `data/raw/*.csv`: dados brutos de clientes, produtos e pedidos
- `data/warehouse.duckdb`: banco gerado pelo ETL

## Como rodar
1) Crie ambiente e instale deps
```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```
2) Execute ETL (ingestao + modelagem)
```bash
python -m src.pipeline etl --verbose
```
3) Rode consultas complexas (usa as tabelas `mart.*`)
```bash
python -m src.pipeline queries
```
	- Para ver somente uma consulta: `python -m src.pipeline queries --name revenue_last_30d_by_country`
	- Para plano de execucao: `--explain`
4) Tudo de uma vez (ETL + queries)
```bash
python -m src.pipeline full
```

## Consultas incluidas
- `revenue_last_30d_by_country`: receita e ticket medio por pais em janela fixa
- `customer_retention_segments`: segmentacao por recencia e receita
- `rolling_revenue_14d`: janela movel de 14 dias
- `product_mix_and_rank`: ranking por categoria com window function

## Notas de performance
- O ETL executa `ANALYZE` para estatisticas do otimizador DuckDB
- Filtro e projeção acontecem cedo (CTEs) para reduzir I/O
- Para datasets maiores, DuckDB usa todos os cores por padrao (`PRAGMA threads;`). Ajuste `PRAGMA memory_limit='2GB';` se precisar

## Como estender
- Adicione novos CSVs em `data/raw` e inclua no `ingest_raw`
- Crie modelos adicionais em `transform` (ex.: `mart.fact_payments`)
- Acrescente consultas em `sql/complex_queries.sql` usando o prefixo `-- name:` para que a CLI as descubra
