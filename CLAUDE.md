# Transaction_Graph

Граф транзакций: построение, анализ и визуализация сетей транзакций между клиентами банка. Включает ETL, graph metrics, community detection (Leiden), GNN (DGL), lookalike-сегментацию.

## Environment

- Python: 3.13 (`.venv/Scripts/python.exe`)
- Venv: `D:\Work\03_Exercises\Transaction_Graph\.venv`
- Activate: `.venv\Scripts\activate`
- CPU-only ML (нет CUDA/ROCm)
- uv: `C:\Users\Quary\AppData\Local\Microsoft\WinGet\Packages\astral-sh.uv_Microsoft.Winget.Source_8wekyb3d8bbwe\uv.exe`

## Key Commands

```bash
# Tests
.venv\Scripts\python.exe -m pytest tests/ -x -q

# Lint / format
.venv\Scripts\python.exe -m ruff check src/
.venv\Scripts\python.exe -m ruff format src/

# Install deps
.venv\Scripts\pip.exe install -r requirements.txt
```

## Project Structure

```
src/           # основные модули
  etl.py       # извлечение данных (Spark/pandas)
  graph_builder.py  # построение графа (networkx/igraph)
  analysis.py  # метрики: PageRank, betweenness, Leiden
  filters.py   # фильтрация вершин/рёбер
  viz.py       # визуализация (pyvis, matplotlib)
  gnn.py       # GNN модель (DGL + PyTorch CPU)
  pipeline.py  # оркестрация шагов
  config.py    # параметры
  schema.py    # схемы данных
  synthetic.py # генерация синтетических данных
tests/         # pytest
notebooks/     # Jupyter: 00-08 pipeline steps
lookalike/     # lookalike-сегментация
specs/         # спеки фич: 001, 002, 003
data/          # .gitkeep, runtime parquet/pickle
```

## Stack

- **Graph**: networkx, python-igraph, leidenalg
- **Data**: pandas, numpy, pyarrow
- **ETL**: pyspark (только на MDP кластере, локально — pandas)
- **Viz**: pyvis, matplotlib
- **GNN**: torch (CPU-only), dgl, scikit-learn
- **Dev**: pytest, ruff

## Code Style

- Line length: 100
- Ruff: E, F, I, UP, B, C90 rules
- Type hints — желательны для публичных функций
- Docstrings — numpy-style
- pyspark-код оборачивать в `if spark_available:` для локального запуска

## Features / Specs

- `001-transaction-graph-mvp` — базовый граф + Leiden + pyvis
- `002-graph-hardening-gnn` — GNN (DGL), устойчивость графа
- `003-advanced-metrics-industry` — отраслевые метрики, K-Means сегментация

## Data

- Источник: Hive (MDP) через pyspark
- Локально: синтетические данные через `src/synthetic.py`
- Хранение: `data/*.parquet`, `data/*.pickle` (в .gitignore)
