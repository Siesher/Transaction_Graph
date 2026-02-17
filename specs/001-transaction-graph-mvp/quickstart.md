# Quickstart: Transaction Graph MVP

## Prerequisites

- JupyterLab on MDP platform (Hadoop cluster)
- PySpark session available (auto-configured on MDP)
- Read access to `s_dmrb` database in Hive

## Installation

On the MDP JupyterLab terminal:

```bash
pip install --user networkx python-igraph leidenalg pyvis pandas numpy matplotlib
```

If `pip install` is restricted, check if packages are pre-installed:
```python
import networkx, igraph, leidenalg, pyvis
```

## Setup

1. Clone or copy the project to JupyterLab:
```bash
git clone <repo-url> Transaction_Graph
cd Transaction_Graph
```

2. Verify PySpark access to Hive:
```python
from pyspark.sql import SparkSession
spark = SparkSession.builder.enableHiveSupport().getOrCreate()
spark.sql("SHOW TABLES IN s_dmrb LIKE 'client_sdim'").show()
```

3. **Verify column names** (critical first step):
```python
spark.sql("DESCRIBE s_dmrb.paymentcounteragent_stran").show(50, truncate=False)
spark.sql("DESCRIBE s_dmrb.client_sdim").show(70, truncate=False)
```
Update `src/schema.py` if column names differ from assumptions.

## Running the Analysis

Execute notebooks sequentially in JupyterLab:

### Step 1: Data Extraction (~10-30 min)
Open `notebooks/01_data_extraction.ipynb`

```python
# Set your seed company
SEED_CLIENT_UK = 12345678  # Replace with actual client_uk
N_HOPS = 2
START_DATE = '2025-01-01'
END_DATE = '2025-12-31'
```

Run all cells. Output: Parquet files in `data/` directory.

### Step 2: Graph Construction (~2-5 min)
Open `notebooks/02_graph_construction.ipynb`

Run all cells. Output: NetworkX graph saved as pickle + filtered backbone.

### Step 3: Analysis (~1-5 min)
Open `notebooks/03_graph_analysis.ipynb`

Run all cells. Output: Cluster assignments, centrality metrics, shell/cycle flags.

### Step 4: Visualization (~1 min)
Open `notebooks/04_visualization.ipynb`

Run all cells. Output: Interactive HTML graph + summary tables.

## Troubleshooting

| Problem | Solution |
|---------|----------|
| `MetaException: Number of partitions exceeds limit` | Narrow the date range in `START_DATE`/`END_DATE` |
| Empty transaction edges | Check that `SEED_CLIENT_UK` exists in `paymentcounteragent_stran` |
| Column name mismatch | Run `DESCRIBE TABLE` and update `src/schema.py` |
| `ModuleNotFoundError: leidenalg` | `pip install --user leidenalg` |
| pyvis not rendering in JupyterLab | Ensure `notebook=True` and try `File > Trust Notebook` |
| Graph too large (>100K nodes) | Reduce `N_HOPS` to 1 or narrow date range |

## Configuration

All configurable parameters are in `src/config.py`:

```python
# Key parameters
HIVE_DATABASE = 's_dmrb'
DEFAULT_N_HOPS = 2
DEFAULT_ALPHA = 0.05        # Disparity filter significance
DEFAULT_MIN_TX_COUNT = 3    # Minimum transactions per edge
DEFAULT_GAMMA_VALUES = [0.5, 0.8, 1.0, 1.5, 2.0]  # Leiden resolutions
SHELL_SCORE_THRESHOLD = 0.5
MAX_CYCLE_LENGTH = 5
```
