"""
Конфигурация проекта Transaction Graph MVP.

Все параметры по умолчанию для ETL, графового анализа и визуализации.
"""

import os as _os

# ==============================================================================
# Hive Database
# ==============================================================================

HIVE_DATABASE = 's_dmrb'

# Таблицы
TABLE_CLIENT = f'{HIVE_DATABASE}.client_sdim'
TABLE_ACCOUNT = f'{HIVE_DATABASE}.account_sdim'
TABLE_PAYMENT_COUNTERAGENT = f'{HIVE_DATABASE}.paymentcounteragent_stran'
TABLE_CLIENT_AUTHORITY = f'{HIVE_DATABASE}.clientauthority_shist'
TABLE_AUTHORITY_CLIENT_RB = f'{HIVE_DATABASE}.clientauthority2clientrb_shist'
TABLE_SALARY_DEAL_LINK = f'{HIVE_DATABASE}.clnt2dealsalary_shist'
TABLE_SALARY_DEAL = f'{HIVE_DATABASE}.dealsalary_sdim'
TABLE_CLIENT_TYPE = f'{HIVE_DATABASE}.clienttype_ldim'
TABLE_CLIENT_STATUS = f'{HIVE_DATABASE}.clientstatus_ldim'

# ==============================================================================
# Extraction Parameters
# ==============================================================================

DEFAULT_N_HOPS = 2
DEFAULT_START_DATE = '2025-01-01'
DEFAULT_END_DATE = '2025-12-31'

# ==============================================================================
# Graph Filtering Parameters
# ==============================================================================

DEFAULT_ALPHA = 0.05            # Disparity filter significance level
DEFAULT_MIN_TX_COUNT = 3        # Minimum transactions per edge
DEFAULT_MIN_TOTAL_AMOUNT = 0.0  # Minimum total amount per edge
DEFAULT_MIN_PERIODS = 2         # Minimum quarters edge must appear in

# ==============================================================================
# Analysis Parameters
# ==============================================================================

DEFAULT_GAMMA_VALUES = [0.5, 0.8, 1.0, 1.5, 2.0]  # Leiden CPM resolution
SHELL_SCORE_THRESHOLD = 0.5     # Shell company flagging threshold
MAX_CYCLE_LENGTH = 5            # Maximum cycle length for detection
MIN_CYCLE_LENGTH = 3            # Minimum cycle length

# Shell score weights
SHELL_WEIGHT_FLOW_THROUGH = 0.30
SHELL_WEIGHT_NO_SALARY = 0.25
SHELL_WEIGHT_HIGH_BC_LOW_CC = 0.20
SHELL_WEIGHT_LOW_COUNTERPARTIES = 0.15
SHELL_WEIGHT_BURSTY = 0.10

# --- Wave 1: Extended Metrics ---
EDGE_SCORE_W_BASE = 0.30
EDGE_SCORE_W_BILATERAL = 0.30
EDGE_SCORE_W_NODE = 0.20
EDGE_SCORE_W_STABILITY = 0.20
HUB_CAP_MIN = 20
HUB_CAP_MAX = 50
TOP_K_COUNTERPARTIES = 5

# --- Wave 2: Industry & Behavioral ---
DEFAULT_OKVED_CODE = "00"
DEFAULT_REGION_CODE = "00"
BEHAVIORAL_K_RANGE = (3, 10)
LOOKALIKE_TOP_DECILE = 0.1

# ==============================================================================
# Visualization
# ==============================================================================

NODE_COLOR_MAP = {
    'company': '#4472C4',           # Blue
    'individual': '#70AD47',        # Green
    'sole_proprietor': '#ED7D31',   # Orange
}

EDGE_COLOR_MAP = {
    'transaction': '#808080',       # Gray
    'authority': '#FF0000',         # Red
    'salary': '#00B050',            # Green
    'shared_employees': '#7030A0',  # Purple
}

NODE_SIZE_MIN = 10
NODE_SIZE_MAX = 50
EDGE_WIDTH_MIN = 1
EDGE_WIDTH_MAX = 10

# ==============================================================================
# Node Type Mapping (clienttype_ldim name → internal type)
# ==============================================================================

# Mapping will be refined after DESCRIBE TABLE verification (T006/T019).
# Keys are assumed names from clienttype_ldim; values are internal types.
CLIENT_TYPE_MAP = {
    'Юридическое лицо': 'company',
    'ЮЛ': 'company',
    'Физическое лицо': 'individual',
    'ФЛ': 'individual',
    'Индивидуальный предприниматель': 'sole_proprietor',
    'ИП': 'sole_proprietor',
}

# Default type for unmapped clienttype_ldim values
DEFAULT_NODE_TYPE = 'company'

# ==============================================================================
# MDP Platform Settings
# ==============================================================================

# На MDP (Hadoop-кластер) Spark по умолчанию пишет на HDFS.
# Для MVP используем локальную ФС через file:// префикс,
# т.к. подграф seed-компании небольшой (до ~10K узлов).
# Pickle, HTML и Pandas-операции работают только с локальной ФС.

# Абсолютный путь к корню проекта (определяется от config.py → src/ → project root)
PROJECT_ROOT = _os.path.dirname(_os.path.dirname(_os.path.abspath(__file__)))

# Локальная директория для данных (абсолютный путь)
DATA_DIR = _os.path.join(PROJECT_ROOT, 'data')

# Spark-совместимый путь (file:// для записи на локальную ФС вместо HDFS)
SPARK_DATA_DIR = f'file://{DATA_DIR}'

# Пути для Spark write (используйте эти в etl.py)
SPARK_OUTPUT_NODES = f'{SPARK_DATA_DIR}/nodes.parquet'
SPARK_OUTPUT_TRANSACTION_EDGES = f'{SPARK_DATA_DIR}/transaction_edges.parquet'
SPARK_OUTPUT_AUTHORITY_EDGES = f'{SPARK_DATA_DIR}/authority_edges.parquet'
SPARK_OUTPUT_SALARY_EDGES = f'{SPARK_DATA_DIR}/salary_edges.parquet'
SPARK_OUTPUT_SHARED_EMPLOYEES = f'{SPARK_DATA_DIR}/shared_employees_edges.parquet'

# Пути для Python read/write (pickle, pandas, pyvis — локальная ФС)
OUTPUT_NODES = _os.path.join(DATA_DIR, 'nodes.parquet')
OUTPUT_TRANSACTION_EDGES = _os.path.join(DATA_DIR, 'transaction_edges.parquet')
OUTPUT_AUTHORITY_EDGES = _os.path.join(DATA_DIR, 'authority_edges.parquet')
OUTPUT_SALARY_EDGES = _os.path.join(DATA_DIR, 'salary_edges.parquet')
OUTPUT_SHARED_EMPLOYEES = _os.path.join(DATA_DIR, 'shared_employees_edges.parquet')
OUTPUT_GRAPH_PICKLE = _os.path.join(DATA_DIR, 'graph.pickle')
OUTPUT_FILTERED_GRAPH_PICKLE = _os.path.join(DATA_DIR, 'filtered_graph.pickle')
OUTPUT_GRAPH_METRICS = _os.path.join(DATA_DIR, 'graph_metrics.parquet')
OUTPUT_CLUSTERS = _os.path.join(DATA_DIR, 'clusters.parquet')
