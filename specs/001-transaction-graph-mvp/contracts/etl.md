# Contract: src/etl.py

Module for PySpark-based data extraction from Hive.

## Functions

### `extract_seed_neighborhood`
```python
def extract_seed_neighborhood(
    spark: SparkSession,
    seed_client_uk: int,
    n_hops: int = 2,
    start_date: str = '2025-01-01',
    end_date: str = '2025-12-31',
    output_dir: str = 'data/'
) -> dict[str, str]:
    """
    Extract N-hop neighborhood of a seed company from Hive.

    Returns dict of output Parquet file paths:
      {'nodes': '...', 'transaction_edges': '...', 'authority_edges': '...', 'salary_edges': '...'}
    """
```

### `extract_nodes`
```python
def extract_nodes(
    spark: SparkSession,
    client_uks: list[int]
) -> DataFrame:
    """
    Extract client records from client_sdim for given client_uk list.
    Joins with clienttype_ldim and clientstatus_ldim.
    Returns DataFrame with columns: client_uk, client_name, first_name,
      inn, node_type, status, is_resident, is_liquidated, birth_date.
    """
```

### `extract_transaction_edges`
```python
def extract_transaction_edges(
    spark: SparkSession,
    client_uks: list[int],
    start_date: str,
    end_date: str
) -> DataFrame:
    """
    Extract and aggregate transactions from paymentcounteragent_stran.
    Filters by date_part range and client set.
    Groups by (source_client_uk, target_client_uk, quarter).
    Returns DataFrame with columns: source_client_uk, target_client_uk,
      period, total_amount, tx_count, avg_amount, std_amount, max_amount,
      min_amount, first_tx_date, last_tx_date.
    """
```

### `extract_authority_edges`
```python
def extract_authority_edges(
    spark: SparkSession,
    client_uks: list[int]
) -> DataFrame:
    """
    Extract authority/representative relationships.
    Joins clientauthority_shist with clientauthority2clientrb_shist.
    Returns DataFrame with columns: authority_uk, company_client_uk,
      representative_client_uk, start_date, end_date, is_active.
    """
```

### `extract_salary_edges`
```python
def extract_salary_edges(
    spark: SparkSession,
    client_uks: list[int]
) -> DataFrame:
    """
    Extract salary project relationships.
    Joins clnt2dealsalary_shist with dealsalary_sdim.
    Returns DataFrame with columns: deal_uk, employer_client_uk,
      employee_client_uk, account_number, start_date, end_date, is_active.
    """
```

### `expand_hop`
```python
def expand_hop(
    spark: SparkSession,
    current_clients: set[int],
    start_date: str,
    end_date: str
) -> set[int]:
    """
    Expand client set by one hop via transactions.
    Queries paymentcounteragent_stran for counterparties of current_clients.
    Returns expanded set including new counterparty client_uks.
    """
```
