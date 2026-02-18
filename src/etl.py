"""
PySpark ETL: извлечение данных из Hive для построения графа.

Стратегия: seed-based extraction — начинаем с seed-компании
и расширяем до N хопов через транзакции контрагентов.

Работает на MDP (JupyterLab на Hadoop-кластере).
Все Parquet-файлы пишутся на локальную ФС через file:// префикс,
чтобы они были доступны и для Spark, и для Pandas/pickle.

ВЕРИФИЦИРОВАНО: имена колонок проверены через 00_verify_schema.ipynb (2026-02-17).
"""

import logging
import os
import sys
from typing import Optional

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F

# Убеждаемся, что Spark использует тот же Python что и текущая сессия.
# На MDP путь к Python может отличаться от ожидаемого Spark'ом.
os.environ.setdefault('PYSPARK_PYTHON', sys.executable)
os.environ.setdefault('PYSPARK_DRIVER_PYTHON', sys.executable)

from src import config
from src import schema

logger = logging.getLogger(__name__)


def _date_to_int(date_str: str) -> int:
    """
    Convert 'YYYY-MM-DD' string to YYYYMMDD integer.

    paymentcounteragent_stran.date_part is partitioned as INT (YYYYMMDD format),
    so date comparisons must use integer literals, not date strings.
    """
    return int(date_str.replace('-', ''))


# =============================================================================
# Hop Expansion
# =============================================================================

def expand_hop(
    spark: SparkSession,
    current_clients: set,
    start_date: str,
    end_date: str,
) -> set:
    """
    Expand client set by one hop via transactions.

    Queries paymentcounteragent_stran for counterparties of current_clients.
    Uses income_flag='N' (outgoing) to find who our clients pay,
    and income_flag='Y' (incoming) to find who pays our clients.
    """
    if not current_clients:
        return current_clients

    client_list = ','.join(str(int(c)) for c in current_clients)
    cols = schema.PAYMENT_COUNTERAGENT

    # date_part — INT тип (YYYYMMDD), сравниваем с целыми числами
    start_int = _date_to_int(start_date)
    end_int = _date_to_int(end_date)

    # Ищем контрагентов в обоих направлениях
    query = f"""
        SELECT DISTINCT counterparty_uk FROM (
            -- Наши клиенты как плательщики → их контрагенты
            SELECT {cols['client_contr_uk']} AS counterparty_uk
            FROM {config.TABLE_PAYMENT_COUNTERAGENT}
            WHERE {cols['date_part']} >= {start_int}
              AND {cols['date_part']} <= {end_int}
              AND {cols['client_uk']} IN ({client_list})
              AND {cols['client_contr_uk']} IS NOT NULL
              AND ({cols['deleted_flag']} IS NULL OR {cols['deleted_flag']} != 'Y')

            UNION

            -- Наши клиенты как контрагенты → их плательщики
            SELECT {cols['client_uk']} AS counterparty_uk
            FROM {config.TABLE_PAYMENT_COUNTERAGENT}
            WHERE {cols['date_part']} >= {start_int}
              AND {cols['date_part']} <= {end_int}
              AND {cols['client_contr_uk']} IN ({client_list})
              AND {cols['client_uk']} IS NOT NULL
              AND ({cols['deleted_flag']} IS NULL OR {cols['deleted_flag']} != 'Y')
        ) t
        WHERE counterparty_uk IS NOT NULL
    """

    try:
        df = spark.sql(query)
        new_clients = {int(row[0]) for row in df.collect()}
        expanded = current_clients | new_clients
        logger.info(
            f"Hop expanded: {len(current_clients)} -> {len(expanded)} "
            f"(+{len(expanded - current_clients)} new)"
        )
        return expanded
    except Exception as e:
        if 'partition' in str(e).lower() and 'limit' in str(e).lower():
            logger.warning(f"Partition limit hit, narrowing date range: {e}")
            return _expand_hop_with_date_fallback(
                spark, current_clients, start_date, end_date
            )
        raise


def _expand_hop_with_date_fallback(
    spark: SparkSession,
    current_clients: set,
    start_date: str,
    end_date: str,
) -> set:
    """Fallback: split date range in half and merge results."""
    from datetime import datetime, timedelta

    d_start = datetime.strptime(start_date, '%Y-%m-%d')
    d_end = datetime.strptime(end_date, '%Y-%m-%d')
    d_mid = d_start + (d_end - d_start) / 2
    mid_str = d_mid.strftime('%Y-%m-%d')

    logger.info(f"Splitting date range: [{start_date}, {mid_str}] + [{mid_str}, {end_date}]")
    set1 = expand_hop(spark, current_clients, start_date, mid_str)
    set2 = expand_hop(spark, current_clients, mid_str, end_date)
    return set1 | set2


# =============================================================================
# Node Extraction
# =============================================================================

def extract_nodes(
    spark: SparkSession,
    client_uks: list,
) -> DataFrame:
    """
    Extract client records from client_sdim for given client_uk list.

    Joins with clienttype_ldim for client type name.
    Note: clientstatus_uk does NOT exist — status derived from flags.
    """
    client_list = ','.join(str(int(c)) for c in client_uks)
    c = schema.CLIENT
    ct = schema.CLIENT_TYPE

    # ВАЖНО: первичный ключ client_sdim — 'uk', не 'client_uk'!
    # liquidation_flag, closed_flag, dead_flag — не существуют, статус = deleted_flag
    query = f"""
        SELECT
            cl.{c['uk']} AS client_uk,
            cl.{c['client_name']} AS client_name,
            cl.{c['first_name']} AS first_name,
            cl.{c['last_name']} AS last_name,
            cl.{c['middle_name']} AS middle_name,
            cl.{c['birth_date']} AS birth_date,
            cl.{c['resident_flag']} AS resident_flag,
            cl.{c['entrepreneur_flag']} AS entrepreneur_flag,
            cl.{c['end_date']} AS end_date,
            cl.{c['deleted_flag']} AS deleted_flag,
            cl.{c['blacklist_flag']} AS blacklist_flag,
            cl.{c['default_flag']} AS default_flag,
            ct.{ct['name']} AS client_type_name,
            CASE
                WHEN cl.{c['deleted_flag']} = 'Y' THEN 'Удалён'
                WHEN cl.{c['end_date']} IS NOT NULL
                 AND cl.{c['end_date']} < CURRENT_DATE() THEN 'Закрыт'
                ELSE 'Активный'
            END AS client_status_name
        FROM {config.TABLE_CLIENT} cl
        LEFT JOIN {config.TABLE_CLIENT_TYPE} ct
            ON cl.{c['clienttype_uk']} = ct.{ct['uk']}
        WHERE cl.{c['uk']} IN ({client_list})
    """

    df = spark.sql(query)

    # Add INN from account_sdim (first non-null taxpayer code per client)
    a = schema.ACCOUNT
    inn_query = f"""
        SELECT
            {a['client_uk']} AS client_uk,
            FIRST_VALUE({a['client_taxpayer_ccode']}) OVER (
                PARTITION BY {a['client_uk']}
                ORDER BY {a['start_date']} DESC
            ) AS inn
        FROM {config.TABLE_ACCOUNT}
        WHERE {a['client_uk']} IN ({client_list})
          AND {a['client_taxpayer_ccode']} IS NOT NULL
          AND {a['client_taxpayer_ccode']} != ''
    """
    try:
        inn_df = spark.sql(inn_query).dropDuplicates(['client_uk'])
        df = df.join(inn_df, on='client_uk', how='left')
    except Exception as e:
        logger.warning(f"Could not extract INN from account_sdim: {e}")
        df = df.withColumn('inn', F.lit(None).cast('string'))

    logger.info(f"Extracted {df.count()} nodes")
    return df


# =============================================================================
# Transaction Edge Extraction
# =============================================================================

def extract_transaction_edges(
    spark: SparkSession,
    client_uks: list,
    start_date: str,
    end_date: str,
) -> DataFrame:
    """
    Extract and aggregate transactions from paymentcounteragent_stran.

    Использует income_flag='N' (исходящие платежи) для определения направления:
    source = client_uk (плательщик), target = client_contr_uk (получатель).

    Берём только income_flag='N', чтобы избежать двойного учёта
    (та же транзакция для получателя будет income_flag='Y').

    Groups by (source, target, quarter). Uses rur_amt (рублёвый эквивалент).
    """
    client_list = ','.join(str(int(c)) for c in client_uks)
    cols = schema.PAYMENT_COUNTERAGENT

    # date_part — INT тип (YYYYMMDD), сравниваем с целыми числами
    start_int = _date_to_int(start_date)
    end_int = _date_to_int(end_date)

    # Для YEAR/QUARTER конвертируем INT → DATE через CAST
    date_expr = f"TO_DATE(CAST({cols['date_part']} AS STRING), 'yyyyMMdd')"

    query = f"""
        SELECT
            {cols['client_uk']} AS source_client_uk,
            {cols['client_contr_uk']} AS target_client_uk,
            CONCAT(
                CAST(YEAR({date_expr}) AS STRING),
                '-Q',
                CAST(QUARTER({date_expr}) AS STRING)
            ) AS period,
            SUM({cols['rur_amt']}) AS total_amount,
            COUNT(*) AS tx_count,
            AVG({cols['rur_amt']}) AS avg_amount,
            STDDEV({cols['rur_amt']}) AS std_amount,
            MAX({cols['rur_amt']}) AS max_amount,
            MIN({cols['rur_amt']}) AS min_amount,
            MIN({cols['date_part']}) AS first_tx_date,
            MAX({cols['date_part']}) AS last_tx_date
        FROM {config.TABLE_PAYMENT_COUNTERAGENT}
        WHERE {cols['date_part']} >= {start_int}
          AND {cols['date_part']} <= {end_int}
          AND {cols['income_flag']} = 'N'
          AND {cols['client_uk']} IN ({client_list})
          AND {cols['client_contr_uk']} IN ({client_list})
          AND {cols['client_uk']} != {cols['client_contr_uk']}
          AND {cols['rur_amt']} IS NOT NULL
          AND {cols['rur_amt']} > 0
          AND ({cols['deleted_flag']} IS NULL OR {cols['deleted_flag']} != 'Y')
        GROUP BY
            {cols['client_uk']},
            {cols['client_contr_uk']},
            CONCAT(
                CAST(YEAR({date_expr}) AS STRING),
                '-Q',
                CAST(QUARTER({date_expr}) AS STRING)
            )
    """

    try:
        df = spark.sql(query)
        count = df.count()
        logger.info(f"Extracted {count} transaction edge records")
        return df
    except Exception as e:
        if 'partition' in str(e).lower() and 'limit' in str(e).lower():
            logger.warning(f"Partition limit, trying with narrower date range: {e}")
            return _extract_tx_edges_fallback(
                spark, client_uks, start_date, end_date
            )
        raise


def _extract_tx_edges_fallback(
    spark: SparkSession,
    client_uks: list,
    start_date: str,
    end_date: str,
) -> DataFrame:
    """Split date range and union results."""
    from datetime import datetime

    d_start = datetime.strptime(start_date, '%Y-%m-%d')
    d_end = datetime.strptime(end_date, '%Y-%m-%d')
    d_mid = d_start + (d_end - d_start) / 2
    mid_str = d_mid.strftime('%Y-%m-%d')

    df1 = extract_transaction_edges(spark, client_uks, start_date, mid_str)
    df2 = extract_transaction_edges(spark, client_uks, mid_str, end_date)
    return df1.unionByName(df2)


# =============================================================================
# Authority Edge Extraction
# =============================================================================

def extract_authority_edges(
    spark: SparkSession,
    client_uks: list,
) -> DataFrame:
    """
    Extract authority/representative relationships.

    Использует clientauthority2clientrb_shist напрямую:
    - client_u_uk и client_x_uk — две стороны связи
    - c2clinkrole_uk — роль связи (FK → c2clinkrole_sdim)

    Также JOIN с clientauthority_shist для дополнительных атрибутов.
    """
    client_list = ','.join(str(int(c)) for c in client_uks)
    rb = schema.AUTHORITY_CLIENT_RB
    ca = schema.CLIENT_AUTHORITY

    # Основной источник — clientauthority2clientrb_shist
    # Содержит прямые связи client_u_uk ↔ client_x_uk
    query = f"""
        SELECT DISTINCT
            r.{rb['client_u_uk']} AS company_client_uk,
            r.{rb['client_x_uk']} AS representative_client_uk,
            r.{rb['c2clinkrole_uk']} AS link_role_uk,
            r.{rb['start_date']} AS start_date,
            r.{rb['end_date']} AS end_date,
            CASE
                WHEN r.{rb['end_date']} > CURRENT_DATE()
                 AND (r.{rb['deleted_flag']} IS NULL OR r.{rb['deleted_flag']} != 'Y')
                THEN true ELSE false
            END AS is_active
        FROM {config.TABLE_AUTHORITY_CLIENT_RB} r
        WHERE (r.{rb['client_u_uk']} IN ({client_list})
               OR r.{rb['client_x_uk']} IN ({client_list}))
          AND r.{rb['client_u_uk']} IS NOT NULL
          AND r.{rb['client_x_uk']} IS NOT NULL
          AND r.{rb['client_u_uk']} != r.{rb['client_x_uk']}
          AND (r.{rb['deleted_flag']} IS NULL OR r.{rb['deleted_flag']} != 'Y')
    """

    df = spark.sql(query)

    # Filter: both ends must be in our client set
    df = df.filter(
        F.col('company_client_uk').isin(client_uks)
        & F.col('representative_client_uk').isin(client_uks)
    )

    logger.info(f"Extracted {df.count()} authority edge records")
    return df


# =============================================================================
# Salary Edge Extraction
# =============================================================================

def extract_salary_edges(
    spark: SparkSession,
    client_uks: list,
) -> DataFrame:
    """
    Extract salary project relationships.

    Joins clnt2dealsalary_shist (сотрудник) с dealsalary_sdim (зарплатный проект).
    Связь: dealsalary_uk → UK (dealsalary_sdim).
    Работодатель: dealsalary_sdim.CLIENT_UK.
    """
    client_list = ','.join(str(int(c)) for c in client_uks)
    sl = schema.SALARY_DEAL_LINK
    sd = schema.SALARY_DEAL

    query = f"""
        SELECT
            d.{sd['uk']} AS deal_uk,
            d.{sd['client_uk']} AS employer_client_uk,
            l.{sl['client_uk']} AS employee_client_uk,
            l.{sl['account_main_number']} AS account_number,
            l.{sl['start_date']} AS start_date,
            l.{sl['end_date']} AS end_date,
            CASE
                WHEN l.{sl['end_date']} > CURRENT_DATE()
                 AND (l.{sl['deleted_flag']} IS NULL OR l.{sl['deleted_flag']} != 'Y')
                THEN true ELSE false
            END AS is_active
        FROM {config.TABLE_SALARY_DEAL_LINK} l
        JOIN {config.TABLE_SALARY_DEAL} d
            ON l.{sl['dealsalary_uk']} = d.{sd['uk']}
        WHERE (d.{sd['client_uk']} IN ({client_list})
               OR l.{sl['client_uk']} IN ({client_list}))
          AND (l.{sl['deleted_flag']} IS NULL OR l.{sl['deleted_flag']} != 'Y')
          AND (d.{sd['deleted_flag']} IS NULL OR d.{sd['deleted_flag']} != 'Y')
    """

    df = spark.sql(query)

    # Filter: both ends must be in our client set
    df = df.filter(
        F.col('employer_client_uk').isin(client_uks)
        & F.col('employee_client_uk').isin(client_uks)
    )
    # Remove self-references
    df = df.filter(F.col('employer_client_uk') != F.col('employee_client_uk'))

    logger.info(f"Extracted {df.count()} salary edge records")
    return df


# =============================================================================
# Orchestrator
# =============================================================================

def extract_seed_neighborhood(
    spark: SparkSession,
    seed_client_uk: int,
    n_hops: int = 2,
    start_date: str = '2025-01-01',
    end_date: str = '2025-12-31',
    output_dir: str = 'data/',
) -> dict:
    """
    Extract N-hop neighborhood of a seed company from Hive.

    Steps:
    1. Expand neighborhood hop by hop via transactions
    2. Extract node attributes for all discovered clients
    3. Extract transaction edges between neighborhood clients
    4. Extract authority relationships
    5. Extract salary relationships
    6. Save all to Parquet

    Returns dict of output Parquet file paths.
    """
    logger.info(
        f"Starting extraction: seed={seed_client_uk}, "
        f"hops={n_hops}, dates=[{start_date}, {end_date}]"
    )

    # Step 1: Expand neighborhood
    clients = {seed_client_uk}
    hop_distances = {seed_client_uk: 0}

    for hop in range(1, n_hops + 1):
        logger.info(f"Expanding hop {hop}...")
        expanded = expand_hop(spark, clients, start_date, end_date)
        new_clients = expanded - clients
        for c in new_clients:
            hop_distances[c] = hop
        clients = expanded
        logger.info(f"After hop {hop}: {len(clients)} total clients")

    client_list = list(clients)
    logger.info(f"Total neighborhood size: {len(client_list)} clients")

    # Step 2: Extract nodes
    logger.info("Extracting nodes...")
    nodes_df = extract_nodes(spark, client_list)

    # Step 3: Extract transaction edges
    logger.info("Extracting transaction edges...")
    tx_edges_df = extract_transaction_edges(
        spark, client_list, start_date, end_date
    )

    # Step 4: Extract authority edges
    logger.info("Extracting authority edges...")
    auth_edges_df = extract_authority_edges(spark, client_list)

    # Step 5: Extract salary edges
    logger.info("Extracting salary edges...")
    salary_edges_df = extract_salary_edges(spark, client_list)

    # Step 6: Save to Parquet (локальная ФС через file://)
    # На MDP Spark по умолчанию пишет на HDFS.
    # Используем абсолютный путь с file:// для записи на локальную ФС.
    output_dir = os.path.abspath(output_dir)
    os.makedirs(output_dir, exist_ok=True)
    spark_dir = f'file://{output_dir}'

    # Пути для Spark write (file://) и для возврата (локальные)
    spark_paths = {
        'nodes': f'{spark_dir}/nodes.parquet',
        'transaction_edges': f'{spark_dir}/transaction_edges.parquet',
        'authority_edges': f'{spark_dir}/authority_edges.parquet',
        'salary_edges': f'{spark_dir}/salary_edges.parquet',
        'hop_distances': f'{spark_dir}/hop_distances.parquet',
    }
    local_paths = {
        'nodes': os.path.join(output_dir, 'nodes.parquet'),
        'transaction_edges': os.path.join(output_dir, 'transaction_edges.parquet'),
        'authority_edges': os.path.join(output_dir, 'authority_edges.parquet'),
        'salary_edges': os.path.join(output_dir, 'salary_edges.parquet'),
        'hop_distances': os.path.join(output_dir, 'hop_distances.parquet'),
    }

    logger.info(f"Saving to Parquet (local FS): {output_dir}")
    nodes_df.write.mode('overwrite').parquet(spark_paths['nodes'])
    tx_edges_df.write.mode('overwrite').parquet(spark_paths['transaction_edges'])
    auth_edges_df.write.mode('overwrite').parquet(spark_paths['authority_edges'])
    salary_edges_df.write.mode('overwrite').parquet(spark_paths['salary_edges'])

    # Save hop distances via Pandas (small data, avoids spark.createDataFrame
    # which requires Python workers at /opt/anaconda37/bin/python on MDP)
    import pandas as pd
    hop_pdf = pd.DataFrame(
        [(int(k), int(v)) for k, v in hop_distances.items()],
        columns=['client_uk', 'hop_distance'],
    )
    hop_pdf.to_parquet(local_paths['hop_distances'], index=False)

    logger.info(f"Extraction complete. Files saved to {output_dir}/")
    return local_paths
