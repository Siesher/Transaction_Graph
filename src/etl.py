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
import tempfile
from typing import Optional

import pandas as pd
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F

# Убеждаемся, что Spark использует тот же Python что и текущая сессия.
# На MDP путь к Python может отличаться от ожидаемого Spark'ом.
os.environ.setdefault('PYSPARK_PYTHON', sys.executable)
os.environ.setdefault('PYSPARK_DRIVER_PYTHON', sys.executable)

from src import config
from src import schema

logger = logging.getLogger(__name__)

# Порог для переключения с IN-клаузы на JOIN через temp view.
# SQL IN с тысячами значений вызывает OOM и деградацию производительности.
_IN_CLAUSE_LIMIT = 500


def _date_to_int(date_str: str) -> int:
    """
    Convert 'YYYY-MM-DD' string to YYYYMMDD integer.

    paymentcounteragent_stran.date_part is partitioned as INT (YYYYMMDD format),
    so date comparisons must use integer literals, not date strings.

    Validates date via datetime.strptime to catch:
    - Non-existent days (e.g. June 31)
    - Missing leading zeros (e.g. '2025-6-1' → 20250601)
    """
    from datetime import datetime
    dt = datetime.strptime(date_str, '%Y-%m-%d')
    return int(dt.strftime('%Y%m%d'))


def _register_client_temp_view(
    spark: SparkSession,
    client_uks: set,
    view_name: str = 'clients_tmp',
    tmp_dir: str = '/tmp',
) -> str:
    """
    Записывает набор client_uk в Parquet через Pandas и регистрирует
    как Spark temp view для использования в JOIN вместо IN-клаузы.

    Избегает spark.createDataFrame() (требует Python worker на MDP).
    Возвращает имя зарегистрированного view.
    """
    pdf = pd.DataFrame({'uk': [int(c) for c in client_uks]})
    tmp_path = os.path.join(tmp_dir, f'{view_name}.parquet')
    pdf.to_parquet(tmp_path, index=False)
    spark.read.parquet(f'file://{tmp_path}').createOrReplaceTempView(view_name)
    logger.info(f"Registered temp view '{view_name}' with {len(client_uks)} clients")
    return view_name


def _make_filter(col: str, client_uks: set, view_name: str) -> str:
    """
    Возвращает SQL-фрагмент для фильтрации по набору client_uk.

    При малом наборе — IN-клауза (быстро).
    При большом — EXISTS с temp view (без OOM).
    """
    if len(client_uks) <= _IN_CLAUSE_LIMIT:
        vals = ','.join(str(int(c)) for c in client_uks)
        return f"{col} IN ({vals})"
    else:
        return f"EXISTS (SELECT 1 FROM {view_name} t WHERE t.uk = {col})"


# =============================================================================
# Hop Expansion
# =============================================================================

def expand_hop(
    spark: SparkSession,
    current_clients: set,
    start_date: str,
    end_date: str,
    min_tx_count: int = 3,
) -> set:
    """
    Expand client set by one hop via transactions.

    Только контрагенты с >= min_tx_count транзакций включаются.
    Это предотвращает взрывной рост через крупные хабы.

    При large client set использует JOIN через temp view вместо IN.
    """
    if not current_clients:
        return current_clients

    cols = schema.PAYMENT_COUNTERAGENT
    start_int = _date_to_int(start_date)
    end_int = _date_to_int(end_date)

    # Регистрируем temp view если набор большой
    if len(current_clients) > _IN_CLAUSE_LIMIT:
        _register_client_temp_view(spark, current_clients, 'hop_clients_tmp')
        client_filter_out = f"EXISTS (SELECT 1 FROM hop_clients_tmp t WHERE t.uk = {cols['client_uk']})"
        client_filter_in  = f"EXISTS (SELECT 1 FROM hop_clients_tmp t WHERE t.uk = {cols['client_contr_uk']})"
    else:
        cl_list = ','.join(str(int(c)) for c in current_clients)
        client_filter_out = f"{cols['client_uk']} IN ({cl_list})"
        client_filter_in  = f"{cols['client_contr_uk']} IN ({cl_list})"

    # Ищем контрагентов в обоих направлениях, фильтруем по min_tx_count
    query = f"""
        SELECT counterparty_uk FROM (
            -- Наши клиенты как плательщики → их контрагенты
            SELECT {cols['client_contr_uk']} AS counterparty_uk,
                   COUNT(*) AS tx_cnt
            FROM {config.TABLE_PAYMENT_COUNTERAGENT}
            WHERE {cols['date_part']} >= {start_int}
              AND {cols['date_part']} <= {end_int}
              AND {client_filter_out}
              AND {cols['client_contr_uk']} IS NOT NULL
              AND ({cols['deleted_flag']} IS NULL OR {cols['deleted_flag']} != 'Y')
            GROUP BY {cols['client_contr_uk']}
            HAVING COUNT(*) >= {min_tx_count}

            UNION ALL

            -- Наши клиенты как контрагенты → их плательщики
            SELECT {cols['client_uk']} AS counterparty_uk,
                   COUNT(*) AS tx_cnt
            FROM {config.TABLE_PAYMENT_COUNTERAGENT}
            WHERE {cols['date_part']} >= {start_int}
              AND {cols['date_part']} <= {end_int}
              AND {client_filter_in}
              AND {cols['client_uk']} IS NOT NULL
              AND ({cols['deleted_flag']} IS NULL OR {cols['deleted_flag']} != 'Y')
            GROUP BY {cols['client_uk']}
            HAVING COUNT(*) >= {min_tx_count}
        ) t
        WHERE counterparty_uk IS NOT NULL
        GROUP BY counterparty_uk
    """

    try:
        df = spark.sql(query)
        new_clients = {int(row[0]) for row in df.collect()}
        expanded = current_clients | new_clients
        logger.info(
            f"Hop expanded: {len(current_clients)} -> {len(expanded)} "
            f"(+{len(expanded - current_clients)} new, min_tx_count={min_tx_count})"
        )
        return expanded
    except Exception as e:
        if 'partition' in str(e).lower() and 'limit' in str(e).lower():
            logger.warning(f"Partition limit hit, narrowing date range: {e}")
            return _expand_hop_with_date_fallback(
                spark, current_clients, start_date, end_date, min_tx_count
            )
        raise


def _expand_hop_with_date_fallback(
    spark: SparkSession,
    current_clients: set,
    start_date: str,
    end_date: str,
    min_tx_count: int = 3,
) -> set:
    """Fallback: split date range in half and merge results."""
    from datetime import datetime

    d_start = datetime.strptime(start_date, '%Y-%m-%d')
    d_end = datetime.strptime(end_date, '%Y-%m-%d')
    d_mid = d_start + (d_end - d_start) / 2
    mid_str = d_mid.strftime('%Y-%m-%d')

    logger.info(f"Splitting date range: [{start_date}, {mid_str}] + [{mid_str}, {end_date}]")
    set1 = expand_hop(spark, current_clients, start_date, mid_str, min_tx_count)
    set2 = expand_hop(spark, current_clients, mid_str, end_date, min_tx_count)
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
    При large client set использует JOIN через temp view.
    """
    c = schema.CLIENT
    ct = schema.CLIENT_TYPE

    client_set = set(client_uks)
    if len(client_set) > _IN_CLAUSE_LIMIT:
        _register_client_temp_view(spark, client_set, 'nodes_clients_tmp')
        where_clause = f"EXISTS (SELECT 1 FROM nodes_clients_tmp t WHERE t.uk = cl.{c['uk']})"
    else:
        cl_list = ','.join(str(int(c_)) for c_ in client_uks)
        where_clause = f"cl.{c['uk']} IN ({cl_list})"

    # ВАЖНО: первичный ключ client_sdim — 'uk', не 'client_uk'!
    # liquidation_flag, closed_flag, dead_flag — не существуют, статус = deleted_flag + end_date
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
            END AS client_status_name,
            COALESCE(cl.{c['okved_code']}, '{config.DEFAULT_OKVED_CODE}') AS okved_code,
            COALESCE(cl.{c['region_code']}, '{config.DEFAULT_REGION_CODE}') AS region_code
        FROM {config.TABLE_CLIENT} cl
        LEFT JOIN {config.TABLE_CLIENT_TYPE} ct
            ON cl.{c['clienttype_uk']} = ct.{ct['uk']}
        WHERE {where_clause}
    """

    df = spark.sql(query)

    # Add INN from account_sdim
    a = schema.ACCOUNT
    if len(client_set) > _IN_CLAUSE_LIMIT:
        _register_client_temp_view(spark, client_set, 'nodes_clients_tmp')
        inn_where = f"EXISTS (SELECT 1 FROM nodes_clients_tmp t WHERE t.uk = {a['client_uk']})"
    else:
        cl_list = ','.join(str(int(c_)) for c_ in client_uks)
        inn_where = f"{a['client_uk']} IN ({cl_list})"

    inn_query = f"""
        SELECT
            {a['client_uk']} AS client_uk,
            FIRST_VALUE({a['client_taxpayer_ccode']}) OVER (
                PARTITION BY {a['client_uk']}
                ORDER BY {a['start_date']} DESC
            ) AS inn
        FROM {config.TABLE_ACCOUNT}
        WHERE {inn_where}
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

    Использует income_flag='N' (исходящие) для направления: source → target.
    При large client set использует JOIN через temp view.
    """
    cols = schema.PAYMENT_COUNTERAGENT
    start_int = _date_to_int(start_date)
    end_int = _date_to_int(end_date)
    date_expr = f"TO_DATE(CAST({cols['date_part']} AS STRING), 'yyyyMMdd')"

    client_set = set(client_uks)
    if len(client_set) > _IN_CLAUSE_LIMIT:
        _register_client_temp_view(spark, client_set, 'tx_clients_tmp')
        src_filter = f"EXISTS (SELECT 1 FROM tx_clients_tmp t WHERE t.uk = {cols['client_uk']})"
        tgt_filter = f"EXISTS (SELECT 1 FROM tx_clients_tmp t WHERE t.uk = {cols['client_contr_uk']})"
    else:
        cl_list = ','.join(str(int(c)) for c in client_uks)
        src_filter = f"{cols['client_uk']} IN ({cl_list})"
        tgt_filter = f"{cols['client_contr_uk']} IN ({cl_list})"

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
          AND {src_filter}
          AND {tgt_filter}
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
            return _extract_tx_edges_fallback(spark, client_uks, start_date, end_date)
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
    Extract authority/representative relationships from clientauthority2clientrb_shist.
    client_u_uk ↔ client_x_uk — две стороны связи.
    """
    rb = schema.AUTHORITY_CLIENT_RB
    client_set = set(client_uks)

    if len(client_set) > _IN_CLAUSE_LIMIT:
        _register_client_temp_view(spark, client_set, 'auth_clients_tmp')
        filter_u = f"EXISTS (SELECT 1 FROM auth_clients_tmp t WHERE t.uk = r.{rb['client_u_uk']})"
        filter_x = f"EXISTS (SELECT 1 FROM auth_clients_tmp t WHERE t.uk = r.{rb['client_x_uk']})"
    else:
        cl_list = ','.join(str(int(c)) for c in client_uks)
        filter_u = f"r.{rb['client_u_uk']} IN ({cl_list})"
        filter_x = f"r.{rb['client_x_uk']} IN ({cl_list})"

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
        WHERE ({filter_u} OR {filter_x})
          AND r.{rb['client_u_uk']} IS NOT NULL
          AND r.{rb['client_x_uk']} IS NOT NULL
          AND r.{rb['client_u_uk']} != r.{rb['client_x_uk']}
          AND (r.{rb['deleted_flag']} IS NULL OR r.{rb['deleted_flag']} != 'Y')
          AND {filter_u}
          AND {filter_x}
    """

    df = spark.sql(query)
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
    clnt2dealsalary_shist (сотрудник) JOIN dealsalary_sdim (работодатель).
    """
    sl = schema.SALARY_DEAL_LINK
    sd = schema.SALARY_DEAL
    client_set = set(client_uks)

    if len(client_set) > _IN_CLAUSE_LIMIT:
        _register_client_temp_view(spark, client_set, 'sal_clients_tmp')
        emp_filter = f"EXISTS (SELECT 1 FROM sal_clients_tmp t WHERE t.uk = d.{sd['client_uk']})"
        ee_filter  = f"EXISTS (SELECT 1 FROM sal_clients_tmp t WHERE t.uk = l.{sl['client_uk']})"
    else:
        cl_list = ','.join(str(int(c)) for c in client_uks)
        emp_filter = f"d.{sd['client_uk']} IN ({cl_list})"
        ee_filter  = f"l.{sl['client_uk']} IN ({cl_list})"

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
        WHERE ({emp_filter} OR {ee_filter})
          AND {emp_filter}
          AND {ee_filter}
          AND (l.{sl['deleted_flag']} IS NULL OR l.{sl['deleted_flag']} != 'Y')
          AND (d.{sd['deleted_flag']} IS NULL OR d.{sd['deleted_flag']} != 'Y')
          AND d.{sd['client_uk']} != l.{sl['client_uk']}
    """

    df = spark.sql(query)
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
    min_tx_count_hop: int = 3,
    max_neighborhood_size: int = 10_000,
) -> dict:
    """
    Extract N-hop neighborhood of a seed company from Hive.

    Args:
        min_tx_count_hop: минимум транзакций с текущим набором для включения
                          контрагента в следующий хоп. Предотвращает взрыв через хабы.
        max_neighborhood_size: жёсткий лимит размера окружения. Если превышен —
                               расширение останавливается с предупреждением.
    """
    logger.info(
        f"Starting extraction: seed={seed_client_uk}, hops={n_hops}, "
        f"dates=[{start_date}, {end_date}], "
        f"min_tx_count_hop={min_tx_count_hop}, max_size={max_neighborhood_size}"
    )

    # Step 1: Expand neighborhood
    clients = {seed_client_uk}
    hop_distances = {seed_client_uk: 0}

    for hop in range(1, n_hops + 1):
        logger.info(f"Expanding hop {hop} (min_tx_count={min_tx_count_hop})...")
        expanded = expand_hop(
            spark, clients, start_date, end_date,
            min_tx_count=min_tx_count_hop,
        )
        new_clients = expanded - clients

        if len(expanded) > max_neighborhood_size:
            logger.warning(
                f"Neighborhood size {len(expanded)} exceeds max_neighborhood_size={max_neighborhood_size}. "
                f"Stopping at hop {hop}. Consider increasing min_tx_count_hop."
            )
            # Включаем только до лимита по числу транзакций (берём что есть на текущем хопе)
            break

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
    tx_edges_df = extract_transaction_edges(spark, client_list, start_date, end_date)

    # Step 4: Extract authority edges
    logger.info("Extracting authority edges...")
    auth_edges_df = extract_authority_edges(spark, client_list)

    # Step 5: Extract salary edges
    logger.info("Extracting salary edges...")
    salary_edges_df = extract_salary_edges(spark, client_list)

    # Step 6: Save to Parquet (локальная ФС через file://)
    output_dir = os.path.abspath(output_dir)
    os.makedirs(output_dir, exist_ok=True)
    spark_dir = f'file://{output_dir}'

    spark_paths = {
        'nodes': f'{spark_dir}/nodes.parquet',
        'transaction_edges': f'{spark_dir}/transaction_edges.parquet',
        'authority_edges': f'{spark_dir}/authority_edges.parquet',
        'salary_edges': f'{spark_dir}/salary_edges.parquet',
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

    # hop_distances через Pandas (избегаем spark.createDataFrame + Python worker).
    # Spark мог оставить директорию на этом пути от предыдущего запуска — удаляем.
    import shutil
    hop_path = local_paths['hop_distances']
    if os.path.isdir(hop_path):
        shutil.rmtree(hop_path)
    hop_pdf = pd.DataFrame(
        [(int(k), int(v)) for k, v in hop_distances.items()],
        columns=['client_uk', 'hop_distance'],
    )
    hop_pdf.to_parquet(hop_path, index=False)

    logger.info(f"Extraction complete. Files saved to {output_dir}/")
    return local_paths
