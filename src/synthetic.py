"""
Генератор синтетических данных для тестирования pipeline без Hive.

Создаёт Parquet-файлы, имитирующие выход ETL (notebook 01),
чтобы можно было тестировать notebooks 02→03→04 локально.

Использование:
    from src.synthetic import generate_synthetic_data
    generate_synthetic_data(output_dir='data/', seed=42)
"""

import logging
import os
import random

import numpy as np
import pandas as pd

from src import config

logger = logging.getLogger(__name__)


def generate_synthetic_data(
    output_dir: str = None,
    n_companies: int = 30,
    n_individuals: int = 50,
    n_ips: int = 5,
    n_tx_edges: int = 200,
    n_authority_edges: int = 15,
    n_salary_edges: int = 40,
    n_periods: int = 4,
    seed: int = 42,
) -> dict:
    """
    Generate synthetic Parquet files mimicking ETL output.

    Creates a realistic corporate graph with:
    - A "parent" company (seed, high connectivity)
    - Several subsidiaries
    - A shell company pattern (high flow-through, no salary)
    - A circular payment cycle (A→B→C→A)
    - Authority/salary edges connecting individuals to companies

    Returns dict of output file paths.
    """
    if output_dir is None:
        output_dir = config.DATA_DIR

    os.makedirs(output_dir, exist_ok=True)
    rng = np.random.RandomState(seed)
    random.seed(seed)

    # OKVED and region code pools (per research.md Decision 3)
    okved_codes = [
        '01', '10', '14', '20', '23', '25', '41', '43', '45', '46',
        '47', '49', '52', '62', '64', '68', '69', '70', '71', '86',
    ]
    region_codes = [
        '77', '78', '50', '23', '16', '54', '66', '63', '74', '52',
    ]

    # =================================================================
    # Generate Nodes
    # =================================================================
    total = n_companies + n_individuals + n_ips
    client_uks = list(range(1000, 1000 + total))

    company_uks = client_uks[:n_companies]
    individual_uks = client_uks[n_companies:n_companies + n_individuals]
    ip_uks = client_uks[n_companies + n_individuals:]

    seed_uk = company_uks[0]  # First company is the seed

    # Company names
    company_names = [
        'ООО "Альфа Групп"', 'ООО "Бета Строй"', 'ООО "Гамма Трейд"',
        'ЗАО "Дельта Инвест"', 'ООО "Эпсилон Лог"', 'ООО "Зета Финанс"',
        'ООО "Эта Консалт"', 'ООО "Тета Сервис"', 'ООО "Йота Транс"',
        'ООО "Каппа Энерго"', 'ООО "Лямбда Тех"', 'ООО "Мю Ресурс"',
        'ООО "Ню Проджект"', 'ООО "Кси Индустри"', 'ООО "Омикрон Холд"',
        'ООО "Пи Девелоп"', 'ООО "Ро Медиа"', 'ООО "Сигма Агро"',
        'ООО "Тау Фарма"', 'ООО "Ипсилон Стил"', 'ООО "Фи Комм"',
        'ООО "Хи Строй"', 'ООО "Пси Лоджистик"', 'ООО "Омега Ритейл"',
        'ООО "Шелл-1 Транзит"', 'ООО "Шелл-2 Поток"',  # Shell companies
        'ООО "Цикл-А"', 'ООО "Цикл-Б"', 'ООО "Цикл-В"',  # Cycle companies
        'ООО "Прочие"',
    ]

    nodes_records = []

    for i, uk in enumerate(company_uks):
        name = company_names[i] if i < len(company_names) else f'ООО "Компания-{i}"'
        hop = 0 if uk == seed_uk else (1 if i < 10 else 2)

        # Last company (index 29, "ООО Прочие") is marked as deleted
        deleted = 'Y' if i == n_companies - 1 else 'N'
        status = 'Ликвидирован' if deleted == 'Y' else 'Активный'

        nodes_records.append({
            'client_uk': uk,
            'client_name': name,
            'first_name': None,
            'middle_name': None,
            'birth_date': None,
            'resident_flag': 'Y',
            'deleted_flag': deleted,
            'end_date': '5999-12-31',
            'client_type_name': 'Юридическое лицо',
            'client_status_name': status,
            'inn': f'{7700000000 + i}',
            'okved_code': random.choice(okved_codes),
            'region_code': random.choice(region_codes),
        })

    for i, uk in enumerate(individual_uks):
        # Last individual has end_date in the past (closed/deregistered)
        if i == n_individuals - 1:
            end_dt = '2020-01-01'
        else:
            end_dt = '5999-12-31'

        nodes_records.append({
            'client_uk': uk,
            'client_name': f'Иванов-{i}',
            'first_name': random.choice(['Иван', 'Пётр', 'Сергей', 'Анна', 'Мария']),
            'middle_name': random.choice(['Иванович', 'Петрович', 'Сергеевич', 'Ивановна', None]),
            'birth_date': f'{rng.randint(1960, 2000)}-{rng.randint(1,12):02d}-{rng.randint(1,28):02d}',
            'resident_flag': 'Y',
            'deleted_flag': 'N',
            'end_date': end_dt,
            'client_type_name': 'Физическое лицо',
            'client_status_name': 'Активный',
            'inn': None,
            'okved_code': config.DEFAULT_OKVED_CODE,
            'region_code': config.DEFAULT_REGION_CODE,
        })

    for i, uk in enumerate(ip_uks):
        nodes_records.append({
            'client_uk': uk,
            'client_name': f'ИП Сидоров-{i}',
            'first_name': f'Предприниматель-{i}',
            'middle_name': None,
            'birth_date': f'{rng.randint(1970, 1995)}-01-01',
            'resident_flag': 'Y',
            'deleted_flag': 'N',
            'end_date': '5999-12-31',
            'client_type_name': 'Индивидуальный предприниматель',
            'client_status_name': 'Активный',
            'inn': f'{7700100000 + i}',
            'okved_code': random.choice(okved_codes),
            'region_code': random.choice(region_codes),
        })

    nodes_df = pd.DataFrame(nodes_records)

    # Hop distances
    hop_records = []
    for _, row in nodes_df.iterrows():
        uk = row['client_uk']
        if uk == seed_uk:
            hop = 0
        elif uk in company_uks[:10]:
            hop = 1
        else:
            hop = 2
        hop_records.append({'client_uk': uk, 'hop_distance': hop})
    hop_df = pd.DataFrame(hop_records)

    # =================================================================
    # Generate Transaction Edges
    # =================================================================
    periods = ['2025-Q1', '2025-Q2', '2025-Q3', '2025-Q4'][:n_periods]
    tx_records = []

    # Seed company → subsidiaries (high volume)
    for sub_uk in company_uks[1:6]:
        for period in periods:
            amount = rng.uniform(5_000_000, 50_000_000)
            tx_records.append(_make_tx_edge(seed_uk, sub_uk, period, amount, rng))
            # Some reverse flow
            if rng.random() > 0.3:
                rev_amount = amount * rng.uniform(0.1, 0.5)
                tx_records.append(_make_tx_edge(sub_uk, seed_uk, period, rev_amount, rng))

    # Inter-subsidiary transactions
    for i in range(1, 6):
        for j in range(1, 6):
            if i != j and rng.random() > 0.6:
                for period in periods:
                    if rng.random() > 0.3:
                        amount = rng.uniform(1_000_000, 10_000_000)
                        tx_records.append(_make_tx_edge(company_uks[i], company_uks[j], period, amount, rng))

    # Shell company pattern: high flow-through
    shell_1 = company_uks[24] if len(company_uks) > 24 else company_uks[-3]
    shell_2 = company_uks[25] if len(company_uks) > 25 else company_uks[-2]
    for period in periods:
        # Big inflow then big outflow (flow-through ≈ 1.0)
        in_amount = rng.uniform(10_000_000, 30_000_000)
        tx_records.append(_make_tx_edge(company_uks[2], shell_1, period, in_amount, rng))
        tx_records.append(_make_tx_edge(shell_1, company_uks[3], period, in_amount * 0.95, rng))
        tx_records.append(_make_tx_edge(company_uks[4], shell_2, period, in_amount * 0.8, rng))
        tx_records.append(_make_tx_edge(shell_2, company_uks[5], period, in_amount * 0.78, rng))

    # Circular payment: A → B → C → A
    cycle_a = company_uks[26] if len(company_uks) > 26 else company_uks[-3]
    cycle_b = company_uks[27] if len(company_uks) > 27 else company_uks[-2]
    cycle_c = company_uks[28] if len(company_uks) > 28 else company_uks[-1]
    for period in periods:
        cyc_amount = rng.uniform(5_000_000, 15_000_000)
        tx_records.append(_make_tx_edge(cycle_a, cycle_b, period, cyc_amount, rng))
        tx_records.append(_make_tx_edge(cycle_b, cycle_c, period, cyc_amount * 0.98, rng))
        tx_records.append(_make_tx_edge(cycle_c, cycle_a, period, cyc_amount * 0.96, rng))

    # Random edges for density
    remaining = n_tx_edges - len(tx_records)
    for _ in range(max(0, remaining)):
        src = random.choice(company_uks + ip_uks)
        tgt = random.choice(company_uks + ip_uks)
        if src != tgt:
            period = random.choice(periods)
            amount = rng.uniform(10_000, 5_000_000)
            tx_records.append(_make_tx_edge(src, tgt, period, amount, rng))

    tx_df = pd.DataFrame(tx_records)

    # =================================================================
    # Generate Authority Edges
    # =================================================================
    auth_records = []
    # CEO/directors for top companies
    for i in range(min(n_authority_edges, len(company_uks))):
        company = company_uks[i]
        rep = random.choice(individual_uks)
        auth_records.append({
            'authority_uk': 5000 + i,
            'company_client_uk': company,
            'representative_client_uk': rep,
            'start_date': '2020-01-01',
            'end_date': '5999-12-31',
            'is_active': True,
        })

    # Shared representative (same person represents 2 companies — suspicious)
    shared_rep = individual_uks[0]
    auth_records.append({
        'authority_uk': 5100,
        'company_client_uk': company_uks[0],
        'representative_client_uk': shared_rep,
        'start_date': '2020-01-01',
        'end_date': '5999-12-31',
        'is_active': True,
    })
    auth_records.append({
        'authority_uk': 5101,
        'company_client_uk': company_uks[3],
        'representative_client_uk': shared_rep,
        'start_date': '2021-06-01',
        'end_date': '5999-12-31',
        'is_active': True,
    })

    auth_df = pd.DataFrame(auth_records)

    # =================================================================
    # Generate Salary Edges
    # =================================================================
    salary_records = []
    # Employees of top companies
    emp_idx = 0
    for comp_i in range(min(8, len(company_uks))):
        n_emp = rng.randint(3, 8)
        for _ in range(n_emp):
            if emp_idx >= len(individual_uks):
                break
            salary_records.append({
                'deal_uk': 6000 + emp_idx,
                'employer_client_uk': company_uks[comp_i],
                'employee_client_uk': individual_uks[emp_idx],
                'account_number': f'4081781060{emp_idx:010d}',
                'start_date': f'{rng.randint(2018, 2024)}-01-01',
                'end_date': '5999-12-31',
                'is_active': True,
            })
            emp_idx += 1

    # Shared employees (same person works at 2 companies)
    if len(individual_uks) > 5:
        for shared_emp in individual_uks[:3]:
            second_company = random.choice(company_uks[5:10])
            salary_records.append({
                'deal_uk': 7000 + len(salary_records),
                'employer_client_uk': second_company,
                'employee_client_uk': shared_emp,
                'account_number': f'4081781099{len(salary_records):010d}',
                'start_date': '2023-01-01',
                'end_date': '5999-12-31',
                'is_active': True,
            })

    # Shell companies: NO salary edges (important for detection)

    salary_df = pd.DataFrame(salary_records)

    # =================================================================
    # Save to Parquet
    # =================================================================
    paths = {
        'nodes': os.path.join(output_dir, 'nodes.parquet'),
        'transaction_edges': os.path.join(output_dir, 'transaction_edges.parquet'),
        'authority_edges': os.path.join(output_dir, 'authority_edges.parquet'),
        'salary_edges': os.path.join(output_dir, 'salary_edges.parquet'),
        'hop_distances': os.path.join(output_dir, 'hop_distances.parquet'),
    }

    nodes_df.to_parquet(paths['nodes'], index=False)
    tx_df.to_parquet(paths['transaction_edges'], index=False)
    auth_df.to_parquet(paths['authority_edges'], index=False)
    salary_df.to_parquet(paths['salary_edges'], index=False)
    hop_df.to_parquet(paths['hop_distances'], index=False)

    logger.info(
        f"Synthetic data generated: "
        f"{len(nodes_df)} nodes, {len(tx_df)} tx edges, "
        f"{len(auth_df)} auth edges, {len(salary_df)} salary edges"
    )
    print(f"\nSynthetic data saved to {output_dir}/")
    print(f"  Nodes: {len(nodes_df)} ({n_companies} companies, {n_individuals} individuals, {n_ips} IPs)")
    print(f"  Transaction edges: {len(tx_df)}")
    print(f"  Authority edges: {len(auth_df)}")
    print(f"  Salary edges: {len(salary_df)}")
    print(f"\nBuilt-in patterns:")
    print(f"  Seed company: client_uk={seed_uk} ('{company_names[0]}')")
    print(f"  Shell companies: client_uk={shell_1}, {shell_2}")
    print(f"  Circular payment: {cycle_a} → {cycle_b} → {cycle_c} → {cycle_a}")
    print(f"  Shared representative: individual {shared_rep} represents companies {company_uks[0]} and {company_uks[3]}")

    return paths


def _make_tx_edge(source, target, period, total_amount, rng):
    """Helper: create a transaction edge record."""
    tx_count = max(1, int(rng.exponential(5)))
    return {
        'source_client_uk': source,
        'target_client_uk': target,
        'period': period,
        'total_amount': round(total_amount, 2),
        'tx_count': tx_count,
        'avg_amount': round(total_amount / tx_count, 2),
        'std_amount': round(total_amount * rng.uniform(0.1, 0.5) / tx_count, 2),
        'max_amount': round(total_amount * rng.uniform(0.3, 0.8), 2),
        'min_amount': round(total_amount * rng.uniform(0.01, 0.1), 2),
        'first_tx_date': period[:4] + '-' + {'Q1': '01-15', 'Q2': '04-15', 'Q3': '07-15', 'Q4': '10-15'}[period[-2:]],
        'last_tx_date': period[:4] + '-' + {'Q1': '03-28', 'Q2': '06-28', 'Q3': '09-28', 'Q4': '12-28'}[period[-2:]],
    }
