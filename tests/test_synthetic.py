"""
Tests for the synthetic data generator module (src/synthetic.py).

Uses session-scoped fixtures from conftest.py.
"""

import os

import pandas as pd


def test_output_files_created(synthetic_data_dir):
    """Verify all 5 expected parquet files exist."""
    expected = [
        'nodes.parquet',
        'transaction_edges.parquet',
        'authority_edges.parquet',
        'salary_edges.parquet',
        'hop_distances.parquet',
    ]
    for fname in expected:
        assert os.path.isfile(os.path.join(synthetic_data_dir, fname)), \
            f"Missing: {fname}"


def test_node_counts(nodes_df):
    """85 total: 30 companies + 50 individuals + 5 IPs."""
    assert len(nodes_df) == 85

    counts = nodes_df['client_type_name'].value_counts()
    assert counts.get('Юридическое лицо', 0) == 30
    assert counts.get('Физическое лицо', 0) == 50
    assert counts.get('Индивидуальный предприниматель', 0) == 5


def test_tx_edges_columns(tx_df):
    """Transaction edges have all required columns."""
    expected = [
        'source_client_uk', 'target_client_uk', 'period',
        'total_amount', 'tx_count', 'avg_amount', 'std_amount',
        'max_amount', 'min_amount', 'first_tx_date', 'last_tx_date',
    ]
    for col in expected:
        assert col in tx_df.columns, f"Missing column: {col}"


def test_shell_no_salary(synthetic_data_dir):
    """Shell companies (1024, 1025) have no salary edges as employer."""
    sal = pd.read_parquet(os.path.join(synthetic_data_dir, 'salary_edges.parquet'))
    shell_uks = {1024, 1025}
    employers = set(sal['employer_client_uk'].unique())
    assert not (shell_uks & employers), \
        f"Shell companies should have no salary edges"


def test_cycle_companies_in_tx(tx_df):
    """Cycle companies (1026, 1027, 1028) appear in transaction edges."""
    cycle_uks = {1026, 1027, 1028}
    present = set(tx_df['source_client_uk']) | set(tx_df['target_client_uk'])
    for uk in cycle_uks:
        assert uk in present, f"Cycle company {uk} not in tx edges"


def test_hop_distances(synthetic_data_dir):
    """Seed company (1000) has hop_distance == 0."""
    hop = pd.read_parquet(os.path.join(synthetic_data_dir, 'hop_distances.parquet'))
    seed = hop[hop['client_uk'] == 1000]
    assert len(seed) == 1
    assert seed.iloc[0]['hop_distance'] == 0


def test_okved_region_columns_exist(nodes_df):
    """Every node has okved_code and region_code columns."""
    assert 'okved_code' in nodes_df.columns
    assert 'region_code' in nodes_df.columns
    assert nodes_df['okved_code'].notna().all()
    assert nodes_df['region_code'].notna().all()


def test_okved_companies_nonzero(nodes_df):
    """Companies and IPs have non-'00' OKVED codes; individuals have '00'."""
    companies = nodes_df[nodes_df['client_type_name'] == 'Юридическое лицо']
    individuals = nodes_df[nodes_df['client_type_name'] == 'Физическое лицо']
    ips = nodes_df[nodes_df['client_type_name'] == 'Индивидуальный предприниматель']

    assert (companies['okved_code'] != '00').all(), "Companies should have real OKVED codes"
    assert (ips['okved_code'] != '00').all(), "IPs should have real OKVED codes"
    assert (individuals['okved_code'] == '00').all(), "Individuals should have '00' OKVED"
    assert (individuals['region_code'] == '00').all(), "Individuals should have '00' region"
