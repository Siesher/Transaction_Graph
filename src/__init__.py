"""
Transaction Graph — корпоративный граф связей из банковских транзакций.

Модули:
    config          — конфигурация и параметры (edge_score weights, hub caps, OKVED defaults, behavioral K-range)
    schema          — маппинг колонок Hive-таблиц (включая okved_code, region_code)
    etl             — PySpark извлечение данных из Hive (с OKVED/region extraction)
    graph_builder   — построение NetworkX графа, метрики рёбер, расширенные метрики узлов
                      (compute_extended_metrics, compute_edge_score, derive_shared_employees)
    filters         — disparity filter, предфильтрация, hub-aware фильтрация (hub_filter)
    analysis        — Leiden кластеризация, центральность, роли, shell-детекция, циклы,
                      OKVED-матрица (build_okved_matrix), OKVED-разнообразие (compute_okved_diversity),
                      поведенческая сегментация (compute_behavioral_features, cluster_behavioral_segments),
                      look-alike scoring (compute_lookalike_scores)
    viz             — pyvis интерактивная визуализация
    pipeline        — one-command analysis pipeline orchestrator (включая Wave 1 + Wave 2 аналитику)
    synthetic       — synthetic data generator for testing (с OKVED/region кодами)
"""
