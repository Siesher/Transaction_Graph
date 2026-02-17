"""
Transaction Graph MVP — корпоративный граф связей из банковских транзакций.

Модули:
    config          — конфигурация и параметры
    schema          — маппинг колонок Hive-таблиц
    etl             — PySpark извлечение данных из Hive
    graph_builder   — построение NetworkX графа
    filters         — disparity filter и фильтрация шума
    analysis        — Leiden кластеризация, центральность, паттерны
    viz             — pyvis интерактивная визуализация
"""
