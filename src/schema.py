"""
Маппинг колонок Hive-таблиц.

ВАЖНО: Имена колонок являются предположениями на основе DDL-скриншотов.
Запустите notebooks/00_verify_schema.ipynb для проверки и автокоррекции.

Каждая таблица представлена словарём {логическое_имя: фактическое_имя_колонки}.
Если фактическое имя отличается от предполагаемого — измените значение справа.
"""

# ==============================================================================
# client_sdim — Клиенты (182M строк, 65 полей)
# ==============================================================================

CLIENT = {
    'uk': 'uk',                             # surrogate key (double)
    'client_name': 'client_name',           # полное имя / наименование
    'first_name': 'first_name',             # имя (ФЛ)
    'middle_name': 'middle_name',           # отчество (ФЛ)
    'birth_date': 'birth_date',             # дата рождения / регистрации
    'resident_flag': 'resident_flag',       # резидент Y/N
    'liquidation_flag': 'liquidation_flag', # ликвидирован Y/N
    'end_date': 'end_date',                 # дата закрытия записи
    'clienttype_uk': 'clienttype_uk',       # FK → clienttype_ldim (double)
    'clientstatus_uk': 'clientstatus_uk',   # FK → clientstatus_ldim (double)
}

# ==============================================================================
# account_sdim — Счета (1.07B строк, 47 полей)
# ==============================================================================

ACCOUNT = {
    'uk': 'uk',                                     # surrogate key
    'client_uk': 'client_uk',                       # FK → client_sdim
    'client_pin': 'client_pin',                     # client PIN
    'client_taxpayer_uk': 'client_taxpayer_uk',     # FK → зарплатная компания
    'client_taxpayer_ccode': 'client_taxpayer_ccode',  # ИНН зарплатной компании
    'account_number': 'account_number',             # номер счёта
    'account_number13': 'account_number13',         # 13-значный номер
    'currency_uk': 'currency_uk',                   # FK → валюта
    'currency_iso_ncode': 'currency_iso_ncode',     # ISO-код валюты
    'start_date': 'start_date',                     # дата открытия
    'end_date': 'end_date',                         # дата закрытия
    'accountkind_uk': 'accountkind_uk',             # активный/пассивный
    'salesplace_uk': 'salesplace_uk',               # FK → подразделение
    'salesplace_ccode': 'salesplace_ccode',         # код подразделения
    'balance_flag': 'balance_flag',                 # балансовый/забалансовый
}

# ==============================================================================
# paymentcounteragent_stran — Транзакции контрагентов (4.98B строк, 30 полей)
# Партиционирована по date_part
# ==============================================================================

PAYMENT_COUNTERAGENT = {
    # ВНИМАНИЕ: Точные имена колонок должны быть проверены через DESCRIBE TABLE.
    # Ниже — предположения на основе конвенций наименования банковских DWH.
    'date_part': 'date_part',                       # партиция (дата)
    'payer_client_uk': 'payer_client_uk',           # FK → плательщик (ПРЕДПОЛОЖЕНИЕ)
    'receiver_client_uk': 'receiver_client_uk',     # FK → получатель (ПРЕДПОЛОЖЕНИЕ)
    'counteragent_client_uk': 'counteragent_client_uk',  # FK → контрагент (ПРЕДПОЛОЖЕНИЕ)
    'amount': 'amount',                             # сумма (ПРЕДПОЛОЖЕНИЕ)
    'currency_uk': 'currency_uk',                   # FK → валюта (ПРЕДПОЛОЖЕНИЕ)
    'document_id': 'document_id',                   # ID документа (ПРЕДПОЛОЖЕНИЕ)
    'client_uk': 'client_uk',                       # FK → клиент (ПРЕДПОЛОЖЕНИЕ)
}

# ==============================================================================
# clientauthority_shist — Доверенности (42.9M строк, 27 полей)
# ==============================================================================

CLIENT_AUTHORITY = {
    'uk': 'uk',                                     # surrogate key
    'client_uk': 'client_uk',                       # FK → компания
    'profile_create_date': 'profile_create_date',   # дата создания профиля
    'attorney_date': 'attorney_date',               # дата доверенности
    'start_date': 'start_date',                     # начало действия
    'end_date': 'end_date',                         # окончание действия
    'job_update': 'job_update',                     # дата обновления
}

# ==============================================================================
# clientauthority2clientrb_shist — Связи доверенностей с клиентами (35.2M строк, 12 полей)
# ==============================================================================

AUTHORITY_CLIENT_RB = {
    'authority_uk': 'authority_uk',                 # FK → clientauthority_shist (ПРЕДПОЛОЖЕНИЕ)
    'client_uk': 'client_uk',                       # FK → представитель (ПРЕДПОЛОЖЕНИЕ)
    'start_date': 'start_date',                     # начало
    'end_date': 'end_date',                         # окончание
}

# ==============================================================================
# clnt2dealsalary_shist — Зарплатные проекты (133M строк, 27 полей)
# ==============================================================================

SALARY_DEAL_LINK = {
    'account_number': 'account_number',             # номер зарплатного счёта (ПРЕДПОЛОЖЕНИЕ)
    'client_uk': 'client_uk',                       # FK → сотрудник (ПРЕДПОЛОЖЕНИЕ)
    'client_ccode': 'client_ccode',                 # код клиента (ПРЕДПОЛОЖЕНИЕ)
    'deal_uk': 'deal_uk',                           # FK → dealsalary_sdim (ПРЕДПОЛОЖЕНИЕ)
    'deal_ccode': 'deal_ccode',                     # код сделки (ПРЕДПОЛОЖЕНИЕ)
    'start_date': 'start_date',                     # начало
    'end_date': 'end_date',                         # окончание
}

# ==============================================================================
# dealsalary_sdim — Зарплатные сделки
# ==============================================================================

SALARY_DEAL = {
    'uk': 'uk',                                     # surrogate key
    'client_uk': 'client_uk',                       # FK → компания-работодатель (ПРЕДПОЛОЖЕНИЕ)
    'client_ccode': 'client_ccode',                 # код работодателя (ПРЕДПОЛОЖЕНИЕ)
    'start_date': 'start_date',                     # начало
    'end_date': 'end_date',                         # окончание
}

# ==============================================================================
# Справочники
# ==============================================================================

CLIENT_TYPE = {
    'uk': 'uk',                                     # surrogate key
    'name': 'name',                                 # наименование типа
}

CLIENT_STATUS = {
    'uk': 'uk',                                     # surrogate key
    'name': 'name',                                 # наименование статуса
}

# ==============================================================================
# Вспомогательные функции
# ==============================================================================

ALL_TABLES = {
    'client_sdim': CLIENT,
    'account_sdim': ACCOUNT,
    'paymentcounteragent_stran': PAYMENT_COUNTERAGENT,
    'clientauthority_shist': CLIENT_AUTHORITY,
    'clientauthority2clientrb_shist': AUTHORITY_CLIENT_RB,
    'clnt2dealsalary_shist': SALARY_DEAL_LINK,
    'dealsalary_sdim': SALARY_DEAL,
    'clienttype_ldim': CLIENT_TYPE,
    'clientstatus_ldim': CLIENT_STATUS,
}


def get_column(table_name: str, logical_name: str) -> str:
    """Get actual column name for a logical column reference."""
    table_schema = ALL_TABLES.get(table_name)
    if table_schema is None:
        raise KeyError(f"Unknown table: {table_name}")
    col = table_schema.get(logical_name)
    if col is None:
        raise KeyError(f"Unknown column '{logical_name}' in table '{table_name}'")
    return col


def describe_table_sql(table_name: str, database: str = 's_dmrb') -> str:
    """Generate DESCRIBE TABLE SQL for schema verification."""
    return f"DESCRIBE {database}.{table_name}"
