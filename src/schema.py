"""
Маппинг колонок Hive-таблиц.

Верифицировано через notebooks/00_verify_schema.ipynb на MDP (2026-02-17).
Каждая таблица представлена словарём {логическое_имя: фактическое_имя_колонки}.
"""

# ==============================================================================
# client_sdim — Клиенты (182M строк, 65 полей)
# Колонки в lowercase
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
    # clientstatus_uk НЕ существует — статус определяется по флагам:
    'closed_flag': 'closed_flag',           # закрыт
    'dead_flag': 'dead_flag',               # умер (ФЛ)
    'deleted_flag': 'deleted_flag',         # удалён
    'default_flag': 'default_flag',         # дефолт
    'client_pin': 'client_pin',             # ПИН клиента
    'country_ccode': 'country_ccode',       # код страны
}

# ==============================================================================
# account_sdim — Счета (1.07B строк, 47 полей)
# ==============================================================================

ACCOUNT = {
    'uk': 'uk',
    'client_uk': 'client_uk',
    'client_pin': 'client_pin',
    'client_taxpayer_uk': 'client_taxpayer_uk',
    'client_taxpayer_ccode': 'client_taxpayer_ccode',  # ИНН
    'account_number': 'account_number',
    'account_number13': 'account_number13',
    'currency_uk': 'currency_uk',
    'currency_iso_ncode': 'currency_iso_ncode',
    'start_date': 'start_date',
    'end_date': 'end_date',
    'accountkind_uk': 'accountkind_uk',
    'salesplace_uk': 'salesplace_uk',
    'salesplace_ccode': 'salesplace_ccode',
    'balance_flag': 'balance_flag',
}

# ==============================================================================
# paymentcounteragent_stran — Транзакции контрагентов (4.98B строк, 26 полей)
# Партиционирована по date_part
# ВЕРИФИЦИРОВАНО: реальные имена колонок
# ==============================================================================

PAYMENT_COUNTERAGENT = {
    # ВАЖНО: date_part имеет тип INT в формате YYYYMMDD (не DATE!)
    # Фильтры: date_part >= 20250101  (не строка '2025-01-01')
    # YEAR/QUARTER: TO_DATE(CAST(date_part AS STRING), 'yyyyMMdd')
    'date_part': 'date_part',                           # партиция INT (YYYYMMDD)
    'client_uk': 'client_uk',                           # FK → клиент банка
    'client_pin': 'client_pin',                         # ПИН клиента
    'client_contr_uk': 'client_contr_uk',               # FK → контрагент (клиент банка)
    'client_contr_pin': 'client_contr_pin',             # ПИН контрагента
    'client_contr_taxpayer_ccode': 'client_contr_taxpayer_ccode',  # ИНН контрагента
    'account_client_uk': 'account_client_uk',           # FK → счёт клиента
    'account_client_number': 'account_client_number',   # номер счёта клиента
    'account_contr_uk': 'account_contr_uk',             # FK → счёт контрагента
    'account_contr_number': 'account_contr_number',     # номер счёта контрагента
    'rur_amt': 'rur_amt',                               # сумма в рублях
    'cur_amt': 'cur_amt',                               # сумма в валюте
    'currency_uk': 'currency_uk',                       # FK → валюта
    'income_flag': 'income_flag',                       # Y=входящий, N=исходящий
    'value_day': 'value_day',                           # дата валютирования
    'trn_source_no': 'trn_source_no',                   # номер транзакции (документ)
    'cashflowcategory_dk': 'cashflowcategory_dk',       # категория денежного потока
    'trn_description': 'trn_description',               # описание транзакции
    'bank_contr_bik_ncode': 'bank_contr_bik_ncode',     # БИК банка контрагента
    'bank_contr_swift_ccode': 'bank_contr_swift_ccode', # SWIFT контрагента
    'taxpayer_client_contr_uk': 'taxpayer_client_contr_uk',
    'deleted_flag': 'deleted_flag',
    'as_of_day': 'as_of_day',
    'job_insert': 'job_insert',
    'job_update': 'job_update',
}

# ==============================================================================
# clientauthority_shist — Доверенности (42.9M строк, 27 полей)
# ВЕРИФИЦИРОВАНО: uk → client_authority_uk, даты → effective_from/to
# ==============================================================================

CLIENT_AUTHORITY = {
    'uk': 'client_authority_uk',             # surrogate key (НЕ 'uk'!)
    'client_uk': 'client_uk',               # FK → компания
    'client_pin': 'client_pin',             # ПИН клиента
    'client_authority_pin': 'client_authority_pin',
    'c2clinkrole_uk': 'c2clinkrole_uk',     # FK → c2clinkrole_sdim (роль связи)
    'clientauthoritytype_uk': 'clientauthoritytype_uk',  # тип доверенности
    'profile_create_date': 'profile_create_date',
    'attorney_date': 'attorney_date',
    'attorney_start_date': 'attorney_start_date',
    'attorney_end_date': 'attorney_end_date',
    'start_date': 'effective_from',          # начало действия (НЕ 'start_date'!)
    'end_date': 'effective_to',              # окончание действия (НЕ 'end_date'!)
    'active_flag': 'active_flag',
    'authority_flag': 'authority_flag',
    'attorney_flag': 'attorney_flag',
    'confirm_flag': 'confirm_flag',
    'deleted_flag': 'deleted_flag',
    'job_update': 'job_update',
}

# ==============================================================================
# clientauthority2clientrb_shist — Связи клиентов (35.2M строк, 12 полей)
# ВЕРИФИЦИРОВАНО: нет authority_uk, связь через client_u_uk ↔ client_x_uk
# ==============================================================================

AUTHORITY_CLIENT_RB = {
    'client_uk': 'client_uk',                # FK → клиент (ключ для JOIN)
    'client_u_uk': 'client_u_uk',            # одна сторона связи
    'client_x_uk': 'client_x_uk',            # другая сторона связи
    'c2clinkrole_uk': 'c2clinkrole_uk',      # FK → c2clinkrole_sdim (роль)
    'clientauthoritytype_uk': 'clientauthoritytype_uk',  # тип доверенности
    'authority_flag': 'authority_flag',
    'start_date': 'effective_from',          # начало действия
    'end_date': 'effective_to',              # окончание действия
    'deleted_flag': 'deleted_flag',
    'as_of_day': 'as_of_day',
    'job_insert': 'job_insert',
    'job_update': 'job_update',
}

# ==============================================================================
# clnt2dealsalary_shist — Зарплатные проекты (133M строк, 27 полей)
# ВЕРИФИЦИРОВАНО: deal_uk → dealsalary_uk, account_number → account_main_number
# ==============================================================================

SALARY_DEAL_LINK = {
    'client_uk': 'client_uk',                   # FK → сотрудник
    'client_pin': 'client_pin',                 # ПИН сотрудника
    'client_agent_uk': 'client_agent_uk',       # FK → агент
    'client_agent_pin': 'client_agent_pin',
    'dealsalary_uk': 'dealsalary_uk',           # FK → dealsalary_sdim (НЕ 'deal_uk'!)
    'deal_ref': 'deal_ref',                     # референс сделки
    'account_main_number': 'account_main_number', # номер счёта (НЕ 'account_number'!)
    'account_main_uk': 'account_main_uk',       # FK → account_sdim
    'salary_rur_amt': 'salary_rur_amt',         # сумма зарплаты в RUB
    'mainjob_flag': 'mainjob_flag',             # основное место работы
    'slrclientstatus_uk': 'slrclientstatus_uk', # статус в зарплатном проекте
    'slrclientstatus_ccode': 'slrclientstatus_ccode',
    'start_date': 'start_date',
    'end_date': 'end_date',
    'deleted_flag': 'deleted_flag',
    'effective_from': 'effective_from',
    'effective_to': 'effective_to',
}

# ==============================================================================
# dealsalary_sdim — Зарплатные сделки (23 поля)
# ВЕРИФИЦИРОВАНО: колонки в UPPERCASE (Hive case-insensitive, запросы работают)
# ==============================================================================

SALARY_DEAL = {
    'uk': 'UK',                                  # surrogate key
    'client_uk': 'CLIENT_UK',                    # FK → компания-работодатель
    'client_pin': 'CLIENT_PIN',
    'client_salesagent_uk': 'CLIENT_SALESAGENT_UK',
    'deal_ref': 'DEAL_REF',
    'dealnum_ccode': 'DEALNUM_CCODE',
    'dealstatus_uk': 'DEALSTATUS_UK',
    'start_date': 'START_DATE',
    'end_date': 'END_DATE',
    'deleted_flag': 'DELETED_FLAG',
    'default_flag': 'DEFAULT_FLAG',
    'channel_uk': 'CHANNEL_UK',
    'channel_ccode': 'CHANNEL_CCODE',
    'module_uk': 'MODULE_UK',
}

# ==============================================================================
# Справочники (колонки в UPPERCASE)
# ==============================================================================

CLIENT_TYPE = {
    'uk': 'UK',
    'name': 'NAME',
    'ccode': 'CCODE',
    'clientgroup_uk': 'CLIENTGROUP_UK',
}

CLIENT_STATUS = {
    'uk': 'uk',       # lowercase (в отличие от clienttype_ldim который UPPERCASE)
    'name': 'name',
    'ncode': 'ncode',
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
