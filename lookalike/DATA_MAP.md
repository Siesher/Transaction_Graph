# Data Map: Look-Alike & Behavioral Segmentation

> Документ описывает все источники данных, необходимые для проекта, их статус и покрытие.
> Последнее обновление: 2026-03-06 (pilot run, Dec 2025, 10% sample)

---

## Сводка по данным (pilot)

| Источник | Статус | Строк (pilot 10%) | Покрытие от universe |
|----------|--------|--------------------|----------------------|
| Universe (paymentcounteragent_stran) | Получен | 1,443,852 | 100% |
| Базовые атрибуты (client_sdim) | Получен | ~1,443,852 | ~100% |
| Клиент/контрагент (clientinfo_shist) | Получен | 1,081,415 | 74.9% |
| ОКВЭД, город (dmoutdmrb_mdm_redclients_shist_dmout) | Получен | 1,269,125 | 87.9% |
| Регион (client2smartsplace_shist) | Получен | 128,095 | 8.9% |
| Модельный сегмент (dmrb_clientgeneralsegment_shist) | НЕ получен | 0 | 0% |
| Выручка СПАРК (sparkfinindicator_sstat) | НЕ получен | 0 | 0% |
| Транзакции — поведение (paymentcounteragent_stran) | В процессе | — | ~100% |
| Остатки (clientbalance_sagg) | В процессе | — | — |
| Продукты (bmbpenetrproduct_stat) | В процессе | — | — |

---

## 1. Universe — вселенная клиентов

- **Источник**: `s_dmrb.paymentcounteragent_stran`
- **Поле**: `client_uk` (DISTINCT)
- **Фильтры**: `date_part` за декабрь 2025, `deleted_flag != 'Y'`, `client_uk IS NOT NULL`, hash-сэмпл 10%
- **Статус**: ПОЛУЧЕН
- **Результат**: **1,443,852** уникальных client_uk
- **Файл**: `data/universe.parquet`

---

## 2. Базовые атрибуты клиента

- **Источник**: `s_dmrb.client_sdim` + `s_dmrb.clienttype_ldim`
- **JOIN**: `universe.client_uk = client_sdim.uk`
- **Статус**: ПОЛУЧЕН
- **Поля**:

| Поле | Описание | Получен |
|------|----------|---------|
| client_uk | Ключ клиента | Да |
| client_name | Название/ФИО | Да |
| entrepreneur_flag | Признак ИП | Да |
| blacklist_flag | Чёрный список | Да |
| client_type_name | Тип (ЮЛ/ФЛ/ИП) из clienttype_ldim | Да |

---

## 3. Статус клиент/контрагент

- **Источник**: `s_dmrb.clientinfo_shist` (SCD-таблица)
- **JOIN**: `clientinfo_shist.client_uk = universe.client_uk`
- **SCD-фильтр**: `effective_to >= '2025-12-31'`, ROW_NUMBER по effective_from DESC
- **Статус**: ПОЛУЧЕН — **1,081,415 строк** (74.9% universe)
- **Распределение**:

| clientcounterparty_flag | Кол-во | Описание |
|-------------------------|--------|----------|
| Y | 1,035,325 | Клиенты банка |
| N | 44,972 | Контрагенты (проспекты) |
| ? | 1,118 | Неизвестно |

- **Поля**:

| Поле | Описание | Получен |
|------|----------|---------|
| clientcounterparty_flag | Y/N/? — клиент или контрагент | Да |
| clientchange_date | Дата перехода из контрагента в клиенты | Да |
| counterpartychange_date | Дата перехода из клиента в контрагенты | Да |
| client_rel_date | Дата начала отношений | Да |
| registry_authcapital_amt | Уставный капитал (зарегистрированный) | Да |
| paid_authcapital_amt | Уставный капитал (оплаченный) | Да |
| loanapplscope_uk | Сфера кредитования | Да |

---

## 4. ОКВЭД и город регистрации

- **Источник**: `s_dmrb.dmoutdmrb_mdm_redclients_shist_dmout` (SCD-таблица)
- **JOIN**: `client_lp_uk = universe.client_uk` (только ЮЛ)
- **SCD-фильтр**: `effective_to >= '2025-12-31'`, ROW_NUMBER по effective_from DESC
- **Статус**: ПОЛУЧЕН — **1,269,125 строк** (87.9% universe)
- **Поля**:

| Поле | Описание | Получен |
|------|----------|---------|
| sparkokved_ccode | Код ОКВЭД из СПАРК | Да |
| reg_city_name | Город регистрации | Да |
| statusfu_ccode | Статус ФУ | Да |

**Примечание**: Только для ЮЛ (`client_lp_uk`). ФЛ и ИП без ОКВЭД — будет NULL.

---

## 5. Регион (адрес)

- **Источник**: `s_dmrb.client2smartsplace_shist` (SCD-таблица)
- **JOIN**: `client2smartsplace_shist.client_uk = universe.client_uk`
- **SCD-фильтр**: `effective_to >= '2025-12-31'`, ROW_NUMBER по effective_from DESC
- **Статус**: ПОЛУЧЕН — **128,095 строк** (8.9% universe)
- **Поля**:

| Поле | Описание | Получен |
|------|----------|---------|
| addrref_region_uk | ID региона | Да |
| addrref_city_name | Город | Да |

**Примечание**: Низкое покрытие (8.9%) — таблица адресов заполнена не для всех клиентов. Город регистрации из п.4 (`reg_city_name`) имеет лучшее покрытие.

---

## 6. Модельный сегмент

- **Источник**: `s_dmrb.dmrb_clientgeneralsegment_shist`
- **Статус**: НЕ ПОЛУЧЕН
- **Причина**: Ключи `client_uk` (DOUBLE в научной нотации) и `client_pin` в этой таблице не совпадают ни с `universe.client_uk`, ни с `client_sdim.client_pin`. Таблица использует собственное пространство идентификаторов.
- **Таблица содержит**: 141,724,857 строк, поля `srvpackagesegment_ccode` (Base, Alfa Smart, ...), `agesegment_ccode`, `client_active_flag`
- **Что нужно**: Найти таблицу-маппинг между ID-пространствами, либо получить доступ к DWH-документации

**Желаемые поля**:

| Поле | Описание | Статус |
|------|----------|--------|
| srvpackagesegment_ccode | Модельный сегмент (Base, Alfa Smart, ...) | Не получен |
| srvpackagelightsegment_ccode | Лёгкий сегмент | Не получен |
| agesegment_ccode | Возрастной сегмент | Не получен |
| client_active_flag | Флаг активности | Не получен |

---

## 7. Выручка из СПАРК

- **Источник**: `s_dmrb.sparkfinindicator_sstat` через `dmoutdmrb_mdm_redclients_shist_dmout.sparkcompany_src_ccode`
- **Статус**: НЕ ПОЛУЧЕН
- **Причина**: Нет прямого FK. `sparkcompany_src_ccode` (STRING) → `sparkcompany_uk` (DOUBLE) через CAST не даёт совпадений. 0 rows matched.
- **Что нужно**: Правильный маппинг между `redclients` и `sparkfinindicator`, либо промежуточная таблица-справочник

**Желаемые поля**:

| Поле | Описание | Статус |
|------|----------|--------|
| revenue_rur_amt | Годовая выручка (RUR) | Не получен |
| adjrevenue_rur_amt | Скорректированная выручка | Не получен |
| failurescore_ccode | Скоринг надёжности | Не получен |
| paymentindex_ccode | Платёжный индекс | Не получен |

---

## 8. Транзакционные агрегаты (поведение)

- **Источник**: `s_dmrb.paymentcounteragent_stran`
- **Ноутбук**: `02_etl_behavior.ipynb`
- **Статус**: В ПРОЦЕССЕ (pilot запускается)
- **Агрегация по**: `client_uk`, GROUP BY

**Поля (вычисляемые)**:

| Поле | Описание |
|------|----------|
| tx_count | Количество транзакций |
| total_amount | Суммарный оборот (RUR) |
| total_outflow | Исходящие платежи |
| total_inflow | Входящие платежи |
| avg_tx_amount | Средняя сумма транзакции |
| std_tx_amount | Стандартное отклонение суммы |
| unique_counterparties | Уникальные контрагенты |
| unique_payees | Уникальные получатели |
| unique_payers | Уникальные плательщики |
| active_months | Активные месяцы |
| amount_first_half / second_half | Обороты 1-й/2-й половины периода |
| cp_first_half / cp_second_half | Контрагенты 1-й/2-й половины |

---

## 9. Остатки на счетах

- **Источник**: `s_dmrb.clientbalance_sagg`
- **Ноутбук**: `02_etl_behavior.ipynb`
- **Статус**: В ПРОЦЕССЕ

**Желаемые поля**:

| Поле | Описание |
|------|----------|
| avg_balance | Средний остаток за период |
| max_balance | Максимальный остаток |
| min_balance | Минимальный остаток |
| avg_balance_30d | Средний 30-дневный остаток |

---

## 10. Продуктовое проникновение

- **Источник**: `s_dmrb.bmbpenetrproduct_stat`
- **Ноутбук**: `02_etl_behavior.ipynb`
- **Статус**: В ПРОЦЕССЕ

**Желаемые поля**:

| Поле | Описание |
|------|----------|
| product_count | Количество продуктов |
| product_type_count | Количество типов продуктов |
| product_total_amt | Суммарный объём по продуктам |

---

## Производные признаки (03_feature_engineering)

Вычисляются из полученных данных в `03_feature_engineering.ipynb`:

| Признак | Формула | Зависит от |
|---------|---------|------------|
| direction_ratio | total_outflow / total_amount | Транзакции |
| avg_monthly_amount | total_amount / active_months | Транзакции |
| amount_growth | (2-я половина - 1-я) / 1-я | Транзакции |
| cp_growth | Рост контрагентов аналогично | Транзакции |
| tx_amount_cv | std / avg суммы транзакции | Транзакции |
| okved_* (dummy) | One-hot top-20 ОКВЭД | ОКВЭД |
| region_* (dummy) | One-hot top-20 регионов | Регион |
| ctype_* (dummy) | One-hot тип клиента | Базовые атрибуты |

---

## Блокеры и рекомендации

### Критичные (необходимо для полной версии):
1. **Модельный сегмент** — нужна таблица маппинга ID `dmrb_clientgeneralsegment_shist` → `client_sdim` (или документация DWH)
2. **Выручка СПАРК** — нужен правильный FK между `redclients.sparkcompany_src_ccode` и `sparkfinindicator.sparkcompany_uk`

### Рекомендации:
3. **Регион** — покрытие 8.9%. Использовать `reg_city_name` из ОКВЭД-таблицы как альтернативу (87.9% покрытие)
4. **Ресурсы** — для полного запуска (100% клиентов, 6 месяцев) нужны executor'ы с >4GB heap или доступ к YARN-кластеру с достаточными ресурсами
5. **Период** — pilot на 1 месяц + 10% sample достаточен для демонстрации. Для прода: `SAMPLE_PCT = 100`, период 3-6 месяцев
