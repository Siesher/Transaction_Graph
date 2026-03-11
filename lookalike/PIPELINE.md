# Look-Alike Pipeline — Описание и технологический стек

> **Проект:** Transaction Graph — Look-Alike Scoring
> **Цель:** Автоматически ранжировать клиентскую базу банка по вероятности принадлежности
> к «эталонной» группе наиболее ценных привлеченных клиентов.

---

## Содержание

1. [Обзор пайплайна](#1-обзор-пайплайна)
2. [Технологический стек](#2-технологический-стек)
3. [Шаг 1 — ETL: профиль клиентов](#3-шаг-1--etl-профиль-клиентов-01_etl_profileipynb)
4. [Шаг 2 — ETL: транзакционное поведение](#4-шаг-2--etl-транзакционное-поведение-02_etl_behavioripynb)
5. [Шаг 3 — Feature Engineering](#5-шаг-3--feature-engineering-03_feature_engineeringipynb)
6. [Шаг 4 — Сегментация](#6-шаг-4--поведенческая-сегментация-04_segmentationipynb)
7. [Шаг 5 — Look-Alike Scoring](#7-шаг-5--look-alike-scoring-05_lookalikepynb)
8. [Метрики качества](#8-метрики-качества)
9. [Архитектурные решения](#9-архитектурные-решения-и-обоснование)

---

## 1. Обзор пайплайна

```
Hive (MDP)
    |
    v
01_etl_profile.ipynb          <- Статические атрибуты клиентов (тип, ОКВЭД, дата регистрации)
    |
    v
02_etl_behavior.ipynb         <- Транзакционная история: годовые агрегаты (Jan-Dec 2025)
    |                              Обороты, контрагенты, динамика, остатки, продукты
    v
03_feature_engineering.ipynb  <- Объединение + граф-метрики (PageRank, betweenness, clustering)
    |                            + 6 производных признаков + Feature Selection (corr + VIF)
    |                            -> feature_matrix.parquet
    v
04_segmentation.ipynb         <- K-Means (best k): поведенческие сегменты всей базы
    |                            UMAP визуализация, HDBSCAN / GMM сравнение
    |                            Bootstrap stability (ARI)
    v
05_lookalike.ipynb            <- Centroid / kNN / GBM / Stacking Ensemble
                                 Stratified K-Fold CV, калибровка, threshold optimization
                                 SHAP-интерпретация, ablation study, segment-aware validation
                                 Score bands (Hot/Warm/Medium/Cool/Cold)
                                 -> lookalike_scores.parquet, model_card.json
```

**Входные данные:** транзакционный граф банка Jan-Dec 2025 (полный год), выборка 1%
**Выходные данные:** `lookalike_scores.parquet` — скор 0-1 для каждого клиента

---

## 2. Технологический стек

| Категория | Библиотека | Версия | Назначение |
|-----------|-----------|--------|------------|
| **Data** | pandas | >=2.0 | Основная обработка табличных данных |
| **Data** | numpy | >=2.0 | Числовые операции, векторизация |
| **Data** | pyarrow | >=12.0 | Parquet I/O, эффективное хранение |
| **Graph** | networkx | >=3.0 | Построение графа транзакций, граф-метрики |
| **Graph** | python-igraph | >=0.10 | Быстрый расчёт метрик на больших графах |
| **Graph** | leidenalg | >=0.9 | Community detection (алгоритм Leiden) |
| **ETL** | pyspark | кластер MDP | Обработка полной базы на кластере Hive |
| **Clustering** | sklearn.cluster.KMeans | >=1.3 | Поведенческая сегментация |
| **Clustering** | sklearn.cluster.HDBSCAN | >=1.3 | Density-based кластеризация (сравнение) |
| **Clustering** | sklearn.mixture.GaussianMixture | >=1.3 | GMM кластеризация (сравнение) |
| **Dim.Red.** | umap-learn | >=0.5.6 | 2D-визуализация клиентов (UMAP) |
| **Scoring** | sklearn.neighbors | >=1.3 | kNN cosine scoring |
| **Scoring** | sklearn.ensemble | >=1.3 | GradientBoostingClassifier |
| **Scoring** | lightgbm | >=4.0 | Гистограммный GBM (primary) |
| **Scoring** | sklearn.linear_model | >=1.3 | LogisticRegression (stacking meta-learner) |
| **Validation** | sklearn.model_selection | >=1.3 | StratifiedKFold, cross-validation |
| **Validation** | sklearn.calibration | >=1.3 | CalibratedClassifierCV, calibration_curve |
| **Feature Sel.** | statsmodels | >=0.14 | VIF (Variance Inflation Factor) |
| **Interp.** | shap | >=0.44 | SHAP-интерпретация GBM модели |
| **Viz** | matplotlib | >=3.7 | Все графики (radar, lift, beeswarm) |
| **Dev** | ruff | >=0.15 | Линтер + форматтер |
| **Dev** | pytest | >=7.0 | Тесты |

---

## 3. Шаг 1 — ETL: профиль клиентов (`01_etl_profile.ipynb`)

### Что делает

Извлекает статические атрибуты клиентов из Hive-таблиц:
- Тип клиента (`clientcounterparty_type`) — юрлицо / ИП / физлицо
- ОКВЭД (`sparkokved_ccode`) — основной вид деятельности
- Дата регистрации, уставной капитал
- Флаг эталонного клиента (`clientcounterparty_flag`)

### Подход

- **Локально:** pandas + synthetic data (`src/synthetic.py`)
- **На кластере MDP:** PySpark -> Hive -> `.parquet`

### Период

**Полный год: Jan-Dec 2025.** Вселенная клиентов = все `client_uk`,
участвовавшие хотя бы в одной транзакции за год.

### Выход

`data/client_profiles.parquet` — статические атрибуты всех клиентов

---

## 4. Шаг 2 — ETL: транзакционное поведение (`02_etl_behavior.ipynb`)

### Что делает

Агрегирует транзакционную историю за полный год (Jan-Dec 2025) по клиентам:
- Суммарный оборот (`total_amount`, `total_inflow`, `total_outflow`)
- Количество транзакций (`tx_count`)
- Средний баланс (`avg_balance_30d`)
- Направление потоков (`direction_ratio` — доля расходов)
- Динамика: рост оборота (`amount_growth`), рост контрагентов (`cp_growth`)
- Половины года: Jan-Jun (first_half) vs Jul-Dec (second_half) для тренд-анализа

### Ключевые технические решения

**Полный год (12 месяцев):** годовые агрегаты дают более стабильные поведенческие паттерны,
чем квартальные. `active_months` может принимать значения 1-12.
**MID_DATE = 2025-07-01:** разделение на две половины для расчёта amount_growth и cp_growth.
**SAMPLE_PCT=1%:** пилотная выборка для проверки подхода без full-scan Hive.
**`balance` cast:** явное приведение типа к `DOUBLE` для совместимости Spark/pandas.

### Выход

`data/behavioral_features.parquet`

---

## 5. Шаг 3 — Feature Engineering (`03_feature_engineering.ipynb`)

### Что делает

Объединяет профиль + поведение и добавляет графовые и производные признаки:

```
client_profiles + behavioral_features
         |
         v
   Граф-метрики (из graph_metrics.parquet)
   |-- PageRank          <- системная важность в сети платежей
   |-- Betweenness       <- роль посредника / хаба
   |-- Clustering coef   <- плотность локального сообщества
   |-- Flow-through ratio <- транзитный оборот
   |-- In/Out degree     <- входящие/исходящие связи
   |-- OKVED diversity   <- отраслевая диверсификация
         |
         v
   Производные признаки (базовые)
   |-- tx_per_month, cp_per_month  <- нормировка на активные месяцы
   |-- payee_ratio, payer_ratio    <- структура платёжных связей
   |-- tx_amount_cv               <- коэффициент вариации сумм
   |-- amt_per_cp                 <- средний оборот на контрагента
         |
         v
   Производные признаки (расширенные)
   |-- herfindahl_index         <- концентрация на контрагентах
   |-- monthly_volatility       <- стабильность оборота
   |-- counterparty_retention   <- лояльность сети контрагентов
   |-- balance_turnover_ratio   <- эффективность cash management
   |-- growth_acceleration      <- ускорение роста
   |-- network_influence        <- комбинированное влияние в сети
         |
         v
   Feature Selection
   |-- Корреляция: drop |corr| > 0.95
   |-- VIF: drop VIF > 10 (мультиколлинеарность)
         |
         v
   feature_matrix.parquet (~70 признаков x N клиентов)
```

### Граф транзакций

**Библиотека:** `networkx` (локально) / `python-igraph` (production, быстрее).
**Вершины:** клиенты и контрагенты.
**Рёбра:** транзакции с весом = сумма.

**PageRank** показывает, насколько клиент является важным узлом в платёжной сети.
**Betweenness Centrality** — доля кратчайших путей, проходящих через данного клиента.

### Feature Selection

Двухэтапная фильтрация перед нормализацией:
1. **Корреляционный фильтр:** из пар с |corr| > 0.95 удаляется один признак
2. **VIF (Variance Inflation Factor):** признаки с VIF > 10 удаляются (мультиколлинеарность)

### Нормализация

`StandardScaler` на числовых признаках для корректной работы cosine-сходства в kNN и UMAP.

### Выход

- `data/feature_matrix.parquet` — нормализованная матрица признаков
- `data/full_client_data.parquet` — полный датафрейм с сырыми признаками

---

## 6. Шаг 4 — Поведенческая сегментация (`04_segmentation.ipynb`)

### Цель

Разделить клиентскую базу на однородные поведенческие группы перед скорингом.
Сегменты используются для: (1) понимания структуры базы, (2) segment-aware валидации модели,
(3) планирования кампаний по сегментам.

### Метод 1: K-Means (выбранный)

**Алгоритм:** `sklearn.cluster.KMeans`
**Параметры:** best_k по composite метрике, random_state=42, n_init=10

**Выбор k:** перебор k=3..8 по ансамблю трёх метрик:

| Метрика | Формула | Интерпретация |
|---------|---------|---------------|
| **Silhouette** | `(b - a) / max(a, b)` | Разделение кластеров [-1, 1]. Выше = лучше. |
| **Davies-Bouldin** | `mean(max_j[(s_i + s_j)/d_ij])` | Компактность vs разделение. Ниже = лучше. |
| **Calinski-Harabasz** | `(SS_B / SS_W) * (n-k)/(k-1)` | Межкластерный vs внутрикластерный разброс. Выше = лучше. |

### Метод 2: HDBSCAN (сравнение)

**Алгоритм:** `sklearn.cluster.HDBSCAN` (sklearn >= 1.3, numpy 2.x совместим)
HDBSCAN — density-based кластеризация, не требует задавать k.
На данной выборке значительная доля выбросов (label = -1).

### Метод 3: GMM (сравнение)

**Алгоритм:** `sklearn.mixture.GaussianMixture`
GMM моделирует каждый кластер как многомерное нормальное распределение.
Мягкое разбиение (вероятность принадлежности), BIC/AIC для выбора числа компонент.

### Сравнение методов

Комбинированная таблица KMeans vs HDBSCAN vs GMM по Silhouette, CH, DB.
Визуализация распределений кластеров по трём методам.

### Стабильность сегментов (Bootstrap)

50 bootstrap-итераций (80% выборки), Adjusted Rand Index (ARI) между прогонами.
ARI >= 0.70 = стабильная сегментация для production.

### Выход

`data/segments.parquet` — `behavioral_segment` + `segment_label` для каждого клиента

---

## 7. Шаг 5 — Look-Alike Scoring (`05_lookalike.ipynb`)

### Цель

Присвоить каждому клиенту скор 0-1, отражающий вероятность принадлежности
к эталонной группе. Несколько методов + stacking/fixed-weight ансамбль.

### 7.1 Определение эталонной группы (многофакторный отбор)

Эталон определяется через **composite quality score** — взвешенная сумма Z-score
по 6 измерениям:

| Измерение | Вес | Что оценивает |
|-----------|-----|---------------|
| tx_count | 25% | Транзакционная активность |
| unique_counterparties | 20% | Ширина сети |
| active_months | 15% | Регулярность присутствия |
| amount_growth | 15% | Динамика роста |
| total_amount | 15% | Объём бизнеса |
| direction_balance | 10% | Сбалансированность потоков |

Топ 20% привлечённых клиентов по composite score -> эталонная группа.

### 7.2 Метод A: Centroid Baseline

Скор = cosine similarity клиента с центроидом эталонной группы.
Нижняя планка качества.

### 7.3 Метод B: kNN Cosine

Среднее cosine similarity к k=50 ближайшим эталонным соседям.
Учитывает форму облака эталонов, устойчив к выбросам.

### 7.4 Метод C: GBM (LightGBM / XGBoost / sklearn)

Бинарный классификатор: эталон vs остальные.
**Whitelist:** только поведенческие + граф-метрики + производные признаки.
Monetary absolutes исключены (защита от target leakage).

### 7.5 Ансамблевая модель

**Stacking (рекомендуется):** LogisticRegression meta-learner на OOF-предсказаниях
трёх базовых моделей (GBM, kNN, Centroid).

**Fixed-weight (fallback):** `0.65 * GBM + 0.35 * kNN`

Выбор автоматический: используется метод с лучшим Separation Index.

### 7.6 Валидация

#### Stratified K-Fold CV (5 fold)

Метрики по каждому fold: AUC-ROC, PR-AUC, Separation Index, Lift@5%.
Mean +/- std для оценки устойчивости.

#### Калибровка модели

- Reliability diagram (calibration curve)
- Brier Score, Expected Calibration Error (ECE)
- CalibratedClassifierCV (isotonic) если ECE > 0.05

#### Threshold Optimization

- ROC curve + Youden's J statistic -> optimal cutoff
- Precision-Recall curve -> F1-optimal threshold
- Сравнение: default 0.5 vs Youden vs F1-optimal vs business threshold

#### Segment-Aware Validation

Lift@K% и AUC-ROC по каждому behavioral_segment.
Выявление слабых сегментов, где модель не работает.

### 7.7 Ablation Study

Удаление групп признаков (граф, категориальные, временные, структура сети)
и оценка влияния на AUC-ROC. Выявление критичных групп.

### 7.8 SHAP-интерпретация

SHAP (SHapley Additive exPlanations) — маргинальный вклад каждого признака.
Beeswarm + bar chart + текстовая интерпретация топ-5 признаков.

### 7.9 Score Bands

| Band | Score Range | Рекомендация |
|------|-------------|-------------|
| Hot | 0.8 - 1.0 | Персональный менеджер |
| Warm | 0.6 - 0.8 | Таргетированные кампании |
| Medium | 0.4 - 0.6 | Массовые кампании |
| Cool | 0.2 - 0.4 | Мониторинг, nurturing |
| Cold | 0.0 - 0.2 | Не тратить ресурсы |

### Выход

| Файл | Описание |
|------|---------|
| `lookalike_scores.parquet` | Все клиенты со скорами (centroid, kNN, GBM, stacking, ensemble) |
| `top_prospects.parquet/.xlsx` | Топ-500 проспектов с score_band |
| `lookalike_gbm.pkl` | Обученная GBM модель |
| `model_card.json` | Метаданные: AUC, Brier, ECE, period, n_features, score bands |

---

## 8. Метрики качества

### Separation Index

```
SI = mean(score | client in reference) - mean(score | client not in reference)
```

Не зависит от порога. SI = 0 -> модель не работает. SI = 1 -> идеальное разделение.

### AUC-ROC / PR-AUC

- **AUC-ROC:** площадь под ROC-кривой. 0.5 = random, 1.0 = perfect.
- **PR-AUC:** площадь под Precision-Recall. Полезна при дисбалансе классов.
- Оценивается через Stratified 5-Fold CV (mean +/- std).

### Brier Score / ECE

- **Brier Score:** среднеквадратичная ошибка вероятностей. < 0.1 = хорошо.
- **ECE (Expected Calibration Error):** разница между предсказанной и фактической P. < 0.05 = хорошо.

### Lift

```
Lift@K% = (доля эталонов в топ-K%) / (общая доля эталонов в базе)
```

### Bootstrap Stability (ARI)

Adjusted Rand Index между bootstrap-прогонами кластеризации. >= 0.70 = стабильно.

---

## 9. Архитектурные решения и обоснование

### Почему полный год, а не квартал?

Годовые агрегаты дают более стабильные поведенческие паттерны:
- `active_months` имеет диапазон 1-12 (vs 1-3 для квартала)
- Сезонные эффекты усредняются
- `amount_growth` и `cp_growth` (первая vs вторая половина года) более репрезентативны
- Больше данных для обучения модели

### Почему K-Means, а не HDBSCAN / GMM?

K-Means: O(n * k * iter) — масштабируется линейно, детерминированный.
HDBSCAN: значительная доля выбросов на пилоте.
GMM: мягкое разбиение (может быть полезно), но BIC/AIC не всегда дают лучший k.
**Решение:** K-Means для production, HDBSCAN/GMM для сравнения и валидации.

### Почему Stacking, а не фиксированные веса?

Stacking (LogisticRegression meta-learner) автоматически подбирает оптимальные веса
на OOF-предсказаниях. Учитывает корреляции между базовыми моделями.
Fallback на fixed-weight (0.65 GBM + 0.35 kNN) если stacking хуже.

### Почему Feature Selection (corr + VIF)?

Мультиколлинеарные признаки:
- Раздувают VIF, делая коэффициенты модели нестабильными
- Не добавляют информации, но увеличивают noise
- Correlation > 0.95 + VIF > 10 — стандартный pipeline

### Защита от Target Leakage

Whitelist поведенческих признаков исключает monetary absolutes
(total_amount, avg_balance и т.д.), которые кодируют целевую переменную.

### Совместимость numpy >= 2.0

1. `standalone hdbscan` -> `sklearn.cluster.HDBSCAN`
2. `np.corrcoef` -> прямая формула Пирсона через dot-product

---

*Последнее обновление: 11.03.2026*
