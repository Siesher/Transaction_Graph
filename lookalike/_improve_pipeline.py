"""Improve reference selection (multi-factor) and segment definitions."""

import json
import sys

sys.stdout.reconfigure(encoding="utf-8")

# ═══════════════════════════════════════════════════════════════════════════════
# 1. Improve 05_lookalike.ipynb — multi-factor reference selection
# ═══════════════════════════════════════════════════════════════════════════════
with open("lookalike/05_lookalike.ipynb", encoding="utf-8") as f:
    nb = json.load(f)

# ── Cell 0: Update markdown overview ──────────────────────────────────────────
nb["cells"][0]["source"] = [
    "# 05. Look-Alike Scoring\n",
    "\n",
    "> **Краткое резюме**: Определяем эталонную группу по многофакторному\n",
    "> composite-скору (активность + сеть + рост + стабильность + объём),\n",
    "> обучаем GBM-классификатор «эталон vs остальные», дополняем kNN cosine\n",
    "> и объединяем в ансамбль. Результат — ранжированный список проспектов.\n",
    "\n",
    "**Алгоритм**:\n",
    "1. **Эталон** (многофакторный): `clientcounterparty_flag = 'Y'`\n",
    "   + `clientchange_date IS NOT NULL` + **composite quality score** (топ 20%)\n",
    "2. **kNN cosine** (35% веса): средняя похожесть к K ближайшим эталонным соседям\n",
    "3. **GBM classifier** (65% веса): P(принадлежность к эталону)\n",
    "4. **Ensemble**: взвешенное среднее → `lookalike_score ∈ [0, 1]`\n",
    "5. **Ранжирование**: проспекты (`flag = 'N'`) отсортированы по score\n",
    "\n",
    "**Composite quality score** объединяет 6 измерений:\n",
    "- Транзакционная активность (tx_count)\n",
    "- Ширина сети (unique_counterparties)\n",
    "- Регулярность (active_months)\n",
    "- Динамика роста (amount_growth)\n",
    "- Объём оборота (total_amount)\n",
    "- Сбалансированность потоков (direction_ratio → |0.5 - x|)\n",
    "\n",
    "**Предпосылки**: `03_feature_engineering` и `04_segmentation` выполнены.\n",
    "\n",
    "---",
]

# ── Cell 2: Add multi-factor params ──────────────────────────────────────────
nb["cells"][2]["source"] = [
    "# =====================================================\n",
    "# ПАРАМЕТРЫ\n",
    "# =====================================================\n",
    "\n",
    "# Доля лучших клиентов для эталона (топ N% по composite quality score)\n",
    "TOP_PERCENTILE = 0.20  # 20%\n",
    "\n",
    "# Многофакторный composite quality score:\n",
    "# Z-score каждой метрики × вес → взвешенная сумма → топ N%\n",
    "# Все метрики: «больше = лучше» (direction_ratio преобразуется в balance)\n",
    "QUALITY_WEIGHTS = {\n",
    '    "tx_count": 0.25,               # Транзакционная активность\n',
    '    "unique_counterparties": 0.20,   # Ширина сети контрагентов\n',
    '    "active_months": 0.15,           # Регулярность присутствия\n',
    '    "amount_growth": 0.15,           # Динамика роста оборота\n',
    '    "total_amount": 0.15,            # Объём оборота\n',
    '    "direction_balance": 0.10,       # Сбалансированность (|0.5-DR| → инверсия)\n',
    "}\n",
    "\n",
    "# Количество ближайших соседей для kNN-скоринга\n",
    "K_NEIGHBORS = 50\n",
    "\n",
    "# Веса в ансамбле: GBM (основной) + kNN cosine (вспомогательный)\n",
    "WEIGHT_GBM = 0.65\n",
    "WEIGHT_KNN = 0.35\n",
    "\n",
    "# Минимальный порог финального score для «горячих» проспектов\n",
    "SCORE_THRESHOLD = 0.60\n",
    "\n",
    "# Количество топ-проспектов для экспорта\n",
    "TOP_N_PROSPECTS = 100\n",
]

# ── Cell 5: Update markdown description ───────────────────────────────────────
nb["cells"][5]["source"] = [
    "---\n",
    "## 2. Определение эталонной группы (многофакторный отбор)\n",
    "\n",
    "**Лучшие привлечённые клиенты** отбираются по composite quality score:\n",
    "\n",
    "1. Сейчас клиент (`clientcounterparty_flag = 'Y'`)\n",
    "2. Когда-то был контрагентом (`clientchange_date IS NOT NULL`)\n",
    "3. **Composite quality score** — взвешенная сумма Z-score по 6 измерениям:\n",
    "   - `tx_count` (25%) — транзакционная активность\n",
    "   - `unique_counterparties` (20%) — ширина сети\n",
    "   - `active_months` (15%) — регулярность\n",
    "   - `amount_growth` (15%) — рост оборота\n",
    "   - `total_amount` (15%) — объём бизнеса\n",
    "   - `direction_balance` (10%) — сбалансированность потоков\n",
    "4. Топ 20% по composite score → **эталонная группа**\n",
    "\n",
    "Преимущество над однофакторным отбором (только по обороту):\n",
    "- Клиент с большим оборотом, но 1 контрагентом и падающей динамикой → не попадёт в эталон\n",
    "- Клиент с умеренным оборотом, но широкой сетью и ростом → попадёт в эталон\n",
    "- Результат: более качественная эталонная группа, лучше отражающая «идеального» клиента",
]

# ── Cell 6: Multi-factor reference selection ──────────────────────────────────
nb["cells"][6]["source"] = [
    "# Клиенты\n",
    'clients_mask = full_df["clientcounterparty_flag"] == "Y"\n',
    'print(f"Clients (Y): {clients_mask.sum():,}")\n',
    "\n",
    "# Привлечённые (были контрагентами → стали клиентами)\n",
    'if "clientchange_date" in full_df.columns:\n',
    '    acquired_mask = clients_mask & full_df["clientchange_date"].notna()\n',
    '    print(f"Acquired (Y + clientchange_date): {acquired_mask.sum():,}")\n',
    "else:\n",
    "    acquired_mask = clients_mask\n",
    '    print("clientchange_date not available — using all clients")\n',
    "\n",
    "# Если мало привлечённых, используем всех клиентов\n",
    "if acquired_mask.sum() < 100:\n",
    '    print(f"Too few acquired ({acquired_mask.sum()}), falling back to all clients")\n',
    "    acquired_mask = clients_mask\n",
    "\n",
    "# ── Многофакторный composite quality score ────────────────────────────────\n",
    "acquired_df = full_df[acquired_mask].copy()\n",
    "\n",
    "# Собираем доступные метрики для composite score\n",
    "quality_raw = pd.DataFrame(index=acquired_df.index)\n",
    "for col in QUALITY_WEIGHTS:\n",
    '    if col == "direction_balance":\n',
    "        # Сбалансированность: |0.5 - DR| → инверсия (ближе к 0.5 = лучше)\n",
    '        if "direction_ratio" in acquired_df.columns:\n',
    '            quality_raw[col] = 1 - (acquired_df["direction_ratio"] - 0.5).abs() * 2\n',
    "    elif col in acquired_df.columns:\n",
    "        quality_raw[col] = acquired_df[col]\n",
    "\n",
    "# Z-score нормализация каждого измерения\n",
    "quality_z = pd.DataFrame(index=quality_raw.index)\n",
    "for col in quality_raw.columns:\n",
    "    _mean = quality_raw[col].mean()\n",
    "    _std = quality_raw[col].std()\n",
    "    quality_z[col] = (quality_raw[col] - _mean) / (_std + 1e-9)\n",
    "\n",
    "# Взвешенная сумма Z-score → composite quality score\n",
    "composite = pd.Series(0.0, index=quality_z.index)\n",
    "active_weights = {}\n",
    "for col in quality_z.columns:\n",
    "    w = QUALITY_WEIGHTS.get(col, 0.0)\n",
    "    composite += quality_z[col] * w\n",
    "    active_weights[col] = w\n",
    "\n",
    "# Нормируем веса если не все метрики доступны\n",
    "total_w = sum(active_weights.values())\n",
    "if total_w > 0 and abs(total_w - 1.0) > 0.01:\n",
    "    composite = composite / total_w\n",
    "\n",
    'acquired_df["composite_quality"] = composite\n',
    "\n",
    "# Топ N% по composite score\n",
    "quality_threshold = composite.quantile(1 - TOP_PERCENTILE)\n",
    "reference_indices = acquired_df[composite >= quality_threshold].index\n",
    "reference_mask = full_df.index.isin(reference_indices)\n",
    "\n",
    'print(f"\\n{"="*60}")\n',
    'print("МНОГОФАКТОРНЫЙ ОТБОР ЭТАЛОННОЙ ГРУППЫ")\n',
    'print(f"{"="*60}")\n',
    'print(f"Composite quality score — {len(active_weights)} измерений:")\n',
    "for col, w in sorted(active_weights.items(), key=lambda x: -x[1]):\n",
    "    z_mean_ref = quality_z.loc[reference_indices, col].mean()\n",
    "    z_mean_all = quality_z[col].mean()\n",
    "    print(\n",
    '        f"  {col:26s}  вес={w:.0%}  Z(эталон)={z_mean_ref:+.2f}"\n',
    '        f"  Z(все)={z_mean_all:+.2f}"\n',
    "    )\n",
    "\n",
    'print(f"\\nComposite threshold (top {TOP_PERCENTILE*100:.0f}%): {quality_threshold:.3f}")\n',
    'print(f"Reference group: {reference_mask.sum():,} клиентов")\n',
    "\n",
    "# ── Сравнение с однофакторным отбором ─────────────────────────────────────\n",
    'old_threshold = acquired_df["total_amount"].quantile(1 - TOP_PERCENTILE)\n',
    'old_mask = acquired_mask & (full_df["total_amount"] >= old_threshold)\n',
    "overlap = (reference_mask & old_mask).sum()\n",
    "only_new = (reference_mask & ~old_mask).sum()\n",
    "only_old = (old_mask & ~reference_mask).sum()\n",
    "\n",
    'print(f"\\nСравнение с отбором по обороту (top {TOP_PERCENTILE*100:.0f}% total_amount):")\n',
    'print(f"  Совпадение: {overlap:,} ({overlap / reference_mask.sum() * 100:.0f}%)")\n',
    'print(f"  Только в новом (многофакт.): {only_new:,} — клиенты с широкой сетью/ростом")\n',
    'print(f"  Только в старом (оборот): {only_old:,} — высокий оборот, но слабая сеть/рост")\n',
    "\n",
    "# ── Профиль: новый эталон vs старый ──────────────────────────────────────\n",
    "_profile_cols = [\n",
    '    c for c in ["total_amount", "tx_count", "unique_counterparties",\n',
    '               "active_months", "amount_growth", "direction_ratio"]\n',
    "    if c in full_df.columns\n",
    "]\n",
    "profile_compare = pd.DataFrame({\n",
    '    "Новый эталон (composite)": full_df.loc[reference_mask, _profile_cols].mean(),\n',
    '    "Старый эталон (оборот)": full_df.loc[old_mask, _profile_cols].mean(),\n',
    '    "Все привлечённые": acquired_df[_profile_cols].mean(),\n',
    "})\n",
    'print("\\nПрофиль эталонных групп:")\n',
    "display(profile_compare.round(2))\n",
]

# ── Cell 8: Update comment about reference selection ──────────────────────────
# Find and replace the leakage comment
cell_8_src = "".join(nb["cells"][8]["source"])
cell_8_src = cell_8_src.replace(
    "# Эталон = top 20% по total_amount → monetary features исключены (leakage).",
    "# Эталон = top 20% по composite quality score (многофакторный).\n"
    "# Monetary features исключены из GBM whitelist (leakage protection).",
)
nb["cells"][8]["source"] = cell_8_src.split("\n")
nb["cells"][8]["source"] = [line + "\n" for line in nb["cells"][8]["source"]]
# Fix trailing empty \n
if nb["cells"][8]["source"][-1] == "\n":
    nb["cells"][8]["source"] = nb["cells"][8]["source"][:-1]

# ── Cell 21 (glossary): Update reference group definition ─────────────────────
cell_21_src = "".join(nb["cells"][21]["source"])
cell_21_src = cell_21_src.replace(
    "Лучшие привлечённые клиенты: были контрагентами → стали клиентами + топ 20% по обороту",
    "Лучшие привлечённые клиенты: composite quality score"
    " (активность + сеть + рост + стабильность + объём), топ 20%",
)
nb["cells"][21]["source"] = cell_21_src.split("\n")
nb["cells"][21]["source"] = [line + "\n" for line in nb["cells"][21]["source"]]
if nb["cells"][21]["source"][-1] == "\n":
    nb["cells"][21]["source"] = nb["cells"][21]["source"][:-1]

with open("lookalike/05_lookalike.ipynb", "w", encoding="utf-8") as f:
    json.dump(nb, f, ensure_ascii=False, indent=1)
print("✅ 05_lookalike.ipynb updated (multi-factor reference selection)")


# ═══════════════════════════════════════════════════════════════════════════════
# 2. Improve 04_segmentation.ipynb — better segment definitions
# ═══════════════════════════════════════════════════════════════════════════════
with open("lookalike/04_segmentation.ipynb", encoding="utf-8") as f:
    seg_nb = json.load(f)

# ── Cell 13: Improved auto_label_segment with reference potential ─────────────
# Find cell-13 by looking for "auto_label_segment"
target_idx = None
for i, cell in enumerate(seg_nb["cells"]):
    src = "".join(cell.get("source", []))
    if "def auto_label_segment" in src:
        target_idx = i
        break

if target_idx is not None:
    seg_nb["cells"][target_idx]["source"] = [
        "# ruff: noqa: F821\n",
        "# Пороги на основе реального распределения всей популяции.\n",
        "# Используем квантили генеральной совокупности, а не медианы сегментных средних.\n",
        "_label_cols = [\n",
        "    c\n",
        '    for c in ["tx_count", "unique_counterparties", "total_amount",\n',
        '             "active_months", "amount_growth"]\n',
        "    if c in full_df.columns\n",
        "]\n",
        "_q25 = full_df[_label_cols].quantile(0.25)\n",
        "_q75 = full_df[_label_cols].quantile(0.75)\n",
        "_q90 = full_df[_label_cols].quantile(0.90)\n",
        "_q95 = full_df[_label_cols].quantile(0.95)\n",
        "_med = full_df[_label_cols].median()\n",
        "\n",
        "\n",
        "def auto_label_segment(profile: pd.Series) -> tuple[str, str]:\n",
        '    """Присваивает бизнес-название и оценку потенциала для эталона."""\n',
        '    tx = profile.get("tx_count", 0)\n',
        '    cp = profile.get("unique_counterparties", 0)\n',
        '    amt = profile.get("total_amount", 0)\n',
        '    act = profile.get("active_months", 0)\n',
        '    grow = profile.get("amount_growth", 0)\n',
        '    dr = profile.get("direction_ratio", 0.5)\n',
        "\n",
        "    # ── Определяем уровни по каждому измерению ────────────────────────\n",
        '    is_mega_tx = tx >= _q95.get("tx_count", 1e9)\n',
        '    is_mega_cp = cp >= _q95.get("unique_counterparties", 1e9)\n',
        '    is_high_tx = tx >= _q75.get("tx_count", 1e9)\n',
        '    is_high_cp = cp >= _q75.get("unique_counterparties", 1e9)\n',
        '    is_high_amt = amt >= _q75.get("total_amount", 1e9)\n',
        '    is_growing = grow >= _q75.get("amount_growth", 1e9)\n',
        '    is_active = act >= _med.get("active_months", 0)\n',
        '    is_declining = grow < _q25.get("amount_growth", -1e9)\n',
        '    is_low_tx = tx < _q25.get("tx_count", 0)\n',
        '    is_low_cp = cp < _q25.get("unique_counterparties", 0)\n',
        "\n",
        "    # ── Composite reference potential (0–100) ─────────────────────────\n",
        "    ref_score = 0\n",
        "    if is_mega_tx or is_mega_cp:\n",
        "        ref_score += 30\n",
        "    elif is_high_tx or is_high_cp:\n",
        "        ref_score += 20\n",
        "    elif is_low_tx and is_low_cp:\n",
        "        ref_score -= 10\n",
        "    if is_growing:\n",
        "        ref_score += 25\n",
        "    elif is_declining:\n",
        "        ref_score -= 15\n",
        "    if is_active:\n",
        "        ref_score += 15\n",
        "    if is_high_amt:\n",
        "        ref_score += 15\n",
        "    if 0.30 <= dr <= 0.70:  # balanced flow\n",
        "        ref_score += 15\n",
        "\n",
        "    # ── Определяем название сегмента ──────────────────────────────────\n",
        "    if is_mega_tx and is_mega_cp:\n",
        '        name = "Мега-хаб (системообразующий)"\n',
        "    elif is_high_tx and is_high_cp and is_growing:\n",
        '        name = "Растущий хаб (высокая активность + рост)"\n',
        "    elif is_high_tx and is_high_cp:\n",
        '        name = "Хаб (много контрагентов, высокая активность)"\n',
        "    elif is_growing and (is_high_tx or is_high_cp):\n",
        '        name = "Растущий активный (рост + широкая сеть)"\n',
        "    elif is_growing:\n",
        '        name = "Растущий (агрессивный рост оборота)"\n',
        "    elif dr >= 0.70:\n",
        "        if is_declining:\n",
        '            name = "Плательщик (расходы, оборот падает)"\n',
        "        else:\n",
        '            name = "Плательщик (преимущественно расходы)"\n',
        "    elif dr <= 0.30:\n",
        "        if is_declining:\n",
        '            name = "Получатель (входящие, оборот падает)"\n',
        "        else:\n",
        '            name = "Получатель (преимущественно входящие)"\n',
        "    elif is_high_amt and is_active:\n",
        '        name = "Крупный стабильный (объём + регулярность)"\n',
        "    elif is_low_tx and is_low_cp and is_declining:\n",
        '        name = "Неактивный / затухающий"\n',
        "    elif is_low_tx and is_low_cp:\n",
        '        name = "Малоактивный (низкие показатели)"\n',
        "    else:\n",
        '        name = "Средний / смешанный профиль"\n',
        "\n",
        "    # Потенциал для эталона\n",
        "    if ref_score >= 60:\n",
        '        potential = "Высокий"\n',
        "    elif ref_score >= 30:\n",
        '        potential = "Средний"\n',
        "    elif ref_score >= 0:\n",
        '        potential = "Низкий"\n',
        "    else:\n",
        '        potential = "Не подходит"\n',
        "\n",
        "    return name, potential\n",
        "\n",
        "\n",
        "segment_labels = {}\n",
        "segment_potentials = {}\n",
        "for seg_id, row in segment_profiles.iterrows():\n",
        "    name, potential = auto_label_segment(row)\n",
        "    segment_labels[seg_id] = name\n",
        "    segment_potentials[seg_id] = potential\n",
        "\n",
        'full_df["segment_label"] = full_df["behavioral_segment"].map(segment_labels)\n',
        "\n",
        'print("Маркировка сегментов:")\n',
        'print(f"{"─"*75}")\n',
        "for seg_id, label in sorted(segment_labels.items()):\n",
        '    n = (full_df["behavioral_segment"] == seg_id).sum()\n',
        "    pct = n / len(full_df) * 100\n",
        "    pot = segment_potentials[seg_id]\n",
        "    pot_icon = {\n",
        '        "Высокий": "✅", "Средний": "⚠️ ",\n',
        '        "Низкий": "❌", "Не подходит": "⛔"\n',
        '    }.get(pot, "?")\n',
        "    print(\n",
        '        f"  Сег. {seg_id} | {n:>5,} ({pct:4.1f}%) | "\n',
        '        f"{pot_icon} Потенциал: {pot:12s} | {label}"\n',
        "    )\n",
        'print(f"{"─"*75}")\n',
        'print("\\nПотенциал для эталона рассчитан по 6 измерениям:")\n',
        'print("  tx_count, counterparties, amount_growth, active_months,"\n',
        '      " total_amount, direction_balance")\n',
    ]
    print(f"✅ 04_segmentation.ipynb cell-{target_idx} updated (improved segment labels)")
else:
    print("⚠️  auto_label_segment not found in 04_segmentation.ipynb")

with open("lookalike/04_segmentation.ipynb", "w", encoding="utf-8") as f:
    json.dump(seg_nb, f, ensure_ascii=False, indent=1)
print("✅ 04_segmentation.ipynb saved")
