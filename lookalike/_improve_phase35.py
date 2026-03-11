"""Insert Phase 3-5 cells into 05_lookalike.ipynb — threshold, segment-aware,
stacking, ablation, score bands, campaigns, model card, updated export/glossary."""

import json
import sys

sys.stdout.reconfigure(encoding="utf-8")

NB_PATH = "lookalike/05_lookalike.ipynb"

with open(NB_PATH, encoding="utf-8") as f:
    nb = json.load(f)


def make_cell(cell_type: str, source: str) -> dict:
    return {
        "cell_type": cell_type,
        "metadata": {},
        "source": source.split("\n")
        if "\n" not in source
        else [line + "\n" for line in source.split("\n")[:-1]] + [source.split("\n")[-1]],
        **({"outputs": [], "execution_count": None} if cell_type == "code" else {}),
    }


# ── Find insertion points ────────────────────────────────────────────────────
# After cell-13 (calibration code) — insert threshold, segment-aware, stacking, ablation
# Before save cell (currently idx 24) — insert score bands, campaigns, model card
# Replace save cell (idx 24) and glossary (idx 25)

cells = nb["cells"]

# Find calibration code cell (has "Calibration Analysis" in source)
calibration_idx = None
for i, c in enumerate(cells):
    src = "".join(c.get("source", []))
    if "Calibration Analysis" in src and c["cell_type"] == "code":
        calibration_idx = i
        break
assert calibration_idx is not None, "Calibration cell not found"
print(f"Calibration code cell at index {calibration_idx}")

# Find save/export cell (has "lookalike_scores.parquet" and "lookalike_gbm.pkl")
save_idx = None
for i, c in enumerate(cells):
    src = "".join(c.get("source", []))
    if "lookalike_scores.parquet" in src and "lookalike_gbm.pkl" in src:
        save_idx = i
        break
assert save_idx is not None, "Save cell not found"
print(f"Save cell at index {save_idx}")

# Find glossary cell (last markdown with "Глоссарий")
glossary_idx = None
for i, c in enumerate(cells):
    src = "".join(c.get("source", []))
    if "Глоссарий" in src and c["cell_type"] == "markdown":
        glossary_idx = i
assert glossary_idx is not None, "Glossary cell not found"
print(f"Glossary cell at index {glossary_idx}")

# ── New cells to insert after calibration ─────────────────────────────────────

THRESHOLD_MD = """\
---
## 3.8. Оптимизация порога (Threshold)

Поиск оптимального cutoff для классификации проспектов."""

THRESHOLD_CODE = """\
# ── Threshold Optimization ───────────────────────────────────────────────────
from sklearn.metrics import roc_curve, precision_recall_curve, f1_score

# ROC curve on validation set
fpr, tpr, roc_thresholds = roc_curve(y_val, clf.predict_proba(X_val)[:, 1])
roc_auc = roc_auc_score(y_val, clf.predict_proba(X_val)[:, 1])

# Youden's J statistic
j_scores = tpr - fpr
youden_idx = j_scores.argmax()
youden_threshold = roc_thresholds[youden_idx]

# Precision-Recall curve
precision_arr, recall_arr, pr_thresholds = precision_recall_curve(
    y_val, clf.predict_proba(X_val)[:, 1]
)
pr_auc = average_precision_score(y_val, clf.predict_proba(X_val)[:, 1])

# F1-optimal threshold
f1_scores_arr = 2 * (precision_arr[:-1] * recall_arr[:-1]) / (
    precision_arr[:-1] + recall_arr[:-1] + 1e-9
)
f1_idx = f1_scores_arr.argmax()
f1_threshold = pr_thresholds[f1_idx]

fig, axes = plt.subplots(1, 2, figsize=(14, 5))

axes[0].plot(fpr, tpr, color="#d62728", linewidth=2, label=f"AUC = {roc_auc:.3f}")
axes[0].plot([0, 1], [0, 1], "k--", alpha=0.5)
axes[0].scatter(fpr[youden_idx], tpr[youden_idx], s=100, c="green", zorder=5,
                label=f"Youden J (t={youden_threshold:.3f})")
axes[0].set_xlabel("False Positive Rate")
axes[0].set_ylabel("True Positive Rate")
axes[0].set_title(f"ROC Curve (AUC = {roc_auc:.3f})")
axes[0].legend()

axes[1].plot(recall_arr, precision_arr, color="#1f77b4", linewidth=2,
             label=f"AP = {pr_auc:.3f}")
axes[1].scatter(recall_arr[f1_idx], precision_arr[f1_idx], s=100, c="red", zorder=5,
                label=f"F1-optimal (t={f1_threshold:.3f})")
axes[1].set_xlabel("Recall")
axes[1].set_ylabel("Precision")
axes[1].set_title(f"Precision-Recall Curve (AP = {pr_auc:.3f})")
axes[1].legend()

plt.suptitle("ROC и Precision-Recall анализ", fontsize=13)
plt.tight_layout()
plt.show()

OPTIMAL_THRESHOLD = youden_threshold
print(f"Youden J threshold: {OPTIMAL_THRESHOLD:.3f}")
print(f"F1-optimal threshold: {f1_threshold:.3f}")
print(f"AUC-ROC: {roc_auc:.3f},  PR-AUC: {pr_auc:.3f}")"""

SEGMENT_AWARE_MD = """\
---
## 3.9. Segment-Aware валидация

Качество модели по каждому поведенческому сегменту: выявляем слабые места."""

SEGMENT_AWARE_CODE = """\
# ── Segment-Aware Validation ─────────────────────────────────────────────────
if "behavioral_segment" in full_df.columns:
    segments = sorted(full_df["behavioral_segment"].dropna().unique())
    seg_metrics = []

    for seg_id in segments:
        seg_mask = full_df["behavioral_segment"] == seg_id
        seg_scores = full_df.loc[seg_mask, "lookalike_score"].values
        seg_ref = reference_mask[seg_mask].values
        n_total = int(seg_mask.sum())
        n_ref = int(seg_ref.sum())

        if n_ref < 5 or n_total - n_ref < 5:
            continue

        sep = float(seg_scores[seg_ref].mean() - seg_scores[~seg_ref].mean())

        try:
            auc = roc_auc_score(seg_ref.astype(int), seg_scores)
        except ValueError:
            auc = np.nan

        n_top = max(1, int(n_total * 0.05))
        top_idx = np.argsort(seg_scores)[::-1][:n_top]
        lift_5 = seg_ref[top_idx].mean() / max(seg_ref.mean(), 1e-9)

        label = ""
        if "segment_label" in full_df.columns:
            label = str(full_df.loc[seg_mask, "segment_label"].iloc[0])[:25]
        seg_metrics.append({
            "Segment": seg_id, "Label": label, "N": n_total, "N_ref": n_ref,
            "Separation": round(sep, 4),
            "AUC-ROC": round(auc, 4) if not np.isnan(auc) else "N/A",
            "Lift@5%": round(lift_5, 2),
        })

    if seg_metrics:
        seg_val_df = pd.DataFrame(seg_metrics)
        print("Качество модели по сегментам:")
        display(seg_val_df)

        weak = [r for r in seg_metrics
                if isinstance(r["AUC-ROC"], float) and r["AUC-ROC"] < 0.65]
        if weak:
            print(f"\\nСлабые сегменты ({len(weak)}):")
            for w in weak:
                print(f"  Сег. {w['Segment']} ({w['Label']}): "
                      f"AUC={w['AUC-ROC']}, Sep={w['Separation']}")
        else:
            print("\\nВсе сегменты с AUC >= 0.65")
    else:
        print("Недостаточно данных для segment-aware анализа")
else:
    print("behavioral_segment не найден — segment-aware анализ пропущен")"""

STACKING_MD = """\
---
## 3.10. Stacking Ensemble

Замена фиксированных весов на обученный meta-learner (LogisticRegression)
поверх OOF-предсказаний базовых моделей."""

STACKING_CODE = """\
# ── Stacking Ensemble ────────────────────────────────────────────────────────
from sklearn.linear_model import LogisticRegression

# Indices of train clients in X
_train_client_idx = np.concatenate([
    np.where(X.index.isin(pos_idx))[0],
    np.where(X.index.isin(neg_idx))[0],
])
knn_train_aligned = knn_scores[_train_client_idx]
centroid_train_aligned = centroid_norm[_train_client_idx]

# Meta-features: [GBM_oof, kNN, Centroid]
meta_train = np.column_stack([oof_predictions, knn_train_aligned, centroid_train_aligned])
meta_all = np.column_stack([
    full_df["score_gbm"].values,
    knn_scores,
    centroid_norm,
])

stacker = LogisticRegression(random_state=42, max_iter=1000)
stacker.fit(meta_train, y_train)
stacking_scores = stacker.predict_proba(meta_all)[:, 1]

_st_min, _st_max = stacking_scores.min(), stacking_scores.max()
stacking_norm = (stacking_scores - _st_min) / (_st_max - _st_min + 1e-9)
full_df["score_stacking"] = stacking_norm

old_sep = (full_df.loc[reference_mask, "lookalike_score"].mean()
           - full_df.loc[~reference_mask, "lookalike_score"].mean())
new_sep = (stacking_norm[reference_mask.values].mean()
           - stacking_norm[~reference_mask.values].mean())

print("Stacking vs Fixed-Weight Ensemble:")
print(f"  Old ensemble (0.65*GBM + 0.35*kNN): Separation = {old_sep:.4f}")
print(f"  Stacking (LR meta-learner):          Separation = {new_sep:.4f}")
print(f"  Stacker coefs: GBM={stacker.coef_[0][0]:.3f}, "
      f"kNN={stacker.coef_[0][1]:.3f}, Centroid={stacker.coef_[0][2]:.3f}")

if new_sep > old_sep:
    full_df["lookalike_score"] = stacking_norm
    ENSEMBLE_METHOD = "Stacking"
    print("\\nStacking лучше — используем stacking score как финальный")
else:
    ENSEMBLE_METHOD = "Fixed-weight"
    print("\\nFixed-weight ensemble лучше — оставляем текущий score")"""

ABLATION_MD = """\
---
## 3.11. Ablation Study

Какие группы признаков критичны для модели?"""

ABLATION_CODE = """\
# ── Ablation Study ───────────────────────────────────────────────────────────
FEATURE_GROUPS = {
    "Граф-метрики": [c for c in gbm_feature_cols if any(
        c.startswith(p) for p in ["pagerank", "betweenness", "clustering_coef",
                                   "flow_through_ratio", "in_degree", "out_degree",
                                   "top_k_concentration", "okved_diversity_", "role_",
                                   "network_influence"])],
    "Категориальные": [c for c in gbm_feature_cols if any(
        c.startswith(p) for p in ["okved_", "region_", "mseg_", "ctype_"])],
    "Временные": [c for c in gbm_feature_cols if any(
        c.startswith(p) for p in ["active_months", "amount_growth", "cp_growth",
                                   "growth_acceleration", "monthly_volatility",
                                   "counterparty_retention"])],
    "Структура сети": [c for c in gbm_feature_cols if any(
        c.startswith(p) for p in ["unique_", "payee_ratio", "payer_ratio",
                                   "cp_per_month", "herfindahl_index", "amt_per_cp"])],
}

ablation_results = []
full_auc = roc_auc_score(y_val, clf.predict_proba(X_val)[:, 1])
ablation_results.append({"Группа": "Все признаки (baseline)",
                          "N_features": len(gbm_feature_cols),
                          "AUC-ROC": round(full_auc, 4), "Delta AUC": 0.0})

for group_name, group_cols in FEATURE_GROUPS.items():
    present = [c for c in group_cols if c in gbm_feature_cols]
    if not present:
        continue
    remaining = [c for c in gbm_feature_cols if c not in present]
    if len(remaining) < 3:
        continue
    remaining_idx = [gbm_feature_cols.index(c) for c in remaining]
    X_tr_abl = X_tr[:, remaining_idx]
    X_val_abl = X_val[:, remaining_idx]
    try:
        import lightgbm as lgb
        abl_clf = lgb.LGBMClassifier(
            n_estimators=300, learning_rate=0.05, num_leaves=63,
            min_child_samples=20, subsample=0.8, colsample_bytree=0.8,
            n_jobs=-1, random_state=42, verbose=-1,
        )
        abl_clf.fit(X_tr_abl, y_tr,
                     eval_set=[(X_val_abl, y_val)],
                     callbacks=[lgb.early_stopping(20, verbose=False),
                                lgb.log_evaluation(period=-1)])
    except ImportError:
        abl_clf = GradientBoostingClassifier(
            n_estimators=200, max_depth=4, learning_rate=0.05, random_state=42)
        abl_clf.fit(X_tr_abl, y_tr)
    abl_auc = roc_auc_score(y_val, abl_clf.predict_proba(X_val_abl)[:, 1])
    ablation_results.append({
        "Группа": f"Без '{group_name}' ({len(present)})",
        "N_features": len(remaining),
        "AUC-ROC": round(abl_auc, 4),
        "Delta AUC": round(abl_auc - full_auc, 4),
    })

abl_df = pd.DataFrame(ablation_results)
print("Ablation Study — влияние групп признаков:")
display(abl_df)

if len(abl_df) > 1:
    fig, ax = plt.subplots(figsize=(10, 5))
    colors = (["#2ca02c"]
              + ["#d62728" if d < -0.01 else "#1f77b4"
                 for d in abl_df["Delta AUC"][1:]])
    ax.barh(abl_df["Группа"], abl_df["AUC-ROC"], color=colors,
            edgecolor="black", alpha=0.8)
    ax.axvline(full_auc, color="green", linestyle="--",
               label=f"Baseline={full_auc:.3f}")
    ax.set_xlabel("AUC-ROC")
    ax.set_title("Ablation: AUC-ROC при удалении групп признаков")
    ax.legend()
    for i, (_, row) in enumerate(abl_df.iterrows()):
        ax.text(row["AUC-ROC"] + 0.002, i,
                f"{row['Delta AUC']:+.4f}", va="center", fontsize=9)
    plt.tight_layout()
    plt.show()"""

# ── New cells to insert before save ──────────────────────────────────────────

SCORE_BANDS_MD = """\
---
## 7. Скоровые зоны (Score Bands)

Разбиение проспектов на бизнес-группы по уровню score для таргетирования."""

SCORE_BANDS_CODE = """\
# ── Score Bands ──────────────────────────────────────────────────────────────
prospects_mask = full_df["clientcounterparty_flag"].isin(["N", "?"])
prospects = full_df[prospects_mask].sort_values("lookalike_score", ascending=False)

band_labels = []
for _, row in prospects.iterrows():
    score = row["lookalike_score"]
    assigned = "Cold"
    for band_name, (lo, hi) in SCORE_BANDS.items():
        if lo <= score < hi or (band_name == "Hot" and score >= lo):
            assigned = band_name
            break
    band_labels.append(assigned)

full_df.loc[prospects_mask, "score_band"] = band_labels

band_order = ["Hot", "Warm", "Medium", "Cool", "Cold"]
band_colors = {"Hot": "#d62728", "Warm": "#ff7f0e", "Medium": "#ffdd57",
               "Cool": "#1f77b4", "Cold": "#aec7e8"}

band_stats = []
for band_name in band_order:
    band_mask = full_df["score_band"] == band_name
    n = int(band_mask.sum())
    if n == 0:
        continue
    band_data = full_df[band_mask]
    pct_etalon = (band_data.index.isin(
        full_df[reference_mask].index)).mean() if reference_mask.sum() > 0 else np.nan
    band_stats.append({
        "Band": band_name, "Count": n,
        "% of total": round(n / len(prospects) * 100, 1),
        "Mean Score": round(band_data["lookalike_score"].mean(), 3),
        "P(etalon)": round(pct_etalon, 3),
        "Mean Amount": round(band_data["total_amount"].mean(), 0)
            if "total_amount" in band_data.columns else "N/A",
    })

band_stats_df = pd.DataFrame(band_stats)
print("Скоровые зоны (Score Bands):")
display(band_stats_df)

fig, axes = plt.subplots(1, 2, figsize=(14, 5))
valid_bands = [b for b in band_order if (full_df["score_band"] == b).sum() > 0]
valid_counts = [(full_df["score_band"] == b).sum() for b in valid_bands]
axes[0].bar(valid_bands, valid_counts,
            color=[band_colors[b] for b in valid_bands], edgecolor="black", alpha=0.85)
axes[0].set_ylabel("Количество проспектов")
axes[0].set_title("Распределение по скоровым зонам")
for i, (b, c) in enumerate(zip(valid_bands, valid_counts)):
    axes[0].text(i, c + len(prospects) * 0.01, f"{c:,}", ha="center", fontsize=9)

for bn in band_order:
    bd = full_df[(full_df["score_band"] == bn) & prospects_mask]
    if len(bd) > 0:
        axes[1].hist(bd["lookalike_score"], bins=20, alpha=0.6,
                     color=band_colors[bn], label=bn, edgecolor="black")
axes[1].set_xlabel("Lookalike Score")
axes[1].set_ylabel("Count")
axes[1].set_title("Распределение score по зонам")
axes[1].legend()

plt.suptitle("Score Bands: бизнес-зоны для таргетирования", fontsize=13)
plt.tight_layout()
plt.show()"""

CAMPAIGN_MD = """\
---
## 8. Рекомендации по кампаниям"""

CAMPAIGN_CODE = """\
# ── Campaign Recommendations ─────────────────────────────────────────────────
_recs = {
    "Hot": "Персональный менеджер. Индивидуальные предложения. Приоритетная обработка.",
    "Warm": "Таргетированные звонки и email-кампании. Пакетные предложения.",
    "Medium": "Массовые кампании. Digital-каналы. Промо-предложения.",
    "Cool": "Мониторинг. Включить в nurturing-программу. Ожидать роста.",
    "Cold": "Не тратить ресурсы. Автоматический мониторинг раз в квартал.",
}

if "total_amount" in full_df.columns and reference_mask.sum() > 0:
    ref_avg_amount = full_df.loc[reference_mask, "total_amount"].mean()
    print(f"Средний оборот эталонного клиента: {ref_avg_amount:,.0f} руб.\\n")

print("=" * 80)
print("РЕКОМЕНДАЦИИ ПО КАМПАНИЯМ")
print("=" * 80)

for band_name in ["Hot", "Warm", "Medium", "Cool", "Cold"]:
    band_data = full_df[full_df["score_band"] == band_name]
    n = len(band_data)
    if n == 0:
        continue
    print(f"\\n{'─' * 70}")
    print(f"  {band_name} ({n:,} проспектов)")
    print(f"  {_recs[band_name]}")
    if "total_amount" in band_data.columns:
        mean_amt = band_data["total_amount"].mean()
        print(f"  Средний текущий оборот: {mean_amt:,.0f} руб.")

total_hot_warm = full_df[full_df["score_band"].isin(["Hot", "Warm"])].shape[0]
print(f"\\n{'─' * 70}")
print(f"\\nРекомендуемый размер кампании: {total_hot_warm:,} проспектов (Hot + Warm)")"""

MODEL_CARD_MD = """\
---
## 9. Model Card

| Параметр | Значение |
|----------|----------|
| **Модель** | LightGBM / XGBoost / sklearn GBM (fallback) |
| **Задача** | Бинарная классификация: эталонный клиент vs остальные |
| **Эталон** | Многофакторный composite quality score (6 измерений), топ 20% |
| **Валидация** | Stratified 5-Fold CV |
| **Ensemble** | Stacking (LR) или Fixed-weight (0.65*GBM + 0.35*kNN) |
| **Leakage protection** | Whitelist поведенческих признаков |
| **Score bands** | Hot / Warm / Medium / Cool / Cold |

### Ограничения
- Модель обучена на данных за 1 год (Jan-Dec 2025)
- Контрагенты без транзакций не попадают в выборку
- Рекомендуется переобучение каждые 6 месяцев

### Bias disclaimer
- Возможно смещение к крупным клиентам с высокой активностью
- Рекомендуется мониторинг распределения score по ОКВЭД и регионам"""

# ── Updated save/export cell ──────────────────────────────────────────────────

SAVE_CODE = """\
# ruff: noqa: F821
import json as _json

scores_path = os.path.join(OUTPUT_DIR, "lookalike_scores.parquet")
full_df.to_parquet(scores_path)
print(f"Full scores saved: {scores_path}")

model_path = os.path.join(OUTPUT_DIR, "lookalike_gbm.pkl")
with open(model_path, "wb") as f:
    pickle.dump({"model": clf, "model_name": clf_name, "feature_names": feature_names}, f)
print(f"GBM model saved: {model_path}")

# Model card JSON
model_card = {
    "model_name": clf_name,
    "task": "binary_classification",
    "target": "reference_client",
    "reference_selection": "composite_quality_score_top20pct",
    "cv_folds": CV_FOLDS,
    "cv_auc_mean": round(float(cv_df["AUC-ROC"].mean()), 4),
    "cv_auc_std": round(float(cv_df["AUC-ROC"].std()), 4),
    "cv_separation_mean": round(float(cv_df["Separation"].mean()), 4),
    "brier_score": round(float(brier), 4),
    "ece": round(float(ece), 4),
    "ensemble_method": ENSEMBLE_METHOD if "ENSEMBLE_METHOD" in dir() else "Fixed-weight",
    "optimal_threshold": round(float(OPTIMAL_THRESHOLD), 4),
    "period": "2025-01-01 to 2025-12-31",
    "n_features": len(gbm_feature_cols),
    "n_reference": int(reference_mask.sum()),
    "n_prospects": int(prospects_mask.sum()),
    "score_bands": {b: f"{lo:.1f}-{hi:.1f}" for b, (lo, hi) in SCORE_BANDS.items()},
}
card_path = os.path.join(OUTPUT_DIR, "model_card.json")
with open(card_path, "w", encoding="utf-8") as f:
    _json.dump(model_card, f, ensure_ascii=False, indent=2)
print(f"Model card saved: {card_path}")

export_cols = [
    c for c in [
        "client_name", "client_type_name", "clientcounterparty_flag",
        "sparkokved_ccode", "addrref_city_name", "reg_city_name",
        "srvpackagesegment_ccode", "behavioral_segment", "segment_label",
        "total_amount", "tx_count", "unique_counterparties",
        "direction_ratio", "active_months", "amount_growth",
        "score_centroid", "score_knn", "score_gbm", "score_gbm_norm",
        "score_stacking", "lookalike_score", "score_band",
    ] if c in prospects.columns
]

top_path = os.path.join(OUTPUT_DIR, "top_prospects.parquet")
prospects[export_cols].head(TOP_N_PROSPECTS * 5).to_parquet(top_path)
print(f"Top prospects saved: {top_path}")

try:
    xlsx_path = os.path.join(OUTPUT_DIR, "top_prospects.xlsx")
    (
        prospects[export_cols]
        .head(TOP_N_PROSPECTS * 5)
        .rename(columns={
            "lookalike_score": "score_final",
            "sparkokved_ccode": "okved",
            "addrref_city_name": "city",
            "srvpackagesegment_ccode": "model_segment",
        })
        .to_excel(xlsx_path, index=True)
    )
    print(f"Excel saved: {xlsx_path}")
except Exception as e:
    print(f"Excel export skipped ({e})")

print(f"\\n{'=' * 60}")
print("ИТОГО:")
print(f"  Модель: {clf_name}")
print(f"  CV AUC-ROC: {cv_df['AUC-ROC'].mean():.3f} +/- {cv_df['AUC-ROC'].std():.3f}")
print(f"  Brier Score: {brier:.4f}, ECE: {ece:.4f}")
print(f"  Эталонная группа: {reference_mask.sum():,}")
print(f"  Проспекты: {prospects_mask.sum():,}")
if "score_band" in full_df.columns:
    for band in ["Hot", "Warm", "Medium", "Cool", "Cold"]:
        n = (full_df["score_band"] == band).sum()
        if n > 0:
            print(f"  {band:8s}: {n:>6,}")"""

# ── Updated glossary ──────────────────────────────────────────────────────────

GLOSSARY_MD = """\
---

## Глоссарий

| Термин | Описание |
|--------|----------|
| **Эталонная группа** | Лучшие привлечённые клиенты: composite quality score, топ 20% |
| **Centroid baseline** | Cosine similarity с центроидом эталонов. Нижняя планка |
| **kNN cosine** | Среднее cosine similarity к K ближайшим эталонам |
| **LightGBM** | Гистограммный GBM. 5-10x быстрее sklearn |
| **GBM whitelist** | Только поведенческие признаки. Защита от target leakage |
| **SHAP** | SHapley Additive exPlanations. Направление + вклад признака |
| **Ensemble** | Stacking LR или 0.65*GBM + 0.35*kNN |
| **Separation Index** | mean(score ref) - mean(score non-ref) |
| **Lift** | Во сколько раз метод лучше random при отборе топ-K% |
| **Stratified K-Fold CV** | K-fold с сохранением пропорции классов |
| **AUC-ROC** | Area Under ROC Curve. 0.5=random, 1.0=perfect |
| **PR-AUC** | Area Under Precision-Recall Curve |
| **Brier Score** | MSE вероятностей. Ниже = лучше |
| **ECE** | Expected Calibration Error. Ниже = лучше |
| **Youden's J** | TPR - FPR. Оптимальный порог для ROC |
| **Stacking** | Meta-learner (LR) на OOF-предсказаниях базовых моделей |
| **Ablation Study** | Удаление группы признаков -> оценка влияния на AUC |
| **Score Bands** | Hot (0.8+), Warm (0.6-0.8), Medium (0.4-0.6), Cool (0.2-0.4), Cold (0-0.2) |
| **Target leakage** | Признак, закодирующий метку |

---

**Pipeline завершён.** Файлы в `data/`:
- `lookalike_scores.parquet` — все клиенты со scores
- `top_prospects.parquet` / `.xlsx` — топ-500 проспектов
- `lookalike_gbm.pkl` — обученная GBM модель
- `model_card.json` — метаданные модели для мониторинга"""

# ══════════════════════════════════════════════════════════════════════════════
# Apply changes
# ══════════════════════════════════════════════════════════════════════════════

# 1. Insert after calibration cell (idx = calibration_idx)
insert_after_cal = [
    make_cell("markdown", THRESHOLD_MD),
    make_cell("code", THRESHOLD_CODE),
    make_cell("markdown", SEGMENT_AWARE_MD),
    make_cell("code", SEGMENT_AWARE_CODE),
    make_cell("markdown", STACKING_MD),
    make_cell("code", STACKING_CODE),
    make_cell("markdown", ABLATION_MD),
    make_cell("code", ABLATION_CODE),
]

# Insert after calibration_idx
for j, new_cell in enumerate(insert_after_cal):
    cells.insert(calibration_idx + 1 + j, new_cell)

print(f"Inserted {len(insert_after_cal)} cells after calibration (idx {calibration_idx})")

# Recalculate save_idx and glossary_idx after insertions
shift = len(insert_after_cal)
save_idx += shift
glossary_idx += shift

# 2. Insert before save cell: score bands, campaigns, model card
insert_before_save = [
    make_cell("markdown", SCORE_BANDS_MD),
    make_cell("code", SCORE_BANDS_CODE),
    make_cell("markdown", CAMPAIGN_MD),
    make_cell("code", CAMPAIGN_CODE),
    make_cell("markdown", MODEL_CARD_MD),
]

for j, new_cell in enumerate(insert_before_save):
    cells.insert(save_idx + j, new_cell)

print(f"Inserted {len(insert_before_save)} cells before save (idx {save_idx})")

# Recalculate after insertions
shift2 = len(insert_before_save)
save_idx += shift2
glossary_idx += shift2

# 3. Replace save cell
cells[save_idx] = make_cell("code", SAVE_CODE)
print(f"Replaced save cell at index {save_idx}")

# 4. Replace glossary cell
cells[glossary_idx] = make_cell("markdown", GLOSSARY_MD)
print(f"Replaced glossary cell at index {glossary_idx}")

nb["cells"] = cells

with open(NB_PATH, "w", encoding="utf-8") as f:
    json.dump(nb, f, ensure_ascii=False, indent=1)

print(f"\nDone! Total cells: {len(cells)}")
