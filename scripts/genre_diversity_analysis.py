"""
Genre Diversity Analysis for Developers and Publishers

This script produces:
- Normalized diversity figures for developers and publishers by year and age
- Normalized comparison figures for developers vs publishers
- A LaTeX table of game-count means and standard deviations by threshold
- Two CSV datasets containing the points used in the figures
"""

import json
import re
import sqlite3
from collections import defaultdict
from pathlib import Path

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
plt.rcParams["figure.figsize"] = (14, 6)

BASE_DIR = Path(__file__).parent.parent
DATA_DIR = BASE_DIR / "data"
FIGURES_DIR = BASE_DIR / "figures" / "genre distribution"
DB_PATH = Path(
    "/Users/pipeton8/Library/CloudStorage/Dropbox/Research/_data/moby-games-data/moby_games.db"
)
YEAR_MIN = 1990
YEAR_MAX = 2023
AGE_MAX = 30

FIGURES_DIR.mkdir(parents=True, exist_ok=True)

THRESHOLDS = [None, 2, 5, 10]
COMPARE_THRESHOLDS = [None, 5]


def parse_year(value):
    if value is None:
        return None
    if isinstance(value, (int, float)):
        year = int(value)
    elif isinstance(value, str):
        match = re.search(r"(\d{4})", value)
        if not match:
            return None
        year = int(match.group(1))
    else:
        return None
    if 1900 <= year <= 2100:
        return year
    return None


def extract_company_game_counts(entity_key, entity_label, include_yearly=False):
    total_counts = defaultdict(int)
    yearly_counts = defaultdict(int) if include_yearly else None
    name_map = {}

    with sqlite3.connect(DB_PATH) as conn:
        cur = conn.cursor()
        cur.execute("SELECT id, raw_data FROM games")
        for game_id, raw_text in cur:
            if not raw_text:
                continue
            raw = json.loads(raw_text)
            year = parse_year(raw.get("release_date"))
            companies = raw.get(entity_key) or []
            seen = set()
            for company in companies:
                company_id = company.get("id")
                if company_id is None or company_id in seen:
                    continue
                seen.add(company_id)
                name_map[company_id] = company.get("name")
                total_counts[company_id] += 1
                if include_yearly and year is not None:
                    yearly_counts[(company_id, year)] += 1

    total_rows = []
    for company_id, count in total_counts.items():
        total_rows.append(
            {
                f"{entity_label}_id": company_id,
                entity_label.capitalize(): name_map.get(company_id),
                "total_games": count,
            }
        )
    total_df = pd.DataFrame(total_rows)

    if not include_yearly:
        return total_df

    yearly_rows = []
    for (company_id, year), count in yearly_counts.items():
        yearly_rows.append(
            {
                f"{entity_label}_id": company_id,
                entity_label.capitalize(): name_map.get(company_id),
                "Year": year,
                "games_in_year": count,
            }
        )
    yearly_df = pd.DataFrame(yearly_rows)

    return total_df, yearly_df


def summarize_game_counts(total_df):
    stats = total_df["total_games"].describe(percentiles=[0.25, 0.5, 0.75])
    summary = pd.DataFrame(
        {
            "count": [stats["count"]],
            "mean": [stats["mean"]],
            "q1": [stats["25%"]],
            "median": [stats["50%"]],
            "q3": [stats["75%"]],
            "min": [stats["min"]],
            "max": [stats["max"]],
        }
    )
    return summary


def write_game_counts_table(dev_counts, pub_counts, output_path):
    rows = []
    for label, df in [("Developers", dev_counts), ("Publishers", pub_counts)]:
        for threshold in THRESHOLDS:
            if threshold is None:
                filtered = df
                threshold_label = "All"
            else:
                filtered = df[df["total_games"] >= threshold]
                threshold_label = f">= {threshold} games"
            rows.append(
                {
                    "Company type": label,
                    "Threshold": threshold_label,
                    "Mean games": filtered["total_games"].mean(),
                    "Std. dev.": filtered["total_games"].std(),
                    "N": int(filtered["total_games"].count()),
                }
            )

    table_df = pd.DataFrame(rows)
    latex = table_df.to_latex(
        index=False,
        float_format="%.3f",
        column_format="llrrr",
        caption="Game counts by company type and threshold",
        label="tab:game_counts_by_type",
    )
    output_path.write_text(latex)
    return output_path


def calculate_diversity_metrics(df, entity_label):
    genre_cols = [
        col
        for col in df.columns
        if col.startswith("genre_") and col.endswith("_share")
    ]

    diversity_metrics = []

    for _, row in df.iterrows():
        shares = np.array(row[genre_cols].values, dtype=float)
        shares_sum = np.sum(shares)
        if shares_sum <= 0:
            continue

        shares_norm = shares / shares_sum
        non_zero_norm = shares_norm[shares_norm > 0]
        hhi_norm = float(np.sum(shares_norm ** 2))
        diversity = 1.0 - hhi_norm
        
        entropy_norm = (
            float(-np.sum(non_zero_norm * np.log(non_zero_norm)))
            if len(non_zero_norm) > 0
            else 0.0
        )

        year_value = int(row["Year"])
        if year_value < YEAR_MIN or year_value > YEAR_MAX:
            continue

        diversity_metrics.append(
            {
                f"{entity_label}_id": row[f"{entity_label}_id"],
                entity_label.capitalize(): row[entity_label.capitalize()],
                "Year": year_value,
                "shares_sum": shares_sum,
                "num_genres": int(len(non_zero_norm)),
                "diversity": diversity,
                "entropy_norm": entropy_norm,
            }
        )

    return pd.DataFrame(diversity_metrics)


def compute_yearly_averages(diversity_df, entity_label):
    metric_cols = ["diversity", "entropy_norm"]
    summary = (
        diversity_df.groupby("Year")[metric_cols + [f"{entity_label}_id"]]
        .agg({**{m: "mean" for m in metric_cols}, f"{entity_label}_id": "count"})
        .rename(columns={f"{entity_label}_id": f"num_{entity_label}s"})
        .reset_index()
    )
    return summary


def compute_age_profiles(diversity_df, entity_label):
    first_year = diversity_df.groupby(f"{entity_label}_id")["Year"].min()
    df_with_age = diversity_df.copy()
    df_with_age["Age"] = df_with_age["Year"] - df_with_age[f"{entity_label}_id"].map(
        first_year
    )
    df_with_age = df_with_age[(df_with_age["Age"] >= 0) & (df_with_age["Age"] <= AGE_MAX)]
    metric_cols = ["diversity", "entropy_norm"]
    summary = (
        df_with_age.groupby("Age")[metric_cols]
        .mean()
        .reset_index()
        .sort_values("Age")
    )
    return summary


def plot_diversity_series(series_by_threshold, entity_label, x_col, fig_name):
    fig, axes = plt.subplots(1, 2, figsize=(16, 6))
    for threshold, df in series_by_threshold.items():
        label = "All" if threshold is None else f">= {threshold} games"
        axes[0].plot(df[x_col], df["diversity"], label=label, linewidth=2)
        axes[1].plot(df[x_col], df["entropy_norm"], label=label, linewidth=2)

    axes[0].set_xlabel(x_col)
    axes[0].set_ylabel("Average Diversity (1 - HHI)")
    axes[0].set_title(f"{entity_label.capitalize()} Diversity (1 - HHI)")
    axes[0].grid(True, alpha=0.3)

    axes[1].set_xlabel(x_col)
    axes[1].set_ylabel("Average Entropy")
    axes[1].set_title(f"{entity_label.capitalize()} Entropy")
    axes[1].grid(True, alpha=0.3)

    axes[0].legend(fontsize=9)
    axes[1].legend(fontsize=9)

    fig.tight_layout()
    fig_path = FIGURES_DIR / fig_name
    fig.savefig(fig_path, dpi=300, bbox_inches="tight")
    plt.close(fig)
    return fig_path


def plot_comparison_series(dev_series, pub_series, x_col, fig_name, label_suffix):
    fig, axes = plt.subplots(1, 2, figsize=(16, 6))
    axes[0].plot(dev_series[x_col], dev_series["diversity"], label="Developers", linewidth=2)
    axes[0].plot(pub_series[x_col], pub_series["diversity"], label="Publishers", linewidth=2)
    axes[1].plot(dev_series[x_col], dev_series["entropy_norm"], label="Developers", linewidth=2)
    axes[1].plot(pub_series[x_col], pub_series["entropy_norm"], label="Publishers", linewidth=2)

    axes[0].set_xlabel(x_col)
    axes[0].set_ylabel("Average Diversity (1 - HHI)")
    axes[0].set_title(f"Diversity comparison ({label_suffix})")
    axes[0].grid(True, alpha=0.3)

    axes[1].set_xlabel(x_col)
    axes[1].set_ylabel("Average Entropy")
    axes[1].set_title(f"Entropy comparison ({label_suffix})")
    axes[1].grid(True, alpha=0.3)

    axes[0].legend(fontsize=9)
    axes[1].legend(fontsize=9)

    fig.tight_layout()
    fig_path = FIGURES_DIR / fig_name
    fig.savefig(fig_path, dpi=300, bbox_inches="tight")
    plt.close(fig)
    return fig_path


def build_combined_dataset(series_by_threshold, entity_label, x_col, metrics):
    frames = []
    for threshold, df in series_by_threshold.items():
        label = "All" if threshold is None else f">= {threshold} games"
        subset = df[[x_col] + metrics].copy()
        subset["entity"] = entity_label.capitalize()
        subset["threshold"] = np.nan if threshold is None else threshold
        subset["threshold_label"] = label
        frames.append(subset)
    return pd.concat(frames, ignore_index=True)


print("=" * 80)
print("Genre Diversity Analysis")
print("=" * 80)

print("\n1. Loading genre-share data...")
developer_data = pd.read_csv(DATA_DIR / "developer_genre_shares.csv")
publisher_data = pd.read_csv(DATA_DIR / "publisher_genre_shares.csv")
print(f"   Developer Data Shape: {developer_data.shape}")
print(f"   Publisher Data Shape: {publisher_data.shape}")

print("\n2. Extracting game counts from database...")
developer_counts = extract_company_game_counts("developers", "developer")
publisher_counts = extract_company_game_counts("publishers", "publisher")
print(f"   Developer total rows: {len(developer_counts)}")
print(f"   Publisher total rows: {len(publisher_counts)}")

dev_count_summary = summarize_game_counts(developer_counts)
pub_count_summary = summarize_game_counts(publisher_counts)

table_path = BASE_DIR / "tables" / "summary statistics" / "game_counts_by_type.tex"
write_game_counts_table(developer_counts, publisher_counts, table_path)
print(f"   Saved: {table_path}")

print("\n3. Calculating diversity metrics...")
developer_diversity = calculate_diversity_metrics(developer_data, "developer")
publisher_diversity = calculate_diversity_metrics(publisher_data, "publisher")
print(f"   Developer diversity rows: {len(developer_diversity)}")
print(f"   Publisher diversity rows: {len(publisher_diversity)}")

print("\n3a. Quick checks and summary snapshots...")
print("   Developer game counts summary:")
print(dev_count_summary.to_string(index=False))
print("   Publisher game counts summary:")
print(pub_count_summary.to_string(index=False))

for label, df in [("Developer", developer_diversity), ("Publisher", publisher_diversity)]:
    diversity_min = df["diversity"].min()
    diversity_max = df["diversity"].max()
    entropy_norm_min = df["entropy_norm"].min()
    entropy_norm_max = df["entropy_norm"].max()

    print(f"   {label} diversity (1-HHI) range: {diversity_min:.4f} to {diversity_max:.4f}")
    print(f"   {label} entropy range: {entropy_norm_min:.4f} to {entropy_norm_max:.4f}")

    if diversity_min < 0 or diversity_max > 1:
        print(f"   WARNING: {label} diversity outside [0, 1]")
    if entropy_norm_min < 0:
        print(f"   WARNING: {label} entropy below 0")

print("\n4. Computing yearly averages and firm-age profiles...")

def build_threshold_series(diversity_df, counts_df, entity_label):
    series_by_threshold = {}
    age_by_threshold = {}

    for threshold in THRESHOLDS:
        filtered = diversity_df
        if threshold is not None:
            filtered = filtered.merge(
                counts_df[[f"{entity_label}_id", "total_games"]],
                on=f"{entity_label}_id",
                how="left",
            )
            filtered = filtered[filtered["total_games"] >= threshold]

        yearly_summary = compute_yearly_averages(filtered, entity_label)
        age_summary = compute_age_profiles(filtered, entity_label)
        series_by_threshold[threshold] = yearly_summary
        age_by_threshold[threshold] = age_summary

    return series_by_threshold, age_by_threshold


dev_yearly_by_threshold, dev_age_by_threshold = build_threshold_series(
    developer_diversity, developer_counts, "developer"
)
pub_yearly_by_threshold, pub_age_by_threshold = build_threshold_series(
    publisher_diversity, publisher_counts, "publisher"
)

print("\n5. Creating diversity plots...")

plot_diversity_series(
    dev_yearly_by_threshold,
    "developer",
    "Year",
    "developer_diversity_yearly_norm.png",
)
plot_diversity_series(
    pub_yearly_by_threshold,
    "publisher",
    "Year",
    "publisher_diversity_yearly_norm.png",
)

plot_diversity_series(
    dev_age_by_threshold,
    "developer",
    "Age",
    "developer_diversity_age_norm.png",
)
plot_diversity_series(
    pub_age_by_threshold,
    "publisher",
    "Age",
    "publisher_diversity_age_norm.png",
)

print("\n6. Creating developer vs publisher comparisons...")
for threshold in COMPARE_THRESHOLDS:
    label = "all" if threshold is None else f"min_games_{threshold}"
    label_text = "All firms" if threshold is None else f">= {threshold} games"
    dev_yearly = dev_yearly_by_threshold[threshold]
    pub_yearly = pub_yearly_by_threshold[threshold]
    dev_age = dev_age_by_threshold[threshold]
    pub_age = pub_age_by_threshold[threshold]

    plot_comparison_series(
        dev_yearly,
        pub_yearly,
        "Year",
        f"comparison_diversity_yearly_norm_{label}.png",
        label_text,
    )
    plot_comparison_series(
        dev_age,
        pub_age,
        "Age",
        f"comparison_diversity_age_norm_{label}.png",
        label_text,
    )


print("\n7. Writing figure datasets...")
year_dataset = pd.concat(
    [
        build_combined_dataset(
            dev_yearly_by_threshold,
            "developer",
            "Year",
            ["diversity", "entropy_norm"],
        ),
        build_combined_dataset(
            pub_yearly_by_threshold,
            "publisher",
            "Year",
            ["diversity", "entropy_norm"],
        ),
    ],
    ignore_index=True,
)
age_dataset = pd.concat(
    [
        build_combined_dataset(
            dev_age_by_threshold,
            "developer",
            "Age",
            ["diversity", "entropy_norm"],
        ),
        build_combined_dataset(
            pub_age_by_threshold,
            "publisher",
            "Age",
            ["diversity", "entropy_norm"],
        ),
    ],
    ignore_index=True,
)
year_path = DATA_DIR / "diversity_year_norm.csv"
age_path = DATA_DIR / "diversity_age_norm.csv"
year_dataset.to_csv(year_path, index=False)
age_dataset.to_csv(age_path, index=False)
print(f"   Saved: {year_path}")
print(f"   Saved: {age_path}")

print("\n" + "=" * 80)
print("Analysis complete!")
print("=" * 80)
