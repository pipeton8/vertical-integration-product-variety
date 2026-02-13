# Genre Diversity Outputs

This note documents the outputs produced by `scripts/genre_diversity_analysis.py`.

## Overview
The script combines two sources:
- Genre-share panels: `data/developer_genre_shares.csv` and `data/publisher_genre_shares.csv`
- Moby Games SQLite DB: `/Users/pipeton8/Library/CloudStorage/Dropbox/Research/_data/moby-games-data/moby_games.db`

It computes:
- Normalized diversity figures for developers and publishers by year and age
- Normalized comparison figures for developers vs publishers
- A LaTeX table with game-count means and standard deviations by threshold
- Two CSV datasets containing the points used in the figures

## Game Count Outputs
**LaTeX table**
- `tables/summary statistics/game_counts_by_type.tex`

## Diversification Outputs
Each dataset includes the points plotted in the corresponding figure and includes
`entity`, `threshold`, and `threshold_label` columns for the firm-size filters (all, >=2, >=5, >=10).

**Figures**
Developer diversity by year (normalized, all thresholds in one graph):
- `figures/genre distribution/developer_diversity_yearly_norm.png`

Developer diversity by age (normalized, all thresholds in one graph):
- `figures/genre distribution/developer_diversity_age_norm.png`

Publisher diversity by year (normalized, all thresholds in one graph):
- `figures/genre distribution/publisher_diversity_yearly_norm.png`

Publisher diversity by age (normalized, all thresholds in one graph):
- `figures/genre distribution/publisher_diversity_age_norm.png`

Developer vs publisher comparisons (normalized):
- `figures/genre distribution/comparison_diversity_yearly_norm_all.png`
- `figures/genre distribution/comparison_diversity_yearly_norm_min_games_5.png`
- `figures/genre distribution/comparison_diversity_age_norm_all.png`
- `figures/genre distribution/comparison_diversity_age_norm_min_games_5.png`

**Datasets used for figures**
- `data/diversity_year_norm.csv`
- `data/diversity_age_norm.csv`

## Notes
- Raw shares can sum above 1 because genre vectors are multi-label. Normalized metrics scale shares to sum to 1 per row.
- The yearly summaries are unweighted averages across firms in that year.
- The game counts are distinct game IDs from the Moby Games DB. Duplicate platforms for the same game are deduplicated at the game level.
