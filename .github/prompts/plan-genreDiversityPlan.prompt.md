## Plan: Expanded Diversity And Count Stats

This revision adds more detail per step and includes a verification for each step. It keeps your decisions: use the SQLite database for firm game counts, raw vs normalized shares for HHI/entropy, and firm age from first appearance in the genre-share panel.

**Steps**
1. Inspect the SQLite database schema at `/Users/pipeton8/Library/CloudStorage/Dropbox/Research/_data/moby-games-data/moby_games.db` to identify the firm–game link tables, the canonical game identifier (e.g., `app_id` or equivalent), and how developers/publishers are stored (names vs IDs).  
   **Verification:** Query the schema and list candidate tables; confirm a query that returns distinct game counts for a small sample of developers/publishers.

2. Implement an extraction step that computes distinct total games per firm (developers and publishers separately) using the DB, deduplicated by the canonical game ID.  
   **Verification:** Compare counts for a handful of known firms against expected magnitudes; ensure no duplicates across platform releases inflate counts.

3. Compute overall distribution stats (mean, Q1, median, Q3) for total games per firm using the cross-sectional list, not panel rows.  
   **Verification:** Output summary tables and spot-check that quartiles are ordered and nonnegative.

4. Compute yearly game-count distributions by firm (based on games released in that year), and generate boxplots by year for developers and publishers. Also export a yearly summary table (mean, Q1, median, Q3).  
   **Verification:** Ensure boxplots render with nonempty years and the summary table has the expected number of years and reasonable ranges.

5. Extend diversity metrics in [scripts/genre_diversity_analysis.py](scripts/genre_diversity_analysis.py) to compute both raw and normalized HHI/entropy from the genre-share panel:  
   - Raw: use shares as provided (row sums may exceed 1)  
   - Normalized: scale shares to sum to 1 per row (current behavior)  
   **Verification:** Check that normalized HHI is in $[0,1]$ and raw HHI can exceed 1; confirm entropy is nonnegative in both cases.

6. Create firm-size strata using total game counts (from Step 2): at least 2, 5, and 10 games. Recompute yearly averages for raw and normalized HHI/entropy within each stratum for both developers and publishers.  
   **Verification:** Confirm each stratum has fewer or equal firms than the previous, and that yearly time series exist for each threshold.

7. Build firm-age profiles from the genre-share panel: define age 0 as the first year the firm appears in the panel, then compute average raw and normalized HHI/entropy by age. Produce plots for developers and publishers, optionally layered by size thresholds.  
   **Verification:** Ensure age starts at 0, ages are contiguous for each firm, and the age-based series have plausible ranges and counts.

8. Write outputs to [data/](data/) and [figures/genre distribution/](figures/genre%20distribution/) with explicit filenames separating raw vs normalized and each threshold; update the script header to document the new outputs.  
   **Verification:** Confirm all files are written, and that rerunning the script reproduces the same outputs without errors.

**Decisions**
- Use the Moby Games SQLite DB as the source of distinct game counts per firm.
- Use raw vs normalized shares for “unnormalized” vs “normalized” HHI/entropy.
- Firm age is based on first appearance in the genre-share panel.
