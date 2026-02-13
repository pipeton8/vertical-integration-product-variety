# Plan: Genre Specialization Dataset with Verification

**TL;DR:** Extract games with their developer/publisher IDs and years from the SQLite JSON, join with genre vectors, validate data integrity at each step, then aggregate genre shares by entity and year. Pipeline: SQLite JSON parsing → validation → join with vectors → validation → aggregate → CSV export with checkpoints.

## Steps

### 1. Set up Python data pipeline with logging
   - Create script `create_specialization_dataset.py` in scripts folder
   - Import: `sqlite3`, `pandas`, `json`, `numpy`
   - Create a logging system to track verification results at each step
   - Define input/output paths and checkpoint log file

### 2. Extract and parse games with developer/publisher IDs from SQLite
   - Query all rows from `games` table (id, raw_data)
   - Parse JSON `raw_data` to extract:
     - `game_id` 
     - `title`
     - `release_date` from `platforms[].release_date` (use first platform's date, or earliest if multiple)
     - `developer_ids` (list of integers from `developers[].id`)
     - `publisher_ids` (list of integers from `publishers[].id`)
   - **Verification**: 
     - Count games extracted
     - Check games with NULL/missing develop_ids or publisher_ids
     - Check games with invalid/missing release years
     - Log summary statistics

### 3. Validate extracted IDs against lookup tables
   - Load `developers.csv` and `publishers.csv`
   - Cross-check: For each developer_id in games, verify it exists in developers.csv
   - Cross-check: For each publisher_id in games, verify it exists in publishers.csv
   - **Verification**:
     - Report unmatched developer IDs (orphaned records)
     - Report unmatched publisher IDs (orphaned records)
     - If mismatches found, decide: exclude games or use string matching as fallback

### 4. Load and validate genre vectors
   - Read `game_genre_vectors_none.csv` (first 100 rows to sample, then full load)
   - Verify structure: columns are `game_id`, `title`, `genre_0` through `genre_230`
   - Check data types: all genre columns should be binary (0 or 1)
   - **Verification**:
     - Count total games in CSV
     - Check for NULL values in genre columns
     - Verify all genre columns are numeric and binary (no values outside [0,1])
     - Sample check: display 5 random rows with their genre sums

### 5. Join games with genre vectors
   - Merge game metadata (from step 2) with genre vectors on `game_id`
   - Identify mismatches:
     - Games in database but not in vectors
     - Games in vectors but not in database
   - **Verification**:
     - Report row count before and after merge
     - Report unmatched games from each source
     - Verify no duplicates introduced
     - Sample row: print a game with its metadata and genre vector

### 6. Expand to developer rows (one row per game-developer pair)
   - For each game with multiple developers, create separate rows
   - Keep columns: `game_id`, `release_year`, `developer_id`, `genre_0`...`genre_230`
   - **Verification**:
     - Count rows before and after expansion
     - Verify no NULL developer_ids remain
     - Spot check: find a game with multiple developers, verify all are present

### 7. Expand to publisher rows (one row per game-publisher pair)
   - Repeat step 6 for publishers
   - **Verification**:
     - Count rows before and after expansion
     - Verify no NULL publisher_ids remain
     - Spot check: find a game with multiple publishers, verify all are present

### 8. Compute and validate genre shares by developer-year
   - Group by (`developer_id`, `release_year`)
   - For each group, compute mean of each genre column (0-230)
   - Join with `developers.csv` to get company names
   - **Verification**:
     - For a sample developer, manually verify computation: select one group, compute mean manually, compare
     - Check that genre means are in [0, 1] range
     - Check for NULL or NaN values in output
     - Verify all unique developers from input are in output (no lost data)
     - Count unique developer-year pairs

### 9. Compute and validate genre shares by publisher-year
   - Repeat step 8 for publishers
   - **Verification** (same as step 8 but for publishers):
     - Manual spot check on sample publisher
     - Range check [0,1]
     - NULL/NaN check
     - Completeness check

### 10. Create final output datasets
    - Developer dataset: columns = `developer_id`, `Developer`, `Year`, `genre_0_share`, ..., `genre_230_share`
    - Publisher dataset: columns = `publisher_id`, `Publisher`, `Year`, `genre_0_share`, ..., `genre_230_share`
    - Both: sort by year, then id.
    - Export to `developer_genre_shares.csv` and `publisher_genre_shares.csv`
    - **Verification**:
      - Check file sizes and row counts
      - Verify headers are correct
      - Sample rows: print first 5, last 5, and random 5 rows from each file
      - Year range check: verify years fall within expected range
      - No duplicate entity-year pairs

### 11. Generate final audit report
    - Summarize all verification steps in a text report
    - Include: games processed, developers/publishers recovered, year range, data loss at each step
    - Report any warnings (unmatched IDs, unexpected year ranges, etc.)
    - Save report as `specialization_audit_report.txt`

## Verification Checklist Across All Steps
- [ ] Developer/Publisher IDs in JSON match lookup tables
- [ ] No data loss during joins (track row counts)
- [ ] Genre vectors are binary and sum to integer counts per game
- [ ] Aggregated genre shares are in [0, 1] range
- [ ] No NULL/NaN values in final outputs (or logged if unavoidable)
- [ ] Manual spot checks on 3-5 entities
- [ ] Year ranges are reasonable (e.g., 1970-2025)

## Decisions
- **ID matching:** Use numeric IDs from JSON to link to lookup tables; flag any mismatches for review
- **Year extraction:** Use earliest release_date across platforms for each game
- **Aggregation:** Mean of binary genre vectors (gives prevalence 0-1)
- **Output:** Two CSV files (developers, publishers) with full genre breakdown per entity-year
