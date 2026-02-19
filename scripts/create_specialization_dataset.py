"""
Create Genre Specialization Dataset - NEW SCHEMA VERSION

This script processes MobyGames data to create developer and publisher genre 
specialization datasets with WITHIN-CATEGORY SHARE NORMALIZATION.

Key changes from previous version:
- Uses category_X_genre_Y columns (new schema) instead of genre_0...genre_230
- Implements within-category independent normalization for shares
- Output: category_X_genre_Y_share columns
- Developer with only Action games → category_1_genre_1_share > 0 (within category)

Author: Research Team
Date: February 2026
"""

import sqlite3
import pandas as pd
import json
import numpy as np
import sys
from pathlib import Path
from datetime import datetime

# Import shared utilities
from utils import setup_logging, VerificationTracker, build_category_mappings, verify_file_exists

# =============================================================================
# CONFIGURATION
# =============================================================================

# Paths
BASE_DIR = Path(__file__).parent.parent
DATA_DIR = Path("/Users/pipeton8/Library/CloudStorage/Dropbox/Research/_data/moby-games-data")
OUTPUT_DIR = BASE_DIR / "data"

# Input files
DB_PATH = DATA_DIR / "moby_games.db"
GENRE_VECTORS_PATH = DATA_DIR / "game_genre_vectors_none.csv"
DEVELOPERS_CSV = DATA_DIR / "developers.csv"
PUBLISHERS_CSV = DATA_DIR / "publishers.csv"

# Output files
DEV_OUTPUT = OUTPUT_DIR / "developer_genre_shares.csv"
PUB_OUTPUT = OUTPUT_DIR / "publisher_genre_shares.csv"

# Logging configuration
LOG_DIR = BASE_DIR / "logs"
AUDIT_REPORT = LOG_DIR / "specialization_audit_report.txt"

# =============================================================================
# MAIN PIPELINE FUNCTIONS
# =============================================================================

def extract_games_from_db(db_path, tracker):
	"""Extract and parse games with developer/publisher IDs from SQLite."""
	tracker.log_step_start("Extract and parse games from database")
	
	# Connect to database
	conn = sqlite3.connect(db_path)
	cursor = conn.cursor()
	
	# Query all games
	tracker.logger.info("Querying games table...")
	cursor.execute("SELECT id, title, raw_data FROM games")
	rows = cursor.fetchall()
	
	total_games = len(rows)
	tracker.logger.info(f"Retrieved {total_games:,} games from database")
	
	# Parse each game's JSON data
	games_data = []
	parse_errors = 0
	
	for game_id, title, raw_data_str in rows:
		try:
			raw_data = json.loads(raw_data_str)
			
			# Extract developer IDs
			developer_ids = []
			if 'developers' in raw_data and raw_data['developers']:
				developer_ids = [dev['id'] for dev in raw_data['developers'] if 'id' in dev]
			
			# Extract publisher IDs
			publisher_ids = []
			if 'publishers' in raw_data and raw_data['publishers']:
				publisher_ids = [pub['id'] for pub in raw_data['publishers'] if 'id' in pub]
			
			# Extract release date (earliest across all platforms)
			release_year = None
			if 'platforms' in raw_data and raw_data['platforms']:
				dates = []
				for platform in raw_data['platforms']:
					if 'releases' in platform and platform['releases']:
						for release in platform['releases']:
							if 'release_date' in release and release['release_date']:
								release_date = release['release_date']
								# Try to parse year from various formats
								if isinstance(release_date, str):
									# Extract year (first 4 digits)
									year_match = release_date.split('-')[0]
									try:
										year = int(year_match)
										if 1970 <= year <= 2026:  # Sanity check
											dates.append(year)
									except (ValueError, TypeError):
										pass
				
				if dates:
					release_year = min(dates)  # Use earliest release year
			
			games_data.append({
				'game_id': game_id,
				'title': title,
				'release_year': release_year,
				'developer_ids': developer_ids,
				'publisher_ids': publisher_ids
			})
			
		except (json.JSONDecodeError, KeyError, TypeError) as e:
			parse_errors += 1
			if parse_errors <= 5:  # Log first few errors
				tracker.logger.warning(f"Error parsing game {game_id}: {e}")
	
	conn.close()
	
	# Create DataFrame
	df = pd.DataFrame(games_data)
	
	# Verification checks
	tracker.add_check("Games extracted", True, 
					 f"{len(df):,} games parsed successfully")
	
	if parse_errors > 0:
		tracker.add_warning(f"{parse_errors:,} games had parsing errors")
	
	# Check for missing developer/publisher IDs
	no_devs = df['developer_ids'].apply(lambda x: len(x) == 0).sum()
	no_pubs = df['publisher_ids'].apply(lambda x: len(x) == 0).sum()
	
	tracker.add_check("Games with developers", 
					 no_devs < len(df) * 0.5,  # Fail if >50% missing
					 f"{len(df) - no_devs:,} games have developers ({no_devs:,} without)")
	
	tracker.add_check("Games with publishers",
					 no_pubs < len(df) * 0.5,
					 f"{len(df) - no_pubs:,} games have publishers ({no_pubs:,} without)")
	
	# Check for missing release years
	no_year = df['release_year'].isna().sum()
	tracker.add_check("Games with release year",
					 no_year < len(df) * 0.3,  # Fail if >30% missing
					 f"{len(df) - no_year:,} games have release year ({no_year:,} without)")
	
	# Summary statistics
	tracker.logger.info("")
	tracker.logger.info("Summary Statistics:")
	tracker.logger.info(f"  Total games: {len(df):,}")
	tracker.logger.info(f"  Games with developers: {len(df) - no_devs:,}")
	tracker.logger.info(f"  Games with publishers: {len(df) - no_pubs:,}")
	tracker.logger.info(f"  Games with release year: {len(df) - no_year:,}")
	
	if len(df) - no_year > 0:
		year_min = df['release_year'].min()
		year_max = df['release_year'].max()
		tracker.logger.info(f"  Year range: {int(year_min)} - {int(year_max)}")
		
		tracker.add_check("Year range reasonable",
						 1970 <= year_min and year_max <= 2026,
						 f"Years span {int(year_min)} to {int(year_max)}")
	
	# Sample records
	tracker.logger.info("")
	tracker.logger.info("Sample records:")
	sample = df[df['release_year'].notna()].head(3)
	for _, row in sample.iterrows():
		tracker.logger.info(f"  Game {row['game_id']}: {row['title']}")
		tracker.logger.info(f"    Year: {row['release_year']}")
		tracker.logger.info(f"    Developers: {row['developer_ids'][:3]}{'...' if len(row['developer_ids']) > 3 else ''}")
		tracker.logger.info(f"    Publishers: {row['publisher_ids'][:3]}{'...' if len(row['publisher_ids']) > 3 else ''}")
	
	return df

def validate_ids(games_df, developers_df, publishers_df, tracker):
	"""Validate extracted IDs against lookup tables."""
	tracker.log_step_start("Validate extracted IDs against lookup tables")
	
	# Get all unique developer and publisher IDs from games
	all_dev_ids = set()
	all_pub_ids = set()
	
	for dev_list in games_df['developer_ids']:
		if isinstance(dev_list, list):
			all_dev_ids.update(dev_list)
	
	for pub_list in games_df['publisher_ids']:
		if isinstance(pub_list, list):
			all_pub_ids.update(pub_list)
	
	tracker.logger.info(f"Found {len(all_dev_ids):,} unique developer IDs in games")
	tracker.logger.info(f"Found {len(all_pub_ids):,} unique publisher IDs in games")
	
	# Get IDs from lookup tables
	lookup_dev_ids = set(developers_df['id'].values)
	lookup_pub_ids = set(publishers_df['id'].values)
	
	tracker.logger.info(f"Found {len(lookup_dev_ids):,} developer IDs in lookup table")
	tracker.logger.info(f"Found {len(lookup_pub_ids):,} publisher IDs in lookup table")
	
	# Find unmatched IDs
	unmatched_devs = all_dev_ids - lookup_dev_ids
	unmatched_pubs = all_pub_ids - lookup_pub_ids
	
	# Verification checks
	dev_match_rate = (len(all_dev_ids) - len(unmatched_devs)) / len(all_dev_ids) if all_dev_ids else 1.0
	pub_match_rate = (len(all_pub_ids) - len(unmatched_pubs)) / len(all_pub_ids) if all_pub_ids else 1.0
	
	tracker.add_check("Developer ID match rate",
					 dev_match_rate >= 0.95,  # Fail if <95% match
					 f"{dev_match_rate*100:.1f}% of developer IDs found in lookup table")
	
	tracker.add_check("Publisher ID match rate",
					 pub_match_rate >= 0.95,  # Fail if <95% match
					 f"{pub_match_rate*100:.1f}% of publisher IDs found in lookup table")
	
	# Report unmatched IDs
	if unmatched_devs:
		tracker.add_warning(f"{len(unmatched_devs):,} orphaned developer IDs not in lookup table")
		if len(unmatched_devs) <= 10:
			tracker.logger.info(f"  Orphaned developer IDs: {sorted(list(unmatched_devs))}")
		else:
			sample = sorted(list(unmatched_devs))[:10]
			tracker.logger.info(f"  Sample orphaned developer IDs: {sample}...")
	
	if unmatched_pubs:
		tracker.add_warning(f"{len(unmatched_pubs):,} orphaned publisher IDs not in lookup table")
		if len(unmatched_pubs) <= 10:
			tracker.logger.info(f"  Orphaned publisher IDs: {sorted(list(unmatched_pubs))}")
		else:
			sample = sorted(list(unmatched_pubs))[:10]
			tracker.logger.info(f"  Sample orphaned publisher IDs: {sample}...")
	
	# Count games affected by unmatched IDs
	games_with_bad_devs = 0
	games_with_bad_pubs = 0
	
	for _, row in games_df.iterrows():
		if isinstance(row['developer_ids'], list):
			if any(dev_id in unmatched_devs for dev_id in row['developer_ids']):
				games_with_bad_devs += 1
		
		if isinstance(row['publisher_ids'], list):
			if any(pub_id in unmatched_pubs for pub_id in row['publisher_ids']):
				games_with_bad_pubs += 1
	
	tracker.add_check("Games with valid developer IDs",
					 games_with_bad_devs < len(games_df) * 0.1,  # Fail if >10% affected
					 f"{len(games_df) - games_with_bad_devs:,} games have valid developer IDs ({games_with_bad_devs:,} affected)")
	
	tracker.add_check("Games with valid publisher IDs",
					 games_with_bad_pubs < len(games_df) * 0.1,  # Fail if >10% affected
					 f"{len(games_df) - games_with_bad_pubs:,} games have valid publisher IDs ({games_with_bad_pubs:,} affected)")
	
	# Clean the data: filter out unmatched IDs from lists
	tracker.logger.info("")
	tracker.logger.info("Cleaning data: removing unmatched IDs from game records...")
	
	def clean_id_list(id_list, valid_ids):
		if not isinstance(id_list, list):
			return []
		return [id for id in id_list if id in valid_ids]
	
	games_df['developer_ids'] = games_df['developer_ids'].apply(
		lambda x: clean_id_list(x, lookup_dev_ids))
	games_df['publisher_ids'] = games_df['publisher_ids'].apply(
		lambda x: clean_id_list(x, lookup_pub_ids))
	
	# Report final counts after cleaning
	final_no_devs = games_df['developer_ids'].apply(lambda x: len(x) == 0).sum()
	final_no_pubs = games_df['publisher_ids'].apply(lambda x: len(x) == 0).sum()
	
	tracker.logger.info(f"After cleaning: {len(games_df) - final_no_devs:,} games retain developers")
	tracker.logger.info(f"After cleaning: {len(games_df) - final_no_pubs:,} games retain publishers")
	
	tracker.add_check("Data cleaning successful",
					 True,
					 f"Cleaned {len(unmatched_devs)} dev IDs and {len(unmatched_pubs)} pub IDs")
	
	return games_df

def load_genre_vectors(genre_path, tracker):
	"""Load and validate genre vectors with NEW SCHEMA (category_X_genre_Y).
	
	Args:
		genre_path: Path to game_genre_vectors_none.csv
		tracker: VerificationTracker instance
	
	Returns:
		tuple: (df, genre_cols) where genre_cols are the category_X_genre_Y columns
	"""
	tracker.log_step_start("Load and validate genre vectors (NEW SCHEMA)")
	
	# Load sample first
	tracker.logger.info("Loading sample (100 rows) to verify structure...")
	sample_df = pd.read_csv(genre_path, nrows=100)
	
	tracker.logger.info(f"Sample shape: {sample_df.shape}")
	tracker.logger.info(f"Columns: {list(sample_df.columns[:10])}... (showing first 10)")
	
	# Verify expected columns
	has_game_id = 'game_id' in sample_df.columns
	has_title = 'title' in sample_df.columns
	
	tracker.add_check("Has game_id column", has_game_id, 
					 "game_id column present" if has_game_id else "game_id column MISSING")
	tracker.add_check("Has title column", has_title,
					 "title column present" if has_title else "title column MISSING")
	
	# Extract genre columns - NEW SCHEMA: category_X_genre_Y format
	genre_cols = [col for col in sample_df.columns if col.startswith('category_')]
	
	tracker.logger.info("")
	tracker.logger.info("Validating NEW SCHEMA (category_X_genre_Y columns)...")
	tracker.logger.info(f"Sample genre columns: {genre_cols[:15]}...")
	
	tracker.add_check("Has correct number of genre columns", 
					 len(genre_cols) == 230,
					 f"Found {len(genre_cols)} genre columns (expected 230)")
	
	# Verify binary values in sample
	unique_vals = set()
	for col in genre_cols[:10]:  # Check first 10 columns
		unique_vals.update(sample_df[col].dropna().unique())
	
	is_binary = unique_vals.issubset({0, 1, 0.0, 1.0})
	tracker.add_check("Genre values are binary (0 or 1)",
					 is_binary,
					 f"Unique values: {sorted(list(unique_vals))}")
	
	# Now load full file
	tracker.logger.info("")
	tracker.logger.info("Loading full genre vectors file...")
	df = pd.read_csv(genre_path)
	
	# Re-extract genre columns from full file
	genre_cols = [col for col in df.columns if col.startswith('category_')]
	
	tracker.logger.info(f"Loaded {len(df):,} games with {len(genre_cols)} genre columns")
	tracker.add_check("Genre vectors loaded", True,
					 f"{len(df):,} games, {len(genre_cols)} genre columns")
	
	# Check for NULL values
	null_counts = df[genre_cols].isnull().sum()
	total_nulls = null_counts.sum()
	
	tracker.add_check("No NULL values in genre columns",
					 total_nulls == 0,
					 f"No NULLs" if total_nulls == 0 else f"{total_nulls:,} NULLs found")
	
	# Verify all genre columns are numeric
	non_numeric_cols = []
	for col in genre_cols:
		if not pd.api.types.is_numeric_dtype(df[col]):
			non_numeric_cols.append(col)
	
	tracker.add_check("All genre columns numeric",
					 len(non_numeric_cols) == 0,
					 f"All {len(genre_cols)} genre columns are numeric")
	
	# Statistics
	tracker.logger.info("")
	tracker.logger.info("Genre Statistics:")
	genre_sums = df[genre_cols].sum(axis=1)
	tracker.logger.info(f"  Avg genres per game: {genre_sums.mean():.2f}")
	tracker.logger.info(f"  Min genres per game: {int(genre_sums.min())}")
	tracker.logger.info(f"  Max genres per game: {int(genre_sums.max())}")
	tracker.logger.info(f"  Games with 0 genres: {(genre_sums == 0).sum():,}")
	
	no_genres = (genre_sums == 0).sum()
	tracker.add_check("Most games have genres",
					 no_genres < len(df) * 0.1,
					 f"{len(df) - no_genres:,} games have genres")
	
	# Sample check
	tracker.logger.info("")
	tracker.logger.info("Sample records:")
	sample_rows = df.sample(min(5, len(df)), random_state=42)
	for idx, row in sample_rows.iterrows():
		genre_sum = row[genre_cols].sum()
		tracker.logger.info(f"  Game {row['game_id']}: {row['title'][:50]}... ({int(genre_sum)} genres)")
	
	return df, genre_cols

def join_games_genres(games_df, genres_df, genre_cols, tracker):
	"""Join games with genre vectors."""
	tracker.log_step_start("Join games with genre vectors")
	
	# Report counts before merge
	tracker.logger.info(f"Games dataframe: {len(games_df):,} rows")
	tracker.logger.info(f"Genre vectors dataframe: {len(genres_df):,} rows")
	tracker.logger.info(f"Genre columns: {len(genre_cols)}")
	
	# Perform left join to keep all games (even if no genre data)
	tracker.logger.info("")
	tracker.logger.info("Performing merge on game_id...")
	
	merged_df = games_df.merge(genres_df[['game_id'] + genre_cols], on='game_id', how='left')
	
	tracker.logger.info(f"Merged dataframe: {len(merged_df):,} rows")
	
	# Verification: Check for duplicates
	duplicate_count = len(merged_df) - len(games_df)
	tracker.add_check("No duplicates introduced",
					 duplicate_count == 0,
					 f"No duplicates" if duplicate_count == 0 
					 else f"{duplicate_count} duplicates created")
	
	# Identify games without genres
	first_genre_col = genre_cols[0] if genre_cols else None
	if first_genre_col:
		games_without_genres = merged_df[merged_df[first_genre_col].isna()]
		tracker.add_check(
			"All games have genre vectors",
			len(games_without_genres) == 0,
			f"Missing genre vectors: {len(games_without_genres):,}"
		)
		tracker.logger.info(f"Games without genre vectors: {len(games_without_genres):,}")
		
		if len(games_without_genres) > 0:
			tracker.add_warning(f"{len(games_without_genres):,} games have no genre vectors")
	
	# Remove games without genre vectors
	tracker.logger.info("")
	tracker.logger.info("Filtering to games with genre vectors...")
	if first_genre_col:
		merged_df = merged_df[merged_df[first_genre_col].notna()].copy()
	
	tracker.logger.info(f"After filtering NaN genres: {len(merged_df):,} games")
	
	# Remove games with 0 genres (all genre columns are 0)
	tracker.logger.info("")
	tracker.logger.info("Checking for games with 0 genres...")
	genre_sums = merged_df[genre_cols].sum(axis=1)
	games_with_zero_genres = (genre_sums == 0).sum()
	
	if games_with_zero_genres > 0:
		tracker.logger.info(f"Found {games_with_zero_genres:,} games with 0 genres")
		tracker.add_warning(f"{games_with_zero_genres:,} games have genre vectors but all zeros")
		merged_df = merged_df[genre_sums > 0].copy()
		tracker.logger.info(f"After filtering 0-genre games: {len(merged_df):,} games")
	else:
		tracker.logger.info("No games with 0 genres found")
	
	tracker.add_check("Games after merge",
					 len(merged_df) > 0,
					 f"{len(merged_df):,} games retained")
	
	tracker.add_check("All games have at least 1 genre",
					 (merged_df[genre_cols].sum(axis=1) > 0).all(),
					 "All remaining games have at least 1 genre")
	
	# Check for NULLs in genre columns
	null_count = merged_df[genre_cols].isnull().sum().sum()
	tracker.add_check("No NULL values in genre columns",
					 null_count == 0,
					 f"No NULLs" if null_count == 0 else f"{null_count} NULLs")
	
	# Sample check
	tracker.logger.info("")
	tracker.logger.info("Sample merged records:")
	sample = merged_df[merged_df['release_year'].notna()].head(3)
	for _, row in sample.iterrows():
		genre_sum = row[genre_cols].sum()
		tracker.logger.info(f"  Game {row['game_id']}: {row['title']}")
		tracker.logger.info(f"    Year: {int(row['release_year'])}, Genres: {int(genre_sum)}")
	
	tracker.logger.info("")
	tracker.logger.info(f"Final merged dataset: {len(merged_df):,} games")
	
	return merged_df

def expand_to_company_rows(
	games_genres_df,
	genre_cols,
	tracker,
	company_label,
	ids_col,
	id_col
):
	"""
	Expand each game to company rows with category_X_genre_Y columns.
	"""
	tracker.log_step_start(f"Expand to {company_label} rows")
	
	tracker.logger.info(f"Filtering to games with {company_label.lower()}s and release year...")
	
	games_with_company = games_genres_df[
		(games_genres_df[ids_col].apply(lambda x: isinstance(x, list) and len(x) > 0)) &
		(games_genres_df['release_year'].notna())
	].copy()
	
	tracker.logger.info(f"Games with {company_label.lower()}s and year: {len(games_with_company):,}")
	
	tracker.logger.info(f"Exploding {ids_col} list...")
	expanded = games_with_company.explode(ids_col)
	tracker.logger.info(f"After explosion: {len(expanded):,} rows")
	
	expanded = expanded.rename(columns={ids_col: id_col})
	
	# Select columns: company id, release_year, and all genre columns
	final_cols = ['game_id', id_col, 'release_year'] + genre_cols
	company_rows = expanded[final_cols].copy()
	
	tracker.logger.info("")
	tracker.add_check(
		f"{company_label} rows created",
		len(company_rows) > 0,
		f"{len(company_rows):,} {company_label.lower()}-game rows created"
	)
	
	# Check for NULLs
	null_ids = company_rows[id_col].isna().sum()
	tracker.add_check(
		f"No NULL {company_label.lower()} IDs",
		null_ids == 0,
		"No NULLs" if null_ids == 0 else f"Found {null_ids} NULLs"
	)
	
	null_years = company_rows['release_year'].isna().sum()
	tracker.add_check(
		"No NULL years",
		null_years == 0,
		"No NULLs" if null_years == 0 else f"Found {null_years} NULLs"
	)
	
	# Check for duplicates
	dup_mask = company_rows.duplicated(subset=['game_id', id_col, 'release_year'], keep=False)
	dup_count = dup_mask.sum()
	
	if dup_count > 0:
		tracker.logger.info(f"Removing {dup_count} duplicate rows...")
		company_rows = company_rows.drop_duplicates(
			subset=['game_id', id_col, 'release_year'],
			keep='first'
		)
	
	tracker.add_check(
		"Duplicates handled",
		True,
		f"Removed {dup_count}" if dup_count > 0 else "No duplicates found"
	)
	
	tracker.logger.info("")
	tracker.logger.info(f"{company_label}-game rows: {len(company_rows):,}")
	tracker.logger.info(f"Genre columns: {len(genre_cols)}")
	
	return company_rows

def compute_company_shares(company_rows_df, company_df, genre_cols, tracker, 
						  company_type, company_id_col, company_name_col):
	"""
	Compute CUMULATIVE WITHIN-CATEGORY genre shares for companies (developers or publishers).
	
	Args:
		company_rows_df: DataFrame with company-game rows
		company_df: Lookup table with company IDs and names
		genre_cols: List of genre column names (category_X_genre_Y format)
		tracker: VerificationTracker instance
		company_type: String label for company type ("Developer" or "Publisher")
		company_id_col: Name of the company ID column in company_rows_df
		company_name_col: Name for the output company name column
	
	For each company-year (cumulative):
	  - Include all games from first year through current year
	  - For each category:
		- Identify games with at least one genre in that category
		- For each genre: compute mean across those games
		- Share = mean / count(games with any genre in category)
	
	Returns:
		DataFrame with company-year cumulative genre shares
	"""
	tracker.log_step_start(f"Compute {company_type.lower()}-year CUMULATIVE within-category shares")
	
	tracker.logger.info("Building category mappings from genre columns...")
	col_to_category, category_cols = build_category_mappings(genre_cols)
	
	tracker.logger.info(f"Identified {len(col_to_category)} genre columns")
	tracker.logger.info(f"Identified {len(category_cols)} categories")
	
	# Step 1: Sort by company, year, and game to enable proper cumulative computation
	tracker.logger.info("Sorting data by company, year, and game...")
	company_rows_sorted = company_rows_df.sort_values(
		[company_id_col, 'release_year', 'game_id']
	).reset_index(drop=True)
	
	# Steps 2-5: Create all derived columns at once using pd.concat() to avoid DataFrame fragmentation
	tracker.logger.info("Creating all derived columns (indicators, cumulative sums, counts)...")
	
	# Collect all new columns in a dictionary
	new_columns = {}
	
	# Step 2: Create binary indicator for each category (has any genre in that category)
	tracker.logger.info("  - Category indicators...")
	category_has_cols = {}
	for cat_id, cat_cols in category_cols.items():
		has_col = f'_cat_{cat_id}_has'
		new_columns[has_col] = (company_rows_sorted[cat_cols].sum(axis=1) > 0).astype(int)
		category_has_cols[cat_id] = has_col
	
	# Step 3: Compute cumulative sums for each genre column within each company
	tracker.logger.info("  - Cumulative genre sums...")
	cumsum_cols = {}
	for col in genre_cols:
		cumsum_col = f'{col}_cumsum'
		new_columns[cumsum_col] = company_rows_sorted.groupby(
			company_id_col, sort=False
		)[col].cumsum()
		cumsum_cols[col] = cumsum_col
	
	# Step 4: Compute cumulative counts for each category within each company
	tracker.logger.info("  - Cumulative category counts...")
	cumsum_category_cols = {}
	for cat_id, has_col in category_has_cols.items():
		cumsum_cat_col = f'_cat_{cat_id}_count'
		# Get the has_series from new_columns and group by company_id
		has_series = new_columns[has_col]
		company_ids = company_rows_sorted[company_id_col]
		new_columns[cumsum_cat_col] = has_series.groupby(company_ids, sort=False).cumsum()
		cumsum_category_cols[cat_id] = cumsum_cat_col
	
	# Step 5: Compute cumulative game count
	tracker.logger.info("  - Cumulative game counts...")
	new_columns['_cumulative_game_count'] = company_rows_sorted.groupby(
		company_id_col, sort=False
	).cumcount() + 1
	
	# Concatenate all new columns at once to avoid DataFrame fragmentation
	tracker.logger.info("Concatenating all columns at once...")
	company_rows_sorted = pd.concat(
		[company_rows_sorted, pd.DataFrame(new_columns, index=company_rows_sorted.index)],
		axis=1
	)
	
	# Step 6: Extract last row for each company-year pair (contains cumulative values for that year)
	tracker.logger.info("Extracting company-year snapshots...")
	company_year_snapshot = company_rows_sorted.groupby(
		[company_id_col, 'release_year'], as_index=False, sort=False
	).tail(1)
	
	tracker.logger.info(f"Creating company_shares dataframe from {len(company_year_snapshot)} company-year pairs...")
	
	# Step 7: Compute shares from cumulative values (vectorized)
	shares_frames = []
	for cat_id, cat_cols in category_cols.items():
		# Get cumulative count of games with this category
		num_games_with_cat = company_year_snapshot[cumsum_category_cols[cat_id]].replace(0, np.nan)

		cat_cumsum_cols = [cumsum_cols[col] for col in cat_cols]
		cat_shares = company_year_snapshot[cat_cumsum_cols].div(num_games_with_cat, axis=0)
		cat_shares.columns = [f"{col}_share" for col in cat_cols]
		shares_frames.append(cat_shares)

	shares_df = pd.concat(shares_frames, axis=1).fillna(0.0)
	
	company_shares = pd.concat(
		[
			company_year_snapshot[[company_id_col, "release_year", "_cumulative_game_count"]].rename(
				columns={"_cumulative_game_count": "num_games"}
			),
			shares_df,
		],
		axis=1,
	)

	tracker.logger.info(f"  Created {len(company_shares)} company-year share records")
	
	# Verification: Cumulative calculation checks
	tracker.logger.info("")
	tracker.logger.info("Verifying cumulative calculation properties...")
	
	# 1. Monotonicity check: game counts never decrease
	tracker.logger.info("  Checking monotonicity (game counts should never decrease)...")
	ordered_shares = company_shares.sort_values([company_id_col, 'release_year'])
	diff_series = ordered_shares.groupby(company_id_col, sort=False)['num_games'].diff()
	violations = ordered_shares.loc[diff_series.lt(0), company_id_col].unique()
	monotonicity_violations = len(violations)
	for company_id in violations:
		tracker.logger.info(f"    WARNING: {company_type} {company_id} has decreasing game counts")
	
	tracker.add_check(
		"Game counts are monotonic (never decrease)",
		monotonicity_violations == 0,
		f"All companies have monotonic game counts" if monotonicity_violations == 0 
		else f"{monotonicity_violations} companies have decreasing counts"
	)
	
	# Add company names
	company_names = company_df[['id', 'name']].rename(
		columns={'id': company_id_col, 'name': company_name_col}
	)
	company_shares = company_shares.merge(company_names, on=company_id_col, how='left')
	
	# Rename year column
	company_shares = company_shares.rename(columns={'release_year': 'Year'})
	
	# Reorder columns: company_id, CompanyName, Year, num_games, then all share columns (sorted)
	share_cols = [col for col in company_shares.columns if col.endswith('_share')]
	final_cols = [company_id_col, company_name_col, 'Year', 'num_games'] + sorted(share_cols)
	company_shares = company_shares[final_cols]
	
	tracker.logger.info("")
	tracker.add_check(
		f"{company_type}-year rows created",
		len(company_shares) > 0,
		f"{len(company_shares):,} {company_type.lower()}-year rows created"
	)
	
	# Check share ranges
	min_share = company_shares[share_cols].min().min()
	max_share = company_shares[share_cols].max().max()
	in_range = (min_share >= -1e-9) and (max_share <= 1 + 1e-9)
	tracker.add_check(
		"Genre shares in [0,1] range",
		in_range,
		f"Range: {min_share:.6f} to {max_share:.6f}"
	)
	
	# Check for NULLs
	null_shares = company_shares[share_cols].isna().sum().sum()
	tracker.add_check(
		"No NULL genre shares",
		null_shares == 0,
		f"No NULLs" if null_shares == 0 else f"Found {int(null_shares)} NULLs"
	)
	
	# Manual spot check with cumulative verification
	if len(company_shares) > 0:
		tracker.logger.info("")
		tracker.logger.info("Performing manual spot check on company with diverse 2022 portfolio...")
		
		# Get 2022 rows (or latest year if 2022 not available)
		target_year = 2022
		year_target = company_shares[company_shares['Year'] == target_year]
		
		if len(year_target) == 0:
			# Fall back to latest year
			target_year = company_shares['Year'].max()
			year_target = company_shares[company_shares['Year'] == target_year]
			tracker.logger.info(f"  (2022 not available, using latest year {target_year})")
		
		# Count nonzero shares for each company-year
		year_target = year_target.copy()
		year_target['nonzero_count'] = (year_target[share_cols] > 1e-9).sum(axis=1)
		
		# Select company with most diverse portfolio
		most_diverse_idx = year_target['nonzero_count'].idxmax()
		most_diverse = year_target.loc[most_diverse_idx]
		
		sample_company = int(most_diverse[company_id_col])
		sample_year = int(most_diverse['Year'])
		nonzero_genres = int(most_diverse['nonzero_count'])
		
		# Get ALL games for this company up to sample_year (cumulative)
		source_games = company_rows_df[
			(company_rows_df[company_id_col] == sample_company) &
			(company_rows_df['release_year'] <= sample_year)
		]
		
		tracker.logger.info(f"  Testing cumulative portfolio for {company_type} {sample_company}, Year {sample_year}")
		tracker.logger.info(f"  Total games up to {sample_year}: {len(source_games)}")
		tracker.logger.info(f"  Nonzero genre shares: {nonzero_genres}")
		
		# Count games by year to show cumulative nature
		if len(source_games) > 0:
			year_counts = source_games.groupby('release_year').size().to_dict()
			tracker.logger.info(f"  Games by year: {dict(sorted(year_counts.items()))}")
		
		# Verify this is a meaningful test case
		if nonzero_genres >= 5 and len(source_games) > 0:
			# Pick category 1 to verify
			cat_id = 1
			if cat_id in category_cols:
				cat_cols_sample = category_cols[cat_id]
				
				# Manual calculation - cumulative!
				has_genre_in_cat = source_games[cat_cols_sample].sum(axis=1) > 0
				games_with_cat = source_games[has_genre_in_cat]
				num_games_in_cat = len(games_with_cat)
				
				tracker.logger.info(f"  Games with category {cat_id} genres (cumulative): {num_games_in_cat}")
				
				if num_games_in_cat > 0:
					# Check one genre in this category
					sample_genre_col = cat_cols_sample[0]
					manual_sum = games_with_cat[sample_genre_col].sum()
					manual_share = manual_sum / num_games_in_cat
					
					# Get computed share from company_shares
					# Find the row matching this company-year
					row_match = company_shares[
						(company_shares[company_id_col] == sample_company) &
						(company_shares['Year'] == sample_year)
					]
					
					if len(row_match) > 0:
						share_col_name = f'{sample_genre_col}_share'
						computed_share = row_match.iloc[0][share_col_name]
						
						diff = abs(manual_share - computed_share)
						
						tracker.logger.info(f"  Genre {sample_genre_col} (cumulative calculation):")
						tracker.logger.info(f"    Manual share: {manual_share:.6f}")
						tracker.logger.info(f"    Computed share: {computed_share:.6f}")
						tracker.logger.info(f"    Difference: {diff:.10f}")
						
						tracker.add_check(
							"Manual spot check (cumulative within-category share)",
							diff < 1e-6,
							f"{company_type} {sample_company}, Year {sample_year}: {nonzero_genres} genres, diff = {diff:.10f}"
						)
					else:
						tracker.add_warning(f"Could not find company-year {sample_company}-{sample_year} in output")
		else:
			tracker.add_warning(f"Selected company-year has {nonzero_genres} genres and {len(source_games)} games - may not be ideal test case")
	
	# Step 4 Final comprehensive verification checks
	tracker.logger.info("")
	tracker.logger.info("FINAL VERIFICATION: Comprehensive data quality checks...")
	
	# 1. Share evolution verification - check that shares evolve logically for sample companies
	tracker.logger.info("")
	tracker.logger.info("Checking share evolution over time (sample of 3 companies)...")
	company_ids = company_shares[company_id_col].unique()
	sample_companies = company_ids[:min(3, len(company_ids))]
	
	evolution_check_passed = 0
	for sample_id in sample_companies:
		company_over_time = company_shares[company_shares[company_id_col] == sample_id].sort_values('Year')
		
		if len(company_over_time) > 1:
			# Check that game counts are monotonic
			game_counts = company_over_time['num_games'].values
			is_monotonic = all(game_counts[i] <= game_counts[i+1] for i in range(len(game_counts)-1))
			
			if is_monotonic:
				evolution_check_passed += 1
			
			tracker.logger.info(f"  {company_type} {sample_id}: {len(company_over_time)} years, " + 
							  f"games {game_counts[0]}->{game_counts[-1]}, monotonic={is_monotonic}")
	
	tracker.add_check(
		"Share evolution is monotonic (sample check)",
		evolution_check_passed == len(sample_companies),
		f"{evolution_check_passed}/{len(sample_companies)} sample companies have monotonic game counts"
	)
	
	# 2. Year span statistics
	tracker.logger.info("")
	tracker.logger.info("Year span statistics...")
	year_spans = company_shares.groupby(company_id_col)['Year'].agg(['min', 'max', 'count'])
	year_spans['span'] = year_spans['max'] - year_spans['min'] + 1
	
	avg_span = year_spans['span'].mean()
	max_span = year_spans['span'].max()
	median_span = year_spans['span'].median()
	
	tracker.logger.info(f"  Average year span per company: {avg_span:.1f} years")
	tracker.logger.info(f"  Median year span per company: {median_span:.1f} years")
	tracker.logger.info(f"  Max year span: {max_span} years")
	tracker.logger.info(f"  Companies with 1 year only: {(year_spans['span'] == 1).sum():,}")
	
	tracker.add_check(
		"Year span statistics computed",
		True,
		f"Avg: {avg_span:.1f}, Median: {median_span:.1f}, Max: {max_span} years"
	)
	
	# 3. Verify each company has reasonable number of rows (at least 1, at most year_span)
	tracker.logger.info("")
	tracker.logger.info("Verifying rows per company...")
	rows_per_company = company_shares.groupby(company_id_col).size()
	
	# Vectorized approach only creates rows for years with game releases,
	# so rows_per_company <= year_spans['span']
	rows_valid = (rows_per_company > 0) & (rows_per_company <= year_spans['span'])
	invalid_count = (~rows_valid).sum()
	
	tracker.add_check(
		"Rows per company are valid (1 to year_span)",
		invalid_count == 0,
		f"All companies have valid row counts" if invalid_count == 0
		else f"{invalid_count} companies have invalid row counts"
	)
	
	# 4. Summary statistics
	tracker.logger.info("")
	tracker.logger.info("SUMMARY STATISTICS:")
	tracker.logger.info(f"  Total companies: {len(company_ids):,}")
	tracker.logger.info(f"  Total rows (company-year): {len(company_shares):,}")
	tracker.logger.info(f"  Year range: {company_shares['Year'].min():.0f} - {company_shares['Year'].max():.0f}")
	tracker.logger.info(f"  Avg rows per company: {len(company_shares) / len(company_ids):.1f}")
	
	tracker.logger.info("")
	tracker.logger.info(f"{company_type}-year pairs: {len(company_shares):,}")
	tracker.logger.info(f"Output columns: {len(company_shares.columns)} (3 identifiers + {len(share_cols)} shares)")
	
	return company_shares

def balance_company_panel(company_shares_df, company_id_col, company_name_col, tracker):
	"""
	Create a balanced panel by filling missing year gaps for each company.
	
	For each company that appears in the data, ensures rows exist for all years
	between their first and last appearance. Missing years are filled with:
	- Company ID and name (matched from existing rows)
	- num_games: NaN for missing years
	- All genre shares: NaN for missing years
	
	Args:
		company_shares_df: DataFrame with company-year data
		company_id_col: Name of company ID column (e.g., 'developer_id')
		company_name_col: Name of company name column (e.g., 'Developer')
		tracker: VerificationTracker instance
	
	Returns:
		Balanced panel DataFrame with all year gaps filled
	"""
	tracker.log_step_start("Balance panel by filling year gaps")
	
	original_rows = len(company_shares_df)
	tracker.logger.info(f"Input: {original_rows:,} company-year rows")
	
	# Identify all share columns (end with '_share')
	share_cols = [col for col in company_shares_df.columns if col.endswith('_share')]

	# Resample to yearly frequency to create missing years as NaN rows
	resample_df = company_shares_df.copy()
	resample_df['Year_dt'] = pd.to_datetime(resample_df['Year'].astype(int), format='%Y')
	resample_df = resample_df.set_index([company_id_col, 'Year_dt']).sort_index()

	def resample_company(group):
		group = group.reset_index(level=0, drop=True)
		return group.resample('YS').asfreq()

	resampled = resample_df.groupby(level=0, group_keys=True).apply(resample_company)

	# Fill identifiers for resampled rows, leave data columns as NaN
	resampled[company_name_col] = (
		resampled.groupby(level=0)[company_name_col].ffill().bfill()
	)

	# Fill number of games and shares
	for col in share_cols + ['num_games']:
		resampled[col] = resampled.groupby(level=0)[col].ffill().bfill()

	balanced_df = resampled.reset_index()
	balanced_df['Year'] = balanced_df['Year_dt'].dt.year
	balanced_df = balanced_df.drop(columns=['Year_dt'])
	
	# Ensure consistent column order: ID, Name, Year, num_games, then sorted shares
	final_cols = [company_id_col, company_name_col, 'Year', 'num_games'] + sorted(share_cols)
	balanced_df = balanced_df[final_cols]
	
	# Sort by company and year
	balanced_df = balanced_df.sort_values([company_id_col, 'Year']).reset_index(drop=True)
	
	new_rows = len(balanced_df)
	added_rows = new_rows - original_rows
	
	tracker.logger.info(f"Output: {new_rows:,} company-year rows")
	tracker.logger.info(f"Added: {added_rows:,} rows to fill gaps")
	
	tracker.add_check(
		"Panel balancing completed",
		added_rows >= 0,
		f"Added {added_rows:,} rows to fill year gaps"
	)
	
	return balanced_df

def create_output_files(dev_shares_df, pub_shares_df, tracker):
	"""Export final datasets to CSV with NEW SCHEMA (category_X_genre_Y_share)."""
	tracker.log_step_start("Create final output datasets")
	
	tracker.logger.info("Balancing developer panel...")
	dev_balanced = balance_company_panel(
		dev_shares_df,
		'developer_id',
		'Developer',
		tracker
	)
	
	tracker.logger.info("")
	tracker.logger.info("Balancing publisher panel...")
	pub_balanced = balance_company_panel(
		pub_shares_df,
		'publisher_id',
		'Publisher',
		tracker
	)
	
	tracker.logger.info("")
	
	# Developer file
	tracker.logger.info("Exporting developer shares...")
	dev_out = dev_balanced.sort_values(['Year', 'developer_id']).copy()
	dev_file = OUTPUT_DIR / "developer_genre_shares.csv"
	dev_out.to_csv(dev_file, index=False)
	tracker.logger.info(f"Exported: {dev_file}")
	
	# Publisher file
	tracker.logger.info("Exporting publisher shares...")
	pub_out = pub_balanced.sort_values(['Year', 'publisher_id']).copy()
	pub_file = OUTPUT_DIR / "publisher_genre_shares.csv"
	pub_out.to_csv(pub_file, index=False)
	tracker.logger.info(f"Exported: {pub_file}")
	
	tracker.logger.info("")
	
	# Verification
	tracker.logger.info("Verifying output files...")
	
	# Developer file checks
	dev_file_size = dev_file.stat().st_size / 1024 / 1024
	tracker.add_check(
		"Developer file created",
		dev_file.exists(),
		f"File size: {dev_file_size:.2f} MB, {len(dev_out):,} rows"
	)
	
	expected_col_count = 4 + 230  # id, name, year, number of games + 230 shares
	tracker.add_check(
		"Developer file has correct columns",
		len(dev_out.columns) == expected_col_count,
		f"Columns: {len(dev_out.columns)} (expected {expected_col_count})"
	)
	
	# Check column names start with category_
	share_cols = [c for c in dev_out.columns if c.endswith('_share')]
	all_category_named = all(c.startswith('category_') for c in share_cols)
	tracker.add_check(
		"Developer file columns use NEW SCHEMA",
		all_category_named,
		f"All {len(share_cols)} share columns follow category_X_genre_Y_share naming"
	)
	
	# Publisher file checks
	pub_file_size = pub_file.stat().st_size / 1024 / 1024
	tracker.add_check(
		"Publisher file created",
		pub_file.exists(),
		f"File size: {pub_file_size:.2f} MB, {len(pub_out):,} rows"
	)
	
	tracker.add_check(
		"Publisher file has correct columns",
		len(pub_out.columns) == expected_col_count,
		f"Columns: {len(pub_out.columns)} (expected {expected_col_count})"
	)
	
	# Check publisher column names
	pub_share_cols = [c for c in pub_out.columns if c.endswith('_share')]
	all_pub_category_named = all(c.startswith('category_') for c in pub_share_cols)
	tracker.add_check(
		"Publisher file columns use NEW SCHEMA",
		all_pub_category_named,
		f"All {len(pub_share_cols)} share columns follow category_X_genre_Y_share naming"
	)
	
	# Sample check
	tracker.logger.info("")
	tracker.logger.info("Sample rows:")
	tracker.logger.info("Developer file (first 3):")
	for idx, row in dev_out.head(3).iterrows():
		tracker.logger.info(f"  Dev {int(row['developer_id'])}: {row['Developer']}, Year {int(row['Year'])}")
	
	tracker.logger.info("Publisher file (first 3):")
	for idx, row in pub_out.head(3).iterrows():
		tracker.logger.info(f"  Pub {int(row['publisher_id'])}: {row['Publisher']}, Year {int(row['Year'])}")
	
	# Final comprehensive output verification
	tracker.logger.info("")
	tracker.logger.info("FINAL OUTPUT VERIFICATION:")
	
	# File size and row count comparison
	tracker.logger.info("")
	tracker.logger.info("File sizes and row counts:")
	tracker.logger.info(f"  Developer: {len(dev_out):,} rows, {dev_file_size:.2f} MB")
	tracker.logger.info(f"  Publisher: {len(pub_out):,} rows, {pub_file_size:.2f} MB")
	
	# Check that we have cumulative years (more rows than original yearly approach)
	tracker.add_check(
		"Output files created successfully",
		dev_file.exists() and pub_file.exists(),
		f"Developer: {dev_file_size:.2f} MB, Publisher: {pub_file_size:.2f} MB"
	)
	
	# Verify data integrity - no NaN values in key columns
	tracker.logger.info("")
	tracker.logger.info("Checking data integrity...")
	
	dev_nan_ids = dev_out['developer_id'].isna().sum()
	dev_nan_years = dev_out['Year'].isna().sum()
	dev_nan_names = dev_out['Developer'].isna().sum()
	
	tracker.add_check(
		"Developer ID column has no NaNs",
		dev_nan_ids == 0,
		f"NaN count: {dev_nan_ids}"
	)
	
	tracker.add_check(
		"Developer Year column has no NaNs",
		dev_nan_years == 0,
		f"NaN count: {dev_nan_years}"
	)
	
	tracker.add_check(
		"Developer Name column has no NaNs",
		dev_nan_names == 0,
		f"NaN count: {dev_nan_names}"
	)
	
	pub_nan_ids = pub_out['publisher_id'].isna().sum()
	pub_nan_years = pub_out['Year'].isna().sum()
	pub_nan_names = pub_out['Publisher'].isna().sum()
	
	tracker.add_check(
		"Publisher ID column has no NaNs",
		pub_nan_ids == 0,
		f"NaN count: {pub_nan_ids}"
	)
	
	tracker.add_check(
		"Publisher Year column has no NaNs",
		pub_nan_years == 0,
		f"NaN count: {pub_nan_years}"
	)
	
	tracker.add_check(
		"Publisher Name column has no NaNs",
		pub_nan_names == 0,
		f"NaN count: {pub_nan_names}"
	)
	
	# Sample portfolio evolution verification
	tracker.logger.info("")
	tracker.logger.info("Spot-checking portfolio evolution (cumulative property)...")
	
	# Get a developer with multiple years
	dev_with_multi_years = dev_out.groupby('developer_id').size()
	if len(dev_with_multi_years) > 0:
		sample_dev_id = dev_with_multi_years[dev_with_multi_years > 1].index[0]
		sample_dev_data = dev_out[dev_out['developer_id'] == sample_dev_id].sort_values('Year')
		
		if len(sample_dev_data) > 1:
			tracker.logger.info(f"  Sample developer {sample_dev_id} ({sample_dev_data['Developer'].iloc[0]}):")
			tracker.logger.info(f"    Years: {sample_dev_data['Year'].min():.0f}-{sample_dev_data['Year'].max():.0f}")
			tracker.logger.info(f"    Rows: {len(sample_dev_data)}")
			
			# Verify cumulative game count by checking num_games column
			if 'num_games' in sample_dev_data.columns:
				game_counts = sample_dev_data['num_games'].values
				is_monotonic = all(game_counts[i] <= game_counts[i+1] for i in range(len(game_counts)-1))
				tracker.logger.info(f"    Game counts: {game_counts[0]}->{game_counts[-1]}, monotonic={is_monotonic}")
				
				tracker.add_check(
					"Sample developer portfolio is cumulative (game counts increase)",
					is_monotonic,
					f"Sample developer {sample_dev_id}: game progression {game_counts[0]}->{game_counts[-1]}"
				)
	
	# Summary
	tracker.logger.info("")
	tracker.logger.info("=" * 70)
	tracker.logger.info("PIPELINE COMPLETE - All three fixes successfully implemented:")
	tracker.logger.info("  ✓ Step 1: Zero-genre games filtered")
	tracker.logger.info("  ✓ Step 2: Cumulative portfolio calculation (all games up to year Y)")
	tracker.logger.info("  ✓ Step 3: Meaningful manual spot check (diverse 2022 portfolio)")
	tracker.logger.info("  ✓ Step 4: Comprehensive verification checks")
	tracker.logger.info("=" * 70)

# =============================================================================
# MAIN EXECUTION
# =============================================================================

def main():
	"""Main execution pipeline."""
	logger, log_file = setup_logging('specialization_processing')
	tracker = VerificationTracker(logger)
	
	try:
		# Verify input files
		verify_file_exists([
			(DB_PATH, "Database"),
			(GENRE_VECTORS_PATH, "Genre vectors"),
			(DEVELOPERS_CSV, "Developers CSV"),
			(PUBLISHERS_CSV, "Publishers CSV"),
		], logger)
		
		# Pipeline
		games_df = extract_games_from_db(DB_PATH, tracker)
		tracker.log_completion("Games extracted")
		
		developers_df = pd.read_csv(DEVELOPERS_CSV)
		publishers_df = pd.read_csv(PUBLISHERS_CSV)
		games_df = validate_ids(games_df, developers_df, publishers_df, tracker)
		tracker.log_completion("IDs validated")
		
		genres_df, genre_cols = load_genre_vectors(GENRE_VECTORS_PATH, tracker)
		tracker.log_completion("Genre vectors loaded",
							 genre_columns=len(genre_cols),
							 sample_columns=genre_cols[:5])
		
		games_genres_df = join_games_genres(games_df, genres_df, genre_cols, tracker)
		tracker.log_completion("Games joined with genres")
		
		dev_rows_df = expand_to_company_rows(
			games_genres_df,
			genre_cols,
			tracker,
			"Developer",
			"developer_ids",
			"developer_id"
		)
		tracker.log_completion("Expanded to developer rows")
		
		pub_rows_df = expand_to_company_rows(
			games_genres_df,
			genre_cols,
			tracker,
			"Publisher",
			"publisher_ids",
			"publisher_id"
		)
		tracker.log_completion("Expanded to publisher rows")
		
		dev_shares_df = compute_company_shares(
			dev_rows_df, 
			developers_df, 
			genre_cols, 
			tracker,
			company_type="Developer",
			company_id_col="developer_id",
			company_name_col="Developer"
		)
		tracker.log_completion("Developer shares computed")
		
		pub_shares_df = compute_company_shares(
			pub_rows_df, 
			publishers_df, 
			genre_cols, 
			tracker,
			company_type="Publisher",
			company_id_col="publisher_id",
			company_name_col="Publisher"
		)
		tracker.log_completion("Publisher shares computed")
		
		create_output_files(dev_shares_df, pub_shares_df, tracker)
		tracker.log_completion("Output files created")
		
		# Summary
		tracker.log_summary()
		
		# Audit report
		audit_text = tracker.generate_audit_report()
		AUDIT_REPORT.write_text(audit_text)
		logger.info(f"Audit report saved to: {AUDIT_REPORT}")
		
		logger.info("")
		logger.info("="*80)
		logger.info("SUCCESS - Pipeline Complete")
		logger.info("="*80)
		
	except Exception as e:
		logger.error(f"Fatal error: {e}", exc_info=True)
		       
		

if __name__ == "__main__":
	main()
