"""
Create Genre Specialization Dataset

This script processes MobyGames data to create developer and publisher genre 
specialization datasets. For each entity per year, it computes the share of 
games released across 231 genre categories.

Author: Research Team
Date: February 2026
"""

import sqlite3
import pandas as pd
import json
import numpy as np
import logging
from pathlib import Path
from datetime import datetime
import sys

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
GENRE_METADATA_PATH = DATA_DIR / "genre_metadata.json"

# Output files
DEV_OUTPUT = OUTPUT_DIR / "developer_genre_shares.csv"
PUB_OUTPUT = OUTPUT_DIR / "publisher_genre_shares.csv"

# Logging configuration
LOG_DIR = BASE_DIR / "logs"
LOG_FILE = LOG_DIR / f"specialization_processing_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
AUDIT_REPORT = LOG_DIR / "specialization_audit_report.txt"

# =============================================================================
# LOGGING SETUP
# =============================================================================

def setup_logging():
    """Configure logging to both file and console."""
    # Create logs directory if needed
    LOG_FILE.parent.mkdir(parents=True, exist_ok=True)
    
    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(LOG_FILE),
            logging.StreamHandler(sys.stdout)
        ]
    )
    
    logger = logging.getLogger(__name__)
    logger.info("="*80)
    logger.info("Genre Specialization Dataset Creation - Starting")
    logger.info("="*80)
    logger.info(f"Log file: {LOG_FILE}")
    
    return logger

# =============================================================================
# VERIFICATION TRACKER
# =============================================================================

class VerificationTracker:
    """Track verification results throughout the pipeline."""
    
    def __init__(self, logger):
        self.logger = logger
        self.checks = []
        self.warnings = []
        self.errors = []
    
    def add_check(self, step, check_name, passed, details=""):
        """Record a verification check result."""
        result = {
            'step': step,
            'check': check_name,
            'passed': passed,
            'details': details,
            'timestamp': datetime.now()
        }
        self.checks.append(result)
        
        status = "✓ PASS" if passed else "✗ FAIL"
        self.logger.info(f"  [{status}] {check_name}: {details}")
        
        if not passed:
            self.errors.append(result)
    
    def add_warning(self, step, message):
        """Record a warning."""
        warning = {
            'step': step,
            'message': message,
            'timestamp': datetime.now()
        }
        self.warnings.append(warning)
        self.logger.warning(f"  [⚠ WARN] {message}")
    
    def log_step_start(self, step_num, step_name):
        """Log the start of a processing step."""
        self.logger.info("")
        self.logger.info("-"*80)
        self.logger.info(f"STEP {step_num}: {step_name}")
        self.logger.info("-"*80)
    
    def log_summary(self):
        """Log summary of all checks."""
        self.logger.info("")
        self.logger.info("="*80)
        self.logger.info("VERIFICATION SUMMARY")
        self.logger.info("="*80)
        self.logger.info(f"Total checks: {len(self.checks)}")
        self.logger.info(f"Passed: {sum(1 for c in self.checks if c['passed'])}")
        self.logger.info(f"Failed: {sum(1 for c in self.checks if not c['passed'])}")
        self.logger.info(f"Warnings: {len(self.warnings)}")
        
        if self.errors:
            self.logger.error("")
            self.logger.error("FAILED CHECKS:")
            for err in self.errors:
                self.logger.error(f"  - Step {err['step']}: {err['check']} - {err['details']}")
    
    def generate_audit_report(self):
        """Generate detailed audit report."""
        report_lines = []
        report_lines.append("="*80)
        report_lines.append("GENRE SPECIALIZATION DATASET - AUDIT REPORT")
        report_lines.append("="*80)
        report_lines.append(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        report_lines.append("")
        
        report_lines.append("VERIFICATION SUMMARY")
        report_lines.append("-"*80)
        report_lines.append(f"Total verification checks: {len(self.checks)}")
        report_lines.append(f"Checks passed: {sum(1 for c in self.checks if c['passed'])}")
        report_lines.append(f"Checks failed: {sum(1 for c in self.checks if not c['passed'])}")
        report_lines.append(f"Warnings issued: {len(self.warnings)}")
        report_lines.append("")
        
        report_lines.append("DETAILED CHECKS")
        report_lines.append("-"*80)
        for check in self.checks:
            status = "PASS" if check['passed'] else "FAIL"
            report_lines.append(f"[{status}] Step {check['step']}: {check['check']}")
            report_lines.append(f"      {check['details']}")
            report_lines.append("")
        
        if self.warnings:
            report_lines.append("WARNINGS")
            report_lines.append("-"*80)
            for warn in self.warnings:
                report_lines.append(f"Step {warn['step']}: {warn['message']}")
            report_lines.append("")
        
        if self.errors:
            report_lines.append("ERRORS")
            report_lines.append("-"*80)
            for err in self.errors:
                report_lines.append(f"Step {err['step']}: {err['check']}")
                report_lines.append(f"      {err['details']}")
            report_lines.append("")
        
        return "\n".join(report_lines)

# =============================================================================
# MAIN PIPELINE FUNCTIONS (to be implemented in subsequent steps)
# =============================================================================

def extract_games_from_db(db_path, tracker):
    """Extract and parse games with developer/publisher IDs from SQLite."""
    tracker.log_step_start(2, "Extract and parse games from database")
    
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
    tracker.add_check(2, "Games extracted", True, 
                     f"{len(df):,} games parsed successfully")
    
    if parse_errors > 0:
        tracker.add_warning(2, f"{parse_errors:,} games had parsing errors")
    
    # Check for missing developer/publisher IDs
    no_devs = df['developer_ids'].apply(lambda x: len(x) == 0).sum()
    no_pubs = df['publisher_ids'].apply(lambda x: len(x) == 0).sum()
    
    tracker.add_check(2, "Games with developers", 
                     no_devs < len(df) * 0.5,  # Fail if >50% missing
                     f"{len(df) - no_devs:,} games have developers ({no_devs:,} without)")
    
    tracker.add_check(2, "Games with publishers",
                     no_pubs < len(df) * 0.5,
                     f"{len(df) - no_pubs:,} games have publishers ({no_pubs:,} without)")
    
    # Check for missing release years
    no_year = df['release_year'].isna().sum()
    tracker.add_check(2, "Games with release year",
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
        
        tracker.add_check(2, "Year range reasonable",
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
    tracker.log_step_start(3, "Validate extracted IDs against lookup tables")
    
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
    
    tracker.add_check(3, "Developer ID match rate",
                     dev_match_rate >= 0.95,  # Fail if <95% match
                     f"{dev_match_rate*100:.1f}% of developer IDs found in lookup table")
    
    tracker.add_check(3, "Publisher ID match rate",
                     pub_match_rate >= 0.95,  # Fail if <95% match
                     f"{pub_match_rate*100:.1f}% of publisher IDs found in lookup table")
    
    # Report unmatched IDs
    if unmatched_devs:
        tracker.add_warning(3, f"{len(unmatched_devs):,} orphaned developer IDs not in lookup table")
        if len(unmatched_devs) <= 10:
            tracker.logger.info(f"  Orphaned developer IDs: {sorted(list(unmatched_devs))}")
        else:
            sample = sorted(list(unmatched_devs))[:10]
            tracker.logger.info(f"  Sample orphaned developer IDs: {sample}...")
    
    if unmatched_pubs:
        tracker.add_warning(3, f"{len(unmatched_pubs):,} orphaned publisher IDs not in lookup table")
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
    
    tracker.add_check(3, "Games with valid developer IDs",
                     games_with_bad_devs < len(games_df) * 0.1,  # Fail if >10% affected
                     f"{len(games_df) - games_with_bad_devs:,} games have valid developer IDs ({games_with_bad_devs:,} affected)")
    
    tracker.add_check(3, "Games with valid publisher IDs",
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
    
    tracker.add_check(3, "Data cleaning successful",
                     True,
                     f"Cleaned {len(unmatched_devs)} dev IDs and {len(unmatched_pubs)} pub IDs")
    
    return games_df

def load_genre_vectors(genre_path, tracker):
    """Load and validate genre vectors."""
    tracker.log_step_start(4, "Load and validate genre vectors")
    
    # First, load a sample to verify structure
    tracker.logger.info("Loading sample (100 rows) to verify structure...")
    sample_df = pd.read_csv(genre_path, nrows=100)
    
    tracker.logger.info(f"Sample shape: {sample_df.shape}")
    tracker.logger.info(f"Columns: {list(sample_df.columns[:5])}... (showing first 5)")
    
    # Verify expected columns
    expected_cols = ['game_id', 'title'] + [f'genre_{i}' for i in range(231)]
    has_game_id = 'game_id' in sample_df.columns
    has_title = 'title' in sample_df.columns
    
    tracker.add_check(4, "Has game_id column", has_game_id, 
                     "game_id column present" if has_game_id else "game_id column MISSING")
    tracker.add_check(4, "Has title column", has_title,
                     "title column present" if has_title else "title column MISSING")
    
    # Count genre columns
    genre_cols = [col for col in sample_df.columns if col.startswith('genre_')]
    tracker.add_check(4, "Has 231 genre columns", len(genre_cols) == 231,
                     f"Found {len(genre_cols)} genre columns (expected 231)")
    
    # Now load full file
    tracker.logger.info("")
    tracker.logger.info("Loading full genre vectors file...")
    df = pd.read_csv(genre_path)
    
    tracker.logger.info(f"Loaded {len(df):,} games with genre vectors")
    tracker.add_check(4, "Genre vectors loaded", True,
                     f"{len(df):,} games loaded from CSV")
    
    # Check for NULL values in genre columns
    null_counts = df[genre_cols].isnull().sum()
    total_nulls = null_counts.sum()
    
    tracker.add_check(4, "No NULL values in genre columns",
                     total_nulls == 0,
                     f"{total_nulls:,} NULL values found" if total_nulls > 0 else "No NULL values")
    
    if total_nulls > 0:
        tracker.add_warning(4, f"Found {total_nulls:,} NULL values in genre columns")
        cols_with_nulls = null_counts[null_counts > 0]
        if len(cols_with_nulls) <= 5:
            for col, count in cols_with_nulls.items():
                tracker.logger.info(f"  {col}: {count} NULLs")
    
    # Verify all genre columns are numeric
    non_numeric_cols = []
    for col in genre_cols:
        if not pd.api.types.is_numeric_dtype(df[col]):
            non_numeric_cols.append(col)
    
    tracker.add_check(4, "All genre columns numeric",
                     len(non_numeric_cols) == 0,
                     f"All genre columns are numeric" if len(non_numeric_cols) == 0 
                     else f"{len(non_numeric_cols)} non-numeric columns")
    
    # Verify values are binary (0 or 1)
    tracker.logger.info("")
    tracker.logger.info("Checking if genre values are binary...")
    
    # Check min and max values across all genre columns
    genre_data = df[genre_cols]
    min_val = genre_data.min().min()
    max_val = genre_data.max().max()
    
    is_binary = (min_val >= 0 and max_val <= 1)
    
    tracker.add_check(4, "Genre values in [0,1] range",
                     is_binary,
                     f"Values range from {min_val} to {max_val}")
    
    # Check if values are actually 0 or 1 (not fractions)
    unique_vals = set()
    for col in genre_cols[:10]:  # Sample first 10 columns
        unique_vals.update(df[col].unique())
    
    tracker.logger.info(f"Sample unique values from genre columns: {sorted(unique_vals)[:20]}")
    
    # Sample check: display 5 random rows with their genre sums
    tracker.logger.info("")
    tracker.logger.info("Sample records with genre counts:")
    sample_rows = df.sample(min(5, len(df)), random_state=42)
    
    for idx, row in sample_rows.iterrows():
        genre_sum = row[genre_cols].sum()
        tracker.logger.info(f"  Game {row['game_id']}: {row['title'][:50]}...")
        tracker.logger.info(f"    Total genres: {int(genre_sum)}")
    
    # Overall statistics
    tracker.logger.info("")
    tracker.logger.info("Genre Statistics:")
    genre_sums = df[genre_cols].sum(axis=1)
    tracker.logger.info(f"  Avg genres per game: {genre_sums.mean():.2f}")
    tracker.logger.info(f"  Min genres per game: {int(genre_sums.min())}")
    tracker.logger.info(f"  Max genres per game: {int(genre_sums.max())}")
    tracker.logger.info(f"  Games with 0 genres: {(genre_sums == 0).sum():,}")
    
    # Check for games with no genres
    no_genres = (genre_sums == 0).sum()
    tracker.add_check(4, "Most games have genres",
                     no_genres < len(df) * 0.1,  # Fail if >10% have no genres
                     f"{len(df) - no_genres:,} games have at least one genre ({no_genres:,} have none)")
    
    return df

def join_games_genres(games_df, genres_df, tracker):
    """Join games with genre vectors."""
    tracker.log_step_start(5, "Join games with genre vectors")
    
    # Report counts before merge
    tracker.logger.info(f"Games dataframe: {len(games_df):,} rows")
    tracker.logger.info(f"Genre vectors dataframe: {len(genres_df):,} rows")
    
    # Perform left join to keep all games (even if no genre data)
    tracker.logger.info("")
    tracker.logger.info("Performing merge on game_id...")
    
    merged_df = games_df.merge(genres_df, on='game_id', how='left', suffixes=('', '_genre'))
    
    tracker.logger.info(f"Merged dataframe: {len(merged_df):,} rows")
    
    # Verification: Check for duplicates
    duplicate_count = len(merged_df) - len(games_df)
    tracker.add_check(5, "No duplicates introduced",
                     duplicate_count == 0,
                     f"No duplicates introduced" if duplicate_count == 0 
                     else f"{duplicate_count} duplicate rows created")
    
    # Identify games in database but not in vectors
    games_without_genres = merged_df[merged_df['genre_0'].isna()]
    tracker.logger.info(f"Games without genre vectors: {len(games_without_genres):,}")
    
    if len(games_without_genres) > 0:
        tracker.add_warning(5, f"{len(games_without_genres):,} games have no genre vectors")
        sample_without = games_without_genres.head(5)
        tracker.logger.info("  Sample games without genres:")
        for _, row in sample_without.iterrows():
            tracker.logger.info(f"    Game {row['game_id']}: {row['title']}")
    
    # Identify games in vectors but not in database
    games_in_db = set(games_df['game_id'])
    games_in_vectors = set(genres_df['game_id'])
    only_in_vectors = games_in_vectors - games_in_db
    
    tracker.logger.info(f"Games only in vectors (not in database): {len(only_in_vectors):,}")
    
    if len(only_in_vectors) > 0:
        tracker.add_warning(5, f"{len(only_in_vectors):,} games in vectors but not in database")
        if len(only_in_vectors) <= 10:
            tracker.logger.info(f"  IDs: {sorted(list(only_in_vectors))}")
        else:
            sample_ids = sorted(list(only_in_vectors))[:10]
            tracker.logger.info(f"  Sample IDs: {sample_ids}...")
    
    # Calculate match rate
    games_with_genres = len(merged_df) - len(games_without_genres)
    match_rate = games_with_genres / len(merged_df) if len(merged_df) > 0 else 0
    
    tracker.add_check(5, "High genre vector match rate",
                     match_rate >= 0.95,  # Fail if <95% have genres
                     f"{match_rate*100:.1f}% of games have genre vectors ({games_with_genres:,}/{len(merged_df):,})")
    
    # Remove games without genre vectors (we can't use them)
    tracker.logger.info("")
    tracker.logger.info("Filtering to games with genre vectors...")
    merged_df = merged_df[merged_df['genre_0'].notna()].copy()
    
    tracker.logger.info(f"After filtering: {len(merged_df):,} games")
    
    tracker.add_check(5, "Games after merge",
                     len(merged_df) > 0,
                     f"{len(merged_df):,} games retained after merge")
    
    # Verify no NULL values in genre columns
    genre_cols = [col for col in merged_df.columns if col.startswith('genre_')]
    null_count = merged_df[genre_cols].isnull().sum().sum()
    
    tracker.add_check(5, "No NULL values in genre columns after merge",
                     null_count == 0,
                     f"No NULL values" if null_count == 0 else f"{null_count} NULL values found")
    
    # Sample row check
    tracker.logger.info("")
    tracker.logger.info("Sample merged records:")
    sample = merged_df[merged_df['release_year'].notna()].head(3)
    
    for _, row in sample.iterrows():
        genre_sum = row[genre_cols].sum()
        tracker.logger.info(f"  Game {row['game_id']}: {row['title']}")
        tracker.logger.info(f"    Year: {int(row['release_year'])}")
        tracker.logger.info(f"    Developers: {row['developer_ids']}")
        tracker.logger.info(f"    Publishers: {row['publisher_ids']}")
        tracker.logger.info(f"    Total genres: {int(genre_sum)}")
    
    # Final statistics
    tracker.logger.info("")
    tracker.logger.info("Final merged dataset statistics:")
    tracker.logger.info(f"  Total games: {len(merged_df):,}")
    tracker.logger.info(f"  Games with developers: {(merged_df['developer_ids'].apply(lambda x: len(x) > 0)).sum():,}")
    tracker.logger.info(f"  Games with publishers: {(merged_df['publisher_ids'].apply(lambda x: len(x) > 0)).sum():,}")
    tracker.logger.info(f"  Games with release year: {merged_df['release_year'].notna().sum():,}")
    
    return merged_df

def expand_to_developer_rows(games_genres_df, tracker):
    """
    Step 6: Expand each game to multiple rows, one per developer
    
    Each row will have: game_id, developer_id, release_year, genre_0..genre_230
    """
    tracker.logger.info("Expanding to developer rows...")
    
    # Filter to games that have developers and release year
    games_with_devs = games_genres_df[
        (games_genres_df['developer_ids'].apply(lambda x: isinstance(x, list) and len(x) > 0)) &
        (games_genres_df['release_year'].notna())
    ].copy()
    
    tracker.logger.info(f"Games with developers and year: {len(games_with_devs):,}")
    
    # Explode the developer_ids list
    tracker.logger.info("Exploding developer_ids list...")
    expanded = games_with_devs.explode('developer_ids')
    tracker.logger.info(f"After explosion: {len(expanded):,} rows")
    
    # Rename developer_ids to developer_id (now scalar)
    expanded = expanded.rename(columns={'developer_ids': 'developer_id'})
    
    # Select final columns: developer_id, release_year, genre_0..genre_230
    genre_cols = [f'genre_{i}' for i in range(231)]
    final_cols = ['game_id', 'developer_id', 'release_year'] + genre_cols
    developer_rows = expanded[final_cols].copy()
    
    # Verification checks
    tracker.logger.info("")
    tracker.add_check(
        6,
        "Developer rows created",
        len(developer_rows) > 0,
        f"{len(developer_rows):,} developer-game rows created"
    )
    
    null_devs = developer_rows['developer_id'].isna().sum()
    tracker.add_check(
        6,
        "No NULL developer IDs",
        null_devs == 0,
        f"No NULL developer IDs" if null_devs == 0 else f"Found {null_devs} NULL developer IDs"
    )
    
    null_years = developer_rows['release_year'].isna().sum()
    tracker.add_check(
        6,
        "No NULL years in developer rows",
        null_years == 0,
        f"No NULL years" if null_years == 0 else f"Found {null_years} NULL years"
    )
    
    # Check developer ID types and ranges
    min_dev_id = developer_rows['developer_id'].min()
    max_dev_id = developer_rows['developer_id'].max()
    unique_devs = developer_rows['developer_id'].nunique()
    
    tracker.logger.info(f"")
    tracker.logger.info(f"Developer ID range: {min_dev_id} to {max_dev_id}")
    tracker.logger.info(f"Unique developers: {unique_devs:,}")
    
    tracker.add_check(
        6,
        "Positive developer IDs",
        min_dev_id > 0,
        f"All developer IDs are positive (min: {min_dev_id})"
    )
    
    # Check for duplicates (game_id, developer_id, year)
    dup_mask = developer_rows.duplicated(subset=['game_id', 'developer_id', 'release_year'], keep=False)
    dup_count = dup_mask.sum()
    
    if dup_count > 0:
        tracker.logger.info("")
        tracker.logger.info(f"Found {dup_count} duplicate rows. Investigating...")
        dup_rows = developer_rows[dup_mask].sort_values(['game_id', 'developer_id', 'release_year'])
        tracker.logger.info(f"Sample duplicates:")
        for idx, row in dup_rows.head(6).iterrows():
            tracker.logger.info(f"  Game {int(row['game_id'])}, Developer {int(row['developer_id'])}, Year {int(row['release_year'])}")
        
        # Remove duplicates, keeping first occurrence
        tracker.logger.info("")
        tracker.logger.info("Removing duplicate rows (keeping first occurrence)...")
        developer_rows = developer_rows.drop_duplicates(subset=['game_id', 'developer_id', 'release_year'], keep='first')
        tracker.logger.info(f"After removing duplicates: {len(developer_rows):,} rows")
    
    tracker.add_check(
        6,
        "Duplicates handled",
        True,
        f"Removed {dup_count} duplicate rows" if dup_count > 0 else "No duplicates found"
    )
    
    # Sample check
    tracker.logger.info("")
    tracker.logger.info("Sample developer rows:")
    sample = developer_rows.head(3)
    for idx, row in sample.iterrows():
        genre_count = row[[f'genre_{i}' for i in range(231)]].sum()
        tracker.logger.info(f"  Game {int(row['game_id'])}, Developer {int(row['developer_id'])}, Year {int(row['release_year'])}")
        tracker.logger.info(f"    Total genres: {int(genre_count)}")
    
    return developer_rows

def expand_to_publisher_rows(games_genres_df, tracker):
    """Expand to publisher rows (one row per game-publisher pair)."""
    # To be implemented in Step 7
    pass

def compute_developer_shares(dev_rows_df, developers_df, tracker):
    """Compute and validate genre shares by developer-year."""
    # To be implemented in Step 8
    pass

def compute_publisher_shares(pub_rows_df, publishers_df, tracker):
    """Compute and validate genre shares by publisher-year."""
    # To be implemented in Step 9
    pass

def create_output_files(dev_shares_df, pub_shares_df, tracker):
    """Create final output datasets."""
    # To be implemented in Step 10
    pass

# =============================================================================
# MAIN EXECUTION
# =============================================================================

def main():
    """Main execution pipeline."""
    # Step 1: Setup
    logger = setup_logging()
    tracker = VerificationTracker(logger)
    
    try:
        # Verify all input files exist
        logger.info("Checking input files...")
        for path, name in [
            (DB_PATH, "Database"),
            (GENRE_VECTORS_PATH, "Genre vectors"),
            (DEVELOPERS_CSV, "Developers CSV"),
            (PUBLISHERS_CSV, "Publishers CSV")
        ]:
            if path.exists():
                logger.info(f"  ✓ {name}: {path}")
            else:
                logger.error(f"  ✗ {name} not found: {path}")
                raise FileNotFoundError(f"{name} not found at {path}")
        
        logger.info("")
        logger.info("All input files found. Ready to begin processing.")
        
        # Step 2: Extract games from database
        games_df = extract_games_from_db(DB_PATH, tracker)
        
        logger.info("")
        logger.info("="*80)
        logger.info("Step 2 Complete - Games extracted from database")
        logger.info("="*80)
        logger.info(f"Extracted {len(games_df):,} games")
        
        # Step 3: Validate IDs against lookup tables
        developers_df = pd.read_csv(DEVELOPERS_CSV)
        publishers_df = pd.read_csv(PUBLISHERS_CSV)
        games_df = validate_ids(games_df, developers_df, publishers_df, tracker)
        
        logger.info("")
        logger.info("="*80)
        logger.info("Step 3 Complete - IDs validated against lookup tables")
        logger.info("="*80)
        
        # Step 4: Load and validate genre vectors
        genres_df = load_genre_vectors(GENRE_VECTORS_PATH, tracker)
        
        logger.info("")
        logger.info("="*80)
        logger.info("Step 4 Complete - Genre vectors loaded and validated")
        logger.info("="*80)
        logger.info(f"Loaded {len(genres_df):,} games with genre data")
        
        # Step 5: Join games with genre vectors
        games_genres_df = join_games_genres(games_df, genres_df, tracker)
        
        logger.info("")
        logger.info("="*80)
        logger.info("Step 5 Complete - Games joined with genre vectors")
        logger.info("="*80)
        logger.info(f"Final dataset: {len(games_genres_df):,} games with complete data")
        
        # Step 6: Expand to developer rows
        logger.info("")
        logger.info("-"*80)
        logger.info("STEP 6: Expand to developer rows")
        logger.info("-"*80)
        dev_rows_df = expand_to_developer_rows(games_genres_df, tracker)
        
        logger.info("")
        logger.info("="*80)
        logger.info("Step 6 Complete - Expanded to developer rows")
        logger.info("="*80)
        logger.info(f"Created {len(dev_rows_df):,} developer-game rows")
        
        # Future steps will be called here:
        # pub_rows_df = expand_to_publisher_rows(games_genres_df, tracker)
        # ... etc
        
        # Final summary
        tracker.log_summary()
        
        # Generate audit report
        audit_text = tracker.generate_audit_report()
        AUDIT_REPORT.write_text(audit_text)
        logger.info(f"Audit report saved to: {AUDIT_REPORT}")
        
        logger.info("")
        logger.info("="*80)
        logger.info("Pipeline Setup Complete")
        logger.info("="*80)
        
    except Exception as e:
        logger.error(f"Fatal error: {e}", exc_info=True)
        sys.exit(1)

if __name__ == "__main__":
    main()
