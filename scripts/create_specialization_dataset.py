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
    # To be implemented in Step 3
    pass

def load_genre_vectors(genre_path, tracker):
    """Load and validate genre vectors."""
    # To be implemented in Step 4
    pass

def join_games_genres(games_df, genres_df, tracker):
    """Join games with genre vectors."""
    # To be implemented in Step 5
    pass

def expand_to_developer_rows(games_genres_df, tracker):
    """Expand to developer rows (one row per game-developer pair)."""
    # To be implemented in Step 6
    pass

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
        
        # Future steps will be called here:
        # developers_df = pd.read_csv(DEVELOPERS_CSV)
        # publishers_df = pd.read_csv(PUBLISHERS_CSV)
        # validate_ids(games_df, developers_df, publishers_df, tracker)
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
