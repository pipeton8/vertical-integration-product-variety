"""
Shared utilities for research pipeline scripts.

Provides consistent logging and verification tracking across all data processing scripts.

Author: Research Team
Date: February 2026
"""

import logging
import sys
from pathlib import Path
from datetime import datetime


def setup_logging(script_name, log_dir=None, base_dir=None):
    """Configure logging to both file and console.
    
    Args:
        script_name: Name of the script (e.g., 'specialization_processing')
        log_dir: Directory for log files (default: base_dir/logs)
        base_dir: Base directory of project (default: script's parent.parent)
    
    Returns:
        tuple: (logger, log_file_path)
    """
    if base_dir is None:
        # Infer from this utils file location
        base_dir = Path(__file__).parent.parent
    
    if log_dir is None:
        log_dir = base_dir / "logs"
    
    log_dir.mkdir(parents=True, exist_ok=True)
    
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    log_file = log_dir / f"{script_name}_{timestamp}.log"
    
    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_file),
            logging.StreamHandler(sys.stdout)
        ]
    )
    
    logger = logging.getLogger(__name__)
    logger.info("="*80)
    logger.info(f"{script_name.replace('_', ' ').title()} - Starting")
    logger.info("="*80)
    logger.info(f"Log file: {log_file}")
    
    return logger, log_file


class VerificationTracker:
    """Track verification results throughout a data pipeline."""
    
    def __init__(self, logger):
        self.logger = logger
        self.checks = []
        self.warnings = []
        self.errors = []
    
    def add_check(self, check_name, passed, details=""):
        """Record a verification check result."""
        result = {
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
    
    def add_warning(self, message):
        """Record a warning."""
        warning = {
            'message': message,
            'timestamp': datetime.now()
        }
        self.warnings.append(warning)
        self.logger.warning(f"  [⚠ WARN] {message}")
    
    def log_step_start(self, step_name):
        """Log the start of a processing step."""
        self.logger.info("")
        self.logger.info("-"*80)
        self.logger.info(step_name)
        self.logger.info("-"*80)
    
    def log_completion(self, step_name, **extra_info):
        """Log completion of a pipeline step with standard formatting."""
        self.logger.info("")
        self.logger.info("="*80)
        self.logger.info(f"Complete - {step_name}")
        self.logger.info("="*80)
        for key, value in extra_info.items():
            self.logger.info(f"  {key}: {value}")
    
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
                self.logger.error(f"  - {err['check']} - {err['details']}")
    
    def generate_audit_report(self):
        """Generate detailed audit report."""
        report_lines = []
        report_lines.append("="*80)
        report_lines.append("AUDIT REPORT")
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
        
        if self.checks:
            report_lines.append("DETAILED CHECKS")
            report_lines.append("-"*80)
            for check in self.checks:
                status = "PASS" if check['passed'] else "FAIL"
                report_lines.append(f"[{status}] {check['check']}")
                report_lines.append(f"    {check['details']}")
                report_lines.append(f"    Timestamp: {check['timestamp'].strftime('%Y-%m-%d %H:%M:%S')}")
                report_lines.append("")
        
        if self.warnings:
            report_lines.append("WARNINGS")
            report_lines.append("-"*80)
            for warning in self.warnings:
                report_lines.append(f"  - {warning['message']}")
                report_lines.append(f"    Timestamp: {warning['timestamp'].strftime('%Y-%m-%d %H:%M:%S')}")
                report_lines.append("")
        
        if self.errors:
            report_lines.append("FAILED CHECKS DETAIL")
            report_lines.append("-"*80)
            for err in self.errors:
                report_lines.append(f"  - {err['check']}")
                report_lines.append(f"    {err['details']}")
                report_lines.append("")
        
        report_lines.append("="*80)
        report_lines.append("END OF REPORT")
        report_lines.append("="*80)
        
        return "\n".join(report_lines)


def build_category_mappings(genre_cols):
    """Build mappings from genre columns to their categories.
    
    Args:
        genre_cols: List of genre column names (category_X_genre_Y format)
    
    Returns:
        tuple: (col_to_category dict, category_cols dict)
            - col_to_category: Maps each column to its category ID
            - category_cols: Maps each category ID to list of its columns
    """
    col_to_category = {}
    category_cols = {}  # category_id -> [list of cols in this category]
    
    for col in genre_cols:
        # Parse column name: category_1_genre_5 -> category_id=1
        parts = col.split('_')
        cat_id = int(parts[1])
        
        col_to_category[col] = cat_id
        if cat_id not in category_cols:
            category_cols[cat_id] = []
        category_cols[cat_id].append(col)
    
    return col_to_category, category_cols


def verify_file_exists(file_paths, logger):
    """Verify that required input files exist.
    
    Args:
        file_paths: List of tuples (path, descriptive_name)
        logger: Logger instance
    
    Raises:
        FileNotFoundError: If any file is missing
    """
    logger.info("Checking input files...")
    for path, name in file_paths:
        if path.exists():
            logger.info(f"  ✓ {name}")
        else:
            logger.error(f"  ✗ {name} not found: {path}")
            raise FileNotFoundError(f"{name} not found at {path}")
    logger.info("")
