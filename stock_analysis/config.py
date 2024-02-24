"""Stock analysis development configuration."""

import pathlib

# Root of this application, useful if it doesn't occupy an entire domain
APPLICATION_ROOT = '/'
STOCK_ANALYSIS_ROOT = pathlib.Path(__file__).resolve().parent.parent

# Required to call AWS services
# Update to real values if moving away from running locally
USE_LOCAL_DDB = True

# Database file is var/stock_analysis.sqlite3
DATABASE_FILENAME = STOCK_ANALYSIS_ROOT/'var'/'stock_analysis.sqlite3'

