# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is a comprehensive Data Warehousing academic project (`tp_datawarehousing`) that implements a complete ETL pipeline with quality controls. The project processes business data through multiple layers from staging to final dimensional model, including data quality monitoring (DQM) and automated reporting.

## Key Commands

### Installation and Setup
```bash
# Install project in development mode
pip install -e .

# Install dependencies
pip install -r requirements.txt
```

### Running the Pipeline
```bash
# Main pipeline execution (runs all steps sequentially)
python -m tp_datawarehousing.main

# Alternative command
tp-datawarehousing

# Check data quality metrics after execution
python check_quality_metrics.py

# Database optimization (if needed)
python optimize_database.py
```

### Individual Step Execution
```bash
# Each step can be run individually (in src/tp_datawarehousing/steps/)
python -m tp_datawarehousing.steps.step_01_setup_staging_area
python -m tp_datawarehousing.steps.step_02_load_staging_data
# ... etc for steps 01-10
```

## Architecture Overview

### Layer Structure (Prefixed Tables)
- **TMP_**: Staging area for raw data validation
- **ING_**: Ingestion layer with referential integrity 
- **DWA_**: Data Warehouse dimensional model (star schema)
- **DQM_**: Data Quality Mart for monitoring and metrics
- **MET_**: Metadata layer for documentation
- **DP_**: Data Products (aggregated business views)

### Pipeline Flow (10 Steps)
1. **Setup Staging Area** - Create database structure and metadata tables
2. **Load Staging Data** - Import CSV files from Ingesta1 to TMP_ tables
3. **Create Ingestion Layer** - Add referential integrity constraints (ING_)
4. **Link World Data** - Standardize country names with external dataset
5. **Create DWH Model** - Build dimensional star schema (DWA_)
6. **Create DQM** - Setup data quality monitoring framework
7. **Initial DWH Load** - Load data with quality checks and SCD implementation
8. **Load Ingesta2** - Import incremental data to TMP2_ staging
9. **Update DWH** - Process incremental updates with SCD Type 2
10. **Create Data Products** - Generate business-focused aggregated tables

### Data Quality Framework
- **Ingestion Quality**: Null checks, format validation, range validation
- **Integration Quality**: Referential integrity, cross-table consistency
- **Business Logic**: Custom validation rules, completeness scores
- **Monitoring**: All quality metrics stored in DQM with execution tracking

## Key Files and Directories

### Source Code Structure
- `src/tp_datawarehousing/main.py` - Main orchestrator script
- `src/tp_datawarehousing/steps/` - Individual pipeline steps (step_01 to step_10)
- `src/tp_datawarehousing/quality_utils.py` - Comprehensive data quality utilities

### Data and Configuration
- `db/tp_dwa.db` - Main SQLite database (created automatically)
- `csvs - dashboards/` - Source CSV files and output data products
- `docs/` - Step-by-step analysis documentation
- `pyproject.toml` - Project configuration and dependencies

### Dashboards and Reports
- `dash_dqm.pbix` - Data Quality Mart dashboard (Power BI)
- `dash_prods.pbix` - Data Products dashboard (Power BI)
- `check_quality_metrics.py` - Console-based quality report

## Database Technology

- **SQLite** with WAL mode for concurrent access
- **Optimized for quality**: Extensive retry logic for locked database scenarios
- **Transaction management**: Automatic rollback on failures
- **Connection pooling**: Managed connections with timeout handling

## Quality Control Standards

The project implements professional-grade data quality controls:
- **Completeness validation** with configurable thresholds
- **Format pattern matching** (emails, dates, codes)
- **Business rule validation** (cross-field logic)
- **Referential integrity checks**
- **Data freshness monitoring**
- **Duplicate detection and SCD Type 2** for historical tracking

## Development Patterns

### Error Handling
- All database operations use retry logic with exponential backoff
- Quality failures are logged but don't necessarily stop pipeline
- Comprehensive logging with structured quality metrics

### Modular Design
- Each step is independent and can be run separately
- Shared utilities in `quality_utils.py`
- Configuration centralized in constants

### Data Processing
- Pandas for data manipulation and CSV processing
- SQL for all data warehouse operations
- Incremental processing with change data capture (CDC)

## Important Notes

- The project requires Python 3.11+ with pandas and numpy
- SQLite database is created automatically in `db/` directory
- All quality metrics are stored and can be queried via DQM tables
- The pipeline is designed to handle both initial loads and incremental updates
- Power BI dashboards are included for business intelligence visualization