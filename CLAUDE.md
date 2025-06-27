# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Common Development Commands

### Installation and Setup
```bash
pip install -e .
```

### Running the Application
```bash
python -m tp_datawarehousing.main
```

### Dependencies
- Uses Python 3.11+
- Main dependencies managed in `pyproject.toml`
- Runtime dependencies: pandas
- Lock file: `uv.lock`

## Architecture Overview

This is a **Data Warehousing (DWA) project** implementing an end-to-end ETL pipeline for academic purposes. The project follows a **modular step-based architecture** with clear separation of concerns.

### Core Architecture Principles

1. **Sequential Step Processing**: The main orchestrator (`main.py`) executes 10 discrete steps in order, each handling a specific phase of the data warehousing process.

2. **Modular Design**: Each step is implemented as a separate module in `src/tp_datawarehousing/steps/`, allowing for independent development and testing of individual ETL phases.

3. **SQLite-based Data Storage**: All data operations use SQLite database (`.data/tp_dwa.db`) with different table prefixes for different layers:
   - `TMP_`: Staging area tables
   - `ING_`: Ingestion layer with integrity constraints
   - `DWH_`: Data warehouse dimensional model
   - `DQM_`: Data quality mart
   - `MET_`: Metadata tables

### Step Sequence and Responsibilities

The 10-step process implements a complete data warehousing flow:

1. **Step 1**: Setup staging area and metadata structures
2. **Step 2**: Load initial data (Ingesta1) into staging
3. **Step 3**: Create ingestion layer with data integrity
4. **Step 4**: Link and standardize world/country data
5. **Step 5**: Create dimensional data warehouse model
6. **Step 6**: Create data quality mart (DQM)
7. **Step 7**: Initial data warehouse load
8. **Step 8**: Load second dataset (Ingesta2) to staging
9. **Step 9**: Update data warehouse with Ingesta2
10. **Step 10**: Create final data products

### Key Implementation Details

- **Database Path**: All database operations use the constant `DB_PATH = ".data/tp_dwa.db"`
- **Logging**: Comprehensive logging system implemented across all modules with INFO level by default
- **Error Handling**: Each step includes try-catch blocks for database operations
- **Transaction Management**: Steps use SQLite transactions for data consistency

### Data Flow Layers

The project implements a multi-layered data architecture:
- **Staging (TMP_)**: Raw data ingestion from external sources
- **Ingestion (ING_)**: Cleansed data with referential integrity
- **Data Warehouse (DWH_)**: Dimensional model for analytics
- **Data Quality Mart (DQM_)**: Quality metrics and monitoring
- **Metadata (MET_)**: Process and data lineage tracking

### Development Notes

- The project is designed to work with Northwind-style transactional data
- All SQL operations use standard SQLite syntax
- Each step can be run independently after its prerequisites are met
- The orchestrator provides detailed logging for monitoring ETL progress