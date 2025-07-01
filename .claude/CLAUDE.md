# CLAUDE.md - Data Warehousing Academic Project

## Project Overview

This is a comprehensive **Data Warehousing Academic Project** (`tp_datawarehousing`) that implements a complete end-to-end data warehouse solution using the Northwind database as the primary data source. The project demonstrates professional data warehousing practices including ETL processes, dimensional modeling, data quality management, and analytical reporting.

## Technology Stack

- **Language**: Python 3.11+
- **Database**: SQLite (single-file database: `db/tp_dwa.db`)
- **Core Libraries**: 
  - `pandas` 2.3.0 - Data manipulation and analysis
  - `numpy` 2.3.0 - Numerical computing
  - `sqlite3` - Database operations (built-in)
- **Package Management**: 
  - `pyproject.toml` (PEP 621 standard)
  - `uv.lock` (modern Python dependency resolution)
- **Architecture**: Modular ETL pipeline with orchestration

## Project Structure

```
tp_datawarehousing/
├── db/                          # Database files
│   └── tp_dwa.db               # Main SQLite database
├── docs/                        # Documentation
│   ├── DER.png                 # Entity Relationship Diagram
│   ├── step_01_analysis.md     # Step-by-step analysis docs
│   └── ... (step_02 through step_10)
├── src/tp_datawarehousing/     # Main source code
│   ├── main.py                 # Main orchestrator
│   ├── quality_utils.py        # Data quality utilities
│   └── steps/                  # Modular ETL steps
│       ├── step_01_setup_staging_area.py
│       ├── step_02_load_staging_data.py
│       ├── step_03_create_ingestion_layer.py
│       ├── step_04_link_world_data.py
│       ├── step_05_create_dwh_model.py
│       ├── step_06_create_dqm.py
│       ├── step_07_initial_dwh_load.py
│       ├── step_08_load_ingesta2_to_staging.py
│       ├── step_09_update_dwh_with_ingesta2.py
│       ├── step_10_1_ventas_mensuales_categoria_pais.py
│       ├── step_10_2_performance_empleados_trimestral.py
│       └── step_10_3_analisis_logistica_shippers.py
├── check_quality_metrics.py    # Quality metrics reporter
├── pyproject.toml              # Project configuration
├── requirements.txt            # Dependencies
└── README.md                   # Project documentation
```

## Data Architecture

The project implements a **comprehensive data warehouse architecture** with the following layers:

### 1. Staging Layer (TMP_*)
- **TMP_categories, TMP_products, TMP_suppliers** - Master data entities
- **TMP_orders, TMP_order_details** - Transaction data
- **TMP_customers, TMP_employees** - Business actors
- **TMP_shippers, TMP_territories, TMP_regions** - Geographic and logistics
- **TMP_world_data_2023** - External enrichment data

### 2. Ingestion Layer (ING_*)
- Clean, validated data with integrity constraints
- Business rules enforcement
- Data type standardization

### 3. Data Warehouse Layer (DWH_*)
- **Dimensional Model**: Star schema design
- **6 Dimensions**: Time, Customers (SCD Type 2), Products, Employees, Geography, Shippers
- **1 Fact Table**: Sales facts with derived metrics
- **SCD Type 2**: Slowly Changing Dimensions for customer history

### 4. Data Quality Mart (DQM_*)
- **DQM_ejecucion_procesos** - Process execution tracking
- **DQM_indicadores_calidad** - Quality metrics and validation results
- Comprehensive quality monitoring and reporting

## Key Features

### ETL Pipeline Orchestration
- **Modular Design**: Each ETL step is a separate, testable module
- **Sequential Execution**: Orchestrated through `main.py`
- **Error Handling**: Robust error management with rollback capabilities
- **Logging**: Comprehensive logging throughout the pipeline

### Data Quality Management
- **Built-in Quality Utils**: Comprehensive data validation framework
- **Multiple Validation Types**:
  - Record count validation
  - NULL value checks
  - Referential integrity validation
  - Data range validation
  - Custom business rule validation
- **Quality Metrics Tracking**: All validations logged in DQM tables
- **Retry Logic**: Robust database operation retry mechanism with exponential backoff

### Analytical Capabilities
- **3 Data Products**:
  - Monthly sales by category and country
  - Quarterly employee performance analysis
  - Logistics and shipping analysis
- **Dimensional Modeling**: Optimized for analytical queries
- **Historical Tracking**: SCD Type 2 implementation for customer changes

## Build and Run Commands

### Installation
```bash
# Install in development mode
pip install -e .

# Or install dependencies directly
pip install -r requirements.txt
```

### Execution
```bash
# Run the complete ETL pipeline
python -m tp_datawarehousing.main

# Or use the installed entry point
tp-datawarehousing

# Check quality metrics after execution
python check_quality_metrics.py
```

### Database Access
```bash
# Access the SQLite database directly
sqlite3 db/tp_dwa.db
```

## Development Patterns

### Code Organization
- **Separation of Concerns**: Each step handles a specific ETL phase
- **Configuration Management**: Database path and settings centralized
- **Professional Structure**: Follows Python package best practices
- **Metadata Management**: Institutional memory through MET_* tables

### Error Handling
- **Transaction Safety**: Operations wrapped in transactions with rollback
- **Retry Logic**: Automatic retry for database lock situations
- **Quality Gates**: Each step validates data before proceeding
- **Comprehensive Logging**: All operations logged with timestamps

### Data Quality Patterns
- **Validation Functions**: Reusable validation utilities
- **Quality Metrics**: Quantitative quality measurement
- **Process Tracking**: End-to-end process monitoring
- **Failure Recovery**: Graceful handling of data quality issues

## Academic Context

This project is designed as a **comprehensive academic exercise** demonstrating:
- Professional data warehousing methodologies
- ETL best practices
- Data quality management
- Dimensional modeling techniques
- Business intelligence foundations

The project follows academic requirements while implementing industry-standard practices, making it suitable for both learning and professional reference.

## Quality Assurance

- **Data Validation**: Multi-layer validation at each ETL step
- **Process Monitoring**: Complete execution tracking
- **Quality Reporting**: Automated quality metrics dashboard
- **Error Recovery**: Robust error handling and recovery mechanisms
- **Performance Optimization**: Database tuning and connection management

## Usage Notes

1. **Database Creation**: The pipeline automatically creates the SQLite database
2. **Idempotency**: Steps can be re-run safely (tables recreated as needed)
3. **Quality Monitoring**: Use `check_quality_metrics.py` to review data quality
4. **Modular Execution**: Individual steps can be run independently for development
5. **Professional Logging**: All operations logged with appropriate detail levels

This project represents a complete, production-ready data warehouse implementation suitable for academic study and professional reference.