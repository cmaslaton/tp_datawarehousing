import sqlite3
import logging
import time
from tp_datawarehousing.quality_utils import (
    get_process_execution_id, 
    update_process_execution,
    log_quality_metric,
    validate_table_count,
    validate_no_nulls,
    validate_referential_integrity,
    log_record_count
)

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)


def connect_with_retry(db_path: str, max_retries: int = 5, delay: float = 1.0) -> sqlite3.Connection:
    """
    Establece conexión a SQLite con reintentos para evitar database locked.
    """
    for attempt in range(max_retries):
        try:
            conn = sqlite3.connect(db_path, timeout=30.0)
            conn.execute("PRAGMA busy_timeout = 30000;")  # 30 segundos
            conn.execute("PRAGMA journal_mode = WAL;")    # Write-Ahead Logging
            conn.execute("PRAGMA synchronous = NORMAL;")  # Balance rendimiento/seguridad
            return conn
        except sqlite3.OperationalError as e:
            if "database is locked" in str(e) and attempt < max_retries - 1:
                logging.warning(f"BD bloqueada, reintentando en {delay}s... (intento {attempt + 1}/{max_retries})")
                time.sleep(delay)
                delay *= 1.5  # Backoff exponencial
            else:
                raise
    raise sqlite3.OperationalError("No se pudo conectar después de múltiples intentos")

DB_PATH = "db/tp_dwa.db"

TABLE_CREATION_QUERIES = {
    "ING_regions": "CREATE TABLE IF NOT EXISTS ING_regions (region_id INTEGER PRIMARY KEY, region_description TEXT NOT NULL);",
    "ING_territories": """
        CREATE TABLE IF NOT EXISTS ING_territories (
            territory_id TEXT PRIMARY KEY,
            territory_description TEXT NOT NULL,
            region_id INTEGER NOT NULL,
            FOREIGN KEY (region_id) REFERENCES ING_regions(region_id)
        );
    """,
    "ING_shippers": "CREATE TABLE IF NOT EXISTS ING_shippers (shipper_id INTEGER PRIMARY KEY, company_name TEXT NOT NULL, phone TEXT);",
    "ING_suppliers": "CREATE TABLE IF NOT EXISTS ING_suppliers (supplier_id INTEGER PRIMARY KEY, company_name TEXT NOT NULL, contact_name TEXT, contact_title TEXT, address TEXT, city TEXT, region TEXT, postal_code TEXT, country TEXT, phone TEXT, fax TEXT, home_page TEXT);",
    "ING_categories": "CREATE TABLE IF NOT EXISTS ING_categories (category_id INTEGER PRIMARY KEY, category_name TEXT NOT NULL, description TEXT, picture BLOB);",
    "ING_customers": "CREATE TABLE IF NOT EXISTS ING_customers (customer_id TEXT PRIMARY KEY, company_name TEXT NOT NULL, contact_name TEXT, contact_title TEXT, address TEXT, city TEXT, region TEXT, postal_code TEXT, country TEXT, phone TEXT, fax TEXT);",
    "ING_employees": """
        CREATE TABLE IF NOT EXISTS ING_employees (
            employee_id INTEGER PRIMARY KEY,
            last_name TEXT NOT NULL, first_name TEXT NOT NULL, title TEXT,
            title_of_courtesy TEXT, birth_date TEXT, hire_date TEXT,
            address TEXT, city TEXT, region TEXT, postal_code TEXT,
            country TEXT, home_phone TEXT, extension TEXT, photo BLOB,
            notes TEXT, reports_to INTEGER, photo_path TEXT,
            FOREIGN KEY (reports_to) REFERENCES ING_employees(employee_id)
        );
    """,
    "ING_products": """
        CREATE TABLE IF NOT EXISTS ING_products (
            product_id INTEGER PRIMARY KEY, product_name TEXT NOT NULL,
            supplier_id INTEGER, category_id INTEGER, quantity_per_unit TEXT,
            unit_price REAL, units_in_stock INTEGER, units_on_order INTEGER,
            reorder_level INTEGER, discontinued INTEGER,
            FOREIGN KEY (category_id) REFERENCES ING_categories(category_id),
            FOREIGN KEY (supplier_id) REFERENCES ING_suppliers(supplier_id)
        );
    """,
    "ING_orders": """
        CREATE TABLE IF NOT EXISTS ING_orders (
            order_id INTEGER PRIMARY KEY, customer_id TEXT, employee_id INTEGER,
            order_date TEXT, required_date TEXT, shipped_date TEXT,
            ship_via INTEGER, freight REAL, ship_name TEXT, ship_address TEXT,
            ship_city TEXT, ship_region TEXT, ship_postal_code TEXT, ship_country TEXT,
            FOREIGN KEY (customer_id) REFERENCES ING_customers(customer_id),
            FOREIGN KEY (employee_id) REFERENCES ING_employees(employee_id),
            FOREIGN KEY (ship_via) REFERENCES ING_shippers(shipper_id)
        );
    """,
    "ING_order_details": """
        CREATE TABLE IF NOT EXISTS ING_order_details (
            order_id INTEGER NOT NULL, product_id INTEGER NOT NULL,
            unit_price REAL NOT NULL, quantity INTEGER NOT NULL, discount REAL NOT NULL,
            PRIMARY KEY (order_id, product_id),
            FOREIGN KEY (order_id) REFERENCES ING_orders(order_id),
            FOREIGN KEY (product_id) REFERENCES ING_products(product_id)
        );
    """,
    "ING_employee_territories": """
        CREATE TABLE IF NOT EXISTS ING_employee_territories (
            employee_id INTEGER NOT NULL,
            territory_id TEXT NOT NULL,
            PRIMARY KEY (employee_id, territory_id),
            FOREIGN KEY (employee_id) REFERENCES ING_employees(employee_id),
            FOREIGN KEY (territory_id) REFERENCES ING_territories(territory_id)
        );
    """,
    "ING_world_data_2023": """
        CREATE TABLE IF NOT EXISTS ING_world_data_2023 (
            country TEXT, density REAL, abbreviation TEXT, agricultural_land REAL,
            land_area REAL, armed_forces_size TEXT, birth_rate REAL, calling_code INTEGER,
            capital_major_city TEXT, co2_emissions REAL, cpi REAL, cpi_change REAL,
            currency_code TEXT, fertility_rate REAL, forested_area REAL,
            gasoline_price REAL, gdp REAL, gross_primary_education_enrollment REAL,
            gross_tertiary_education_enrollment REAL, infant_mortality REAL,
            largest_city TEXT, life_expectancy REAL, maternal_mortality_ratio INTEGER,
            minimum_wage REAL, official_language TEXT, out_of_pocket_health_expenditure REAL,
            physicians_per_thousand REAL, population REAL, population_labor_force_participation REAL,
            tax_revenue REAL, total_tax_rate REAL, unemployment_rate REAL,
            urban_population REAL, latitude REAL, longitude REAL
        );
    """,
}

# El orden es crucial para respetar las dependencias de FK
INSERTION_ORDER = [
    "ING_regions",
    "ING_shippers",
    "ING_suppliers",
    "ING_categories",
    "ING_customers",
    "ING_employees",
    "ING_territories",
    "ING_products",
    "ING_orders",
    "ING_order_details",
    "ING_employee_territories",
    "ING_world_data_2023",
]


def create_and_load_ingestion_layer():
    """
    Crea y puebla la capa de Ingesta (ING_) a partir de la capa Temporal (TMP_),
    asegurando la integridad de los datos.
    Incluye controles de calidad completos y registro en DQM.
    """
    # Inicializar tracking de calidad
    execution_id = get_process_execution_id("STEP_03_CREATE_INGESTION")
    
    conn = None
    try:
        conn = connect_with_retry(DB_PATH)
        cursor = conn.cursor()
        cursor.execute("PRAGMA foreign_keys = ON;")
        
        log_quality_metric(execution_id, "FOREIGN_KEYS_ENABLED", "DATABASE", "PASS", 
                         "Foreign keys habilitadas correctamente")

        logging.info("Creando tablas de la capa de Ingesta (ING_)...")
        for table in INSERTION_ORDER:
            cursor.execute(TABLE_CREATION_QUERIES[table])
        conn.commit()
        logging.info("Tablas ING_ creadas con éxito.")
        
        log_quality_metric(execution_id, "TABLES_CREATION", "SCHEMA", str(len(INSERTION_ORDER)), 
                         f"Creadas {len(INSERTION_ORDER)} tablas ING_")

        # Validar que las tablas TMP_ origen existan y tengan datos
        validate_source_tables(execution_id, cursor)

        logging.info("Vaciando tablas ING_ antes de la carga...")
        for table in reversed(
            INSERTION_ORDER
        ):  # Vaciar en orden inverso para no violar FKs
            cursor.execute(f"DELETE FROM {table};")
        conn.commit()

        logging.info("Cargando datos de TMP_ a ING_...")
        successful_loads = 0
        
        for table in INSERTION_ORDER:
            source_table = table.replace("ING_", "TMP_")
            logging.info(f"Cargando {source_table} -> {table}")

            # Contar registros antes de la carga
            cursor.execute(f"SELECT COUNT(*) FROM {source_table}")
            source_count = cursor.fetchone()[0]
            
            if source_count == 0:
                log_quality_metric(execution_id, "EMPTY_SOURCE_TABLE", source_table, "WARNING", 
                                 f"Tabla origen {source_table} está vacía")
                continue

            try:
                if table == "ING_employees":
                    # Caso especial para limpiar 'reports_to'
                    cursor.execute(
                        f"""
                        INSERT INTO ING_employees
                        SELECT 
                            e.employee_id, e.last_name, e.first_name, e.title, e.title_of_courtesy,
                            e.birth_date, e.hire_date, e.address, e.city, e.region, e.postal_code,
                            e.country, e.home_phone, e.extension, e.photo, e.notes,
                            CASE 
                                WHEN e.reports_to IN (SELECT employee_id FROM TMP_employees) THEN e.reports_to
                                ELSE NULL
                            END,
                            e.photo_path
                        FROM {source_table} e;
                    """
                    )
                    
                    # Validar limpieza de FK huérfanas
                    cursor.execute("""
                        SELECT COUNT(*) FROM TMP_employees 
                        WHERE reports_to IS NOT NULL 
                        AND reports_to NOT IN (SELECT employee_id FROM TMP_employees)
                    """)
                    orphan_fks = cursor.fetchone()[0]
                    if orphan_fks > 0:
                        log_quality_metric(execution_id, "FK_CLEANUP", "ING_employees", "PERFORMED", 
                                         f"Se limpiaron {orphan_fks} FKs huérfanas en reports_to")
                else:
                    # Caso general para el resto de las tablas
                    conn.execute("BEGIN IMMEDIATE;")
                    try:
                        cursor.execute(f"INSERT INTO {table} SELECT * FROM {source_table};")
                        conn.commit()
                    except Exception as e:
                        conn.rollback()
                        raise e
                
                # Validar la carga
                cursor.execute(f"SELECT COUNT(*) FROM {table}")
                target_count = cursor.fetchone()[0]
                
                log_record_count(execution_id, "TRANSFERRED", table, target_count)
                
                # Validar que los conteos coincidan (excepto para employees que puede tener limpieza)
                if table != "ING_employees" and source_count != target_count:
                    log_quality_metric(execution_id, "COUNT_MISMATCH", table, "FAIL", 
                                     f"Origen: {source_count}, Destino: {target_count}")
                else:
                    log_quality_metric(execution_id, "COUNT_VALIDATION", table, "PASS", 
                                     f"Registros transferidos correctamente: {target_count}")
                
                successful_loads += 1
                logging.info(f"Carga de {table} completada: {target_count} registros.")
                
            except sqlite3.Error as e:
                log_quality_metric(execution_id, "LOAD_ERROR", table, "FAIL", f"Error SQL: {str(e)}")
                logging.error(f"Error cargando {table}: {e}")
                raise

        # Validaciones post-carga
        validate_ingestion_integrity(execution_id, cursor)
        
        # Resumen final
        log_quality_metric(execution_id, "INGESTION_SUMMARY", "PROCESS", f"{successful_loads}/{len(INSERTION_ORDER)}", 
                         f"Tablas cargadas exitosamente: {successful_loads}")

        if successful_loads == len(INSERTION_ORDER):
            update_process_execution(execution_id, "Exitoso", 
                                   f"Capa de ingesta creada: {successful_loads} tablas")
        else:
            update_process_execution(execution_id, "Parcialmente Exitoso", 
                                   f"Carga parcial: {successful_loads}/{len(INSERTION_ORDER)} tablas")

        logging.info("Capa de Ingesta (ING_) creada y cargada exitosamente.")

    except sqlite3.Error as e:
        logging.error(
            f"Error en la base de datos durante la creación de la capa de ingesta: {e}"
        )
        log_quality_metric(execution_id, "DATABASE_ERROR", "PROCESS", "FAIL", f"Error SQL: {str(e)}")
        update_process_execution(execution_id, "Fallido", f"Error de base de datos: {str(e)}")
        if conn:
            conn.rollback()
    finally:
        if conn:
            conn.close()
            logging.info("Conexión a la base de datos cerrada.")


def validate_source_tables(execution_id: int, cursor: sqlite3.Cursor):
    """
    Valida que las tablas TMP_ origen existan y tengan datos.
    """
    tmp_tables_missing = []
    tmp_tables_empty = []
    
    for table in INSERTION_ORDER:
        source_table = table.replace("ING_", "TMP_")
        
        # Verificar que la tabla existe
        cursor.execute("""
            SELECT COUNT(*) FROM sqlite_master 
            WHERE type='table' AND name=?
        """, (source_table,))
        
        table_exists = cursor.fetchone()[0] > 0
        
        if not table_exists:
            tmp_tables_missing.append(source_table)
        else:
            # Verificar que tenga datos
            cursor.execute(f"SELECT COUNT(*) FROM {source_table}")
            count = cursor.fetchone()[0]
            if count == 0:
                tmp_tables_empty.append(source_table)
    
    if tmp_tables_missing:
        log_quality_metric(execution_id, "MISSING_SOURCE_TABLES", "VALIDATION", "FAIL", 
                         f"Tablas TMP_ faltantes: {', '.join(tmp_tables_missing)}")
    
    if tmp_tables_empty:
        log_quality_metric(execution_id, "EMPTY_SOURCE_TABLES", "VALIDATION", "WARNING", 
                         f"Tablas TMP_ vacías: {', '.join(tmp_tables_empty)}")
    
    if not tmp_tables_missing and not tmp_tables_empty:
        log_quality_metric(execution_id, "SOURCE_TABLES_VALIDATION", "VALIDATION", "PASS", 
                         "Todas las tablas TMP_ están disponibles y contienen datos")


def validate_ingestion_integrity(execution_id: int, cursor: sqlite3.Cursor):
    """
    Valida la integridad de los datos en la capa de ingesta.
    """
    # Validar integridad referencial crítica
    critical_fks = [
        ("ING_territories", "ING_regions", "region_id", "region_id"),
        ("ING_products", "ING_categories", "category_id", "category_id"),
        ("ING_products", "ING_suppliers", "supplier_id", "supplier_id"),
        ("ING_orders", "ING_customers", "customer_id", "customer_id"),
        ("ING_orders", "ING_employees", "employee_id", "employee_id"),
        ("ING_orders", "ING_shippers", "ship_via", "shipper_id"),
        ("ING_order_details", "ING_orders", "order_id", "order_id"),
        ("ING_order_details", "ING_products", "product_id", "product_id"),
    ]
    
    integrity_failures = 0
    for child_table, parent_table, fk_column, pk_column in critical_fks:
        is_valid = validate_referential_integrity(execution_id, child_table, parent_table, fk_column, pk_column)
        if not is_valid:
            integrity_failures += 1
    
    # Validar campos obligatorios críticos
    critical_not_nulls = [
        ("ING_customers", "customer_id"),
        ("ING_products", "product_name"),
        ("ING_orders", "order_date"),
        ("ING_order_details", "unit_price"),
        ("ING_order_details", "quantity"),
    ]
    
    null_failures = 0
    for table, column in critical_not_nulls:
        is_valid = validate_no_nulls(execution_id, table, column)
        if not is_valid:
            null_failures += 1
    
    # Resumen de validación de integridad
    total_validations = len(critical_fks) + len(critical_not_nulls)
    total_failures = integrity_failures + null_failures
    
    if total_failures == 0:
        log_quality_metric(execution_id, "INTEGRITY_VALIDATION", "INGESTION_LAYER", "PASS", 
                         f"Todas las validaciones de integridad pasaron ({total_validations} checks)")
    else:
        log_quality_metric(execution_id, "INTEGRITY_VALIDATION", "INGESTION_LAYER", "FAIL", 
                         f"Fallos de integridad: {total_failures}/{total_validations}")
    
    # Conteos finales por tabla
    total_records = 0
    for table in INSERTION_ORDER:
        cursor.execute(f"SELECT COUNT(*) FROM {table}")
        count = cursor.fetchone()[0]
        total_records += count
    
    log_quality_metric(execution_id, "TOTAL_INGESTION_RECORDS", "INGESTION_LAYER", str(total_records), 
                     f"Total de registros en capa de ingesta: {total_records}")


def main():
    create_and_load_ingestion_layer()


if __name__ == "__main__":
    main()
