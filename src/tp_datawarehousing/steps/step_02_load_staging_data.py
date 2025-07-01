import pandas as pd
import sqlite3
import logging
import os
import re
from pathlib import Path
from tp_datawarehousing.quality_utils import (
    get_process_execution_id, 
    update_process_execution,
    log_quality_metric,
    validate_table_count,
    log_record_count
)

# --- Configuración de Logging ---
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)

# --- Constantes ---
DB_PATH = "db/tp_dwa.db"
INGESTA_PATH = ".data/ingesta1"

# Mapeo de archivos CSV a nombres de tablas en la BD
TABLE_MAPPING = {
    "categories.csv": "TMP_categories",
    "products.csv": "TMP_products",
    "suppliers.csv": "TMP_suppliers",
    "order_details.csv": "TMP_order_details",
    "orders.csv": "TMP_orders",
    "customers.csv": "TMP_customers",
    "employees.csv": "TMP_employees",
    "shippers.csv": "TMP_shippers",
    "territories.csv": "TMP_territories",
    "employee_territories.csv": "TMP_employee_territories",
    "regions.csv": "TMP_regions",
    "world-data-2023.csv": "TMP_world_data_2023",
}


def normalize_column_name(col_name: str) -> str:
    """
    Convierte un nombre de columna a formato snake_case y limpia caracteres especiales.
    E.g., 'categoryID' -> 'category_id', 'Density (P/Km2)' -> 'density'
    """
    if "(" in col_name:
        col_name = col_name.split("(")[0].strip()

    # 2. Reemplazar espacios y caracteres no alfanuméricos por guion bajo
    # 'CPI Change' -> 'CPI_Change', 'world-data' -> 'world_data', 'Capital/Major City' -> 'Capital_Major_City'
    name = re.sub(r"[-\s\./:]+", "_", col_name)

    # 3. Insertar guion bajo antes de mayúsculas en camelCase (productName -> product_Name)
    # y antes de 'id' en casos como 'shipperid' -> 'shipper_id'.
    # No afecta a 'ID' en mayúsculas.
    name = re.sub(r"([a-z\d])([A-Z])", r"\1_\2", name)
    name = re.sub(r"([a-zA-Z])(id)$", r"\1_\2", name, flags=re.IGNORECASE)

    # 4. Convertir todo a minúsculas
    name = name.lower()

    # 5. Manejar casos especiales que el regex no cubre bien, como 'territorydescription'.
    # Esto asegura que palabras compuestas en minúsculas se separen correctamente.
    special_cases = {
        "companyname": "company_name",
        "territorydescription": "territory_description",
        "regiondescription": "region_description",
    }
    if name in special_cases:
        name = special_cases[name]

    # 6. Limpiar guiones bajos duplicados que puedan haberse formado
    name = re.sub(r"__+", "_", name)

    return name


def load_csv_to_table(file_path: Path, table_name: str, conn: sqlite3.Connection, execution_id: int):
    """
    Carga los datos de un archivo CSV a una tabla específica en la base de datos,
    normalizando los nombres de las columnas antes de la inserción.
    Incluye controles de calidad y logging en DQM.
    """
    try:
        logging.info(f"Procesando archivo: {file_path.name} -> Tabla: {table_name}")

        # Limpiar la tabla de staging antes de la carga para evitar duplicados
        cursor = conn.cursor()
        cursor.execute(f"DELETE FROM {table_name};")
        conn.commit()
        logging.info(f"Tabla {table_name} vaciada antes de la nueva carga.")

        # Intenta leer con UTF-8, si falla, prueba con latin-1
        try:
            df = pd.read_csv(file_path)
            log_quality_metric(execution_id, "FILE_ENCODING", file_path.name, "UTF-8", 
                             "Archivo leído correctamente con encoding UTF-8")
        except UnicodeDecodeError:
            logging.warning(
                f"Error de decodificación UTF-8 en {file_path.name}. Intentando con 'latin-1'."
            )
            df = pd.read_csv(file_path, encoding="latin-1")
            log_quality_metric(execution_id, "FILE_ENCODING", file_path.name, "LATIN-1", 
                             "Archivo requirió encoding latin-1")

        # Control de calidad: Validar que el DataFrame no esté vacío
        if df.empty:
            log_quality_metric(execution_id, "EMPTY_FILE_CHECK", file_path.name, "FAIL", 
                             "El archivo CSV está vacío")
            logging.error(f"El archivo {file_path.name} está vacío")
            return False
        else:
            log_quality_metric(execution_id, "EMPTY_FILE_CHECK", file_path.name, "PASS", 
                             f"Archivo contiene {len(df)} registros")

        # Control de calidad: Validar estructura básica del archivo
        log_quality_metric(execution_id, "COLUMN_COUNT", file_path.name, str(len(df.columns)), 
                         f"Número de columnas: {len(df.columns)}")
        
        # Control de calidad: Verificar duplicados por filas completas
        duplicates = df.duplicated().sum()
        if duplicates > 0:
            log_quality_metric(execution_id, "DUPLICATE_ROWS", file_path.name, "WARNING", 
                             f"Se encontraron {duplicates} filas duplicadas")
        else:
            log_quality_metric(execution_id, "DUPLICATE_ROWS", file_path.name, "PASS", 
                             "No se encontraron filas duplicadas")

        # Normalizar los nombres de las columnas del DataFrame
        original_columns = df.columns
        df.columns = [normalize_column_name(col) for col in original_columns]
        renamed_cols_map = {
            orig: new for orig, new in zip(original_columns, df.columns) if orig != new
        }
        if renamed_cols_map:
            logging.info(f"Columnas renombradas: {renamed_cols_map}")
            # Registrar conteo de columnas renombradas
            log_quality_metric(execution_id, "COLUMN_NORMALIZATION", file_path.name, "PERFORMED", 
                             f"Columnas renombradas: {len(renamed_cols_map)}")
            # Registrar mapeo detallado para trazabilidad completa
            import json
            log_quality_metric(execution_id, "COLUMN_NORMALIZATION_DETAIL", file_path.name, "MAPPING", 
                             json.dumps(renamed_cols_map, ensure_ascii=False))

        # Control de calidad: Validar tipos de datos críticos
        validate_data_types(df, table_name, file_path.name, execution_id)

        # Cargar datos en la tabla. 'append' añade los datos, 'replace' borraría la tabla primero.
        records_before = len(df)
        df.to_sql(table_name, conn, if_exists="append", index=False)
        
        # Registrar la carga exitosa
        log_record_count(execution_id, "LOADED", table_name, records_before)
        logging.info(f"Carga exitosa de {records_before} registros en la tabla {table_name}.")
        
        # Control de calidad post-carga: Verificar que la tabla tenga los registros esperados
        validate_table_count(execution_id, table_name, records_before)
        
        return True
    except Exception as e:
        logging.error(
            f"Error al cargar el archivo {file_path.name} a la tabla {table_name}: {e}"
        )
        log_quality_metric(execution_id, "LOAD_ERROR", file_path.name, "FAIL", f"Error: {str(e)}")
        return False


def validate_data_types(df: pd.DataFrame, table_name: str, file_name: str, execution_id: int):
    """
    Valida tipos de datos específicos según la tabla.
    """
    # Validaciones específicas por tabla
    if "order_details" in table_name:
        # Validar que unit_price y quantity sean numéricos positivos
        if 'unit_price' in df.columns:
            negative_prices = (df['unit_price'] < 0).sum()
            if negative_prices > 0:
                log_quality_metric(execution_id, "NEGATIVE_PRICE", table_name, "FAIL", 
                                 f"Se encontraron {negative_prices} precios negativos")
            else:
                log_quality_metric(execution_id, "NEGATIVE_PRICE", table_name, "PASS", 
                                 "Todos los precios son válidos")
        
        if 'quantity' in df.columns:
            zero_quantities = (df['quantity'] <= 0).sum()
            if zero_quantities > 0:
                log_quality_metric(execution_id, "ZERO_QUANTITY", table_name, "WARNING", 
                                 f"Se encontraron {zero_quantities} cantidades <= 0")
    
    elif "products" in table_name:
        # Validar precios de productos
        if 'unit_price' in df.columns:
            null_prices = df['unit_price'].isnull().sum()
            if null_prices > 0:
                log_quality_metric(execution_id, "NULL_PRICE", table_name, "WARNING", 
                                 f"Se encontraron {null_prices} productos sin precio")
    
    elif "orders" in table_name:
        # Validar fechas de órdenes
        if 'order_date' in df.columns:
            null_dates = df['order_date'].isnull().sum()
            if null_dates > 0:
                log_quality_metric(execution_id, "NULL_ORDER_DATE", table_name, "FAIL", 
                                 f"Se encontraron {null_dates} órdenes sin fecha")
            else:
                log_quality_metric(execution_id, "NULL_ORDER_DATE", table_name, "PASS", 
                                 "Todas las órdenes tienen fecha")
    
    # Validación general: Conteo de valores nulos por columna
    for column in df.columns:
        null_count = df[column].isnull().sum()
        if null_count > 0:
            percentage = (null_count / len(df)) * 100
            result = "WARNING" if percentage < 10 else "FAIL"
            log_quality_metric(execution_id, "NULL_VALUES", f"{table_name}.{column}", result, 
                             f"Valores nulos: {null_count} ({percentage:.1f}%)")


def load_all_staging_data():
    """
    Orquesta la carga de todos los archivos CSV de Ingesta1 a sus tablas de staging.
    Incluye controles de calidad completos y registro en DQM.
    """
    logging.info("Iniciando la carga de datos de Ingesta1 al área de Staging.")
    
    # Inicializar tracking de calidad
    execution_id = get_process_execution_id("STEP_02_LOAD_STAGING")
    success_count = 0
    total_files = len(TABLE_MAPPING)

    if not Path(DB_PATH).exists():
        logging.error(
            f"La base de datos {DB_PATH} no existe. Ejecute el paso 01 primero."
        )
        log_quality_metric(execution_id, "DATABASE_EXISTS", "DB_FILE", "FAIL", 
                         f"Base de datos no encontrada: {DB_PATH}")
        update_process_execution(execution_id, "Fallido", "Base de datos no encontrada")
        return

    try:
        conn = sqlite3.connect(DB_PATH)
        logging.info(f"Conexión exitosa a la base de datos {DB_PATH}.")
        log_quality_metric(execution_id, "DATABASE_CONNECTION", "DB_FILE", "PASS", 
                         "Conexión exitosa a la base de datos")

        ingesta_dir = Path(INGESTA_PATH)
        if not ingesta_dir.exists():
            logging.error(f"El directorio de ingesta {INGESTA_PATH} no se encuentra.")
            log_quality_metric(execution_id, "INGESTA_DIR_EXISTS", "DIRECTORY", "FAIL", 
                             f"Directorio no encontrado: {INGESTA_PATH}")
            update_process_execution(execution_id, "Fallido", "Directorio de ingesta no encontrado")
            return
        else:
            log_quality_metric(execution_id, "INGESTA_DIR_EXISTS", "DIRECTORY", "PASS", 
                             f"Directorio encontrado: {INGESTA_PATH}")

        # Validar que todos los archivos esperados estén presentes
        missing_files = []
        existing_files = []
        for csv_file in TABLE_MAPPING.keys():
            file_path = ingesta_dir / csv_file
            if file_path.exists():
                existing_files.append(csv_file)
            else:
                missing_files.append(csv_file)

        if missing_files:
            log_quality_metric(execution_id, "MISSING_FILES", "FILE_VALIDATION", "WARNING", 
                             f"Archivos faltantes: {', '.join(missing_files)}")
        
        log_quality_metric(execution_id, "AVAILABLE_FILES", "FILE_VALIDATION", str(len(existing_files)), 
                         f"Archivos disponibles: {len(existing_files)}/{total_files}")

        # Procesar cada archivo
        for csv_file, table_name in TABLE_MAPPING.items():
            file_path = ingesta_dir / csv_file
            if file_path.exists():
                success = load_csv_to_table(file_path, table_name, conn, execution_id)
                if success:
                    success_count += 1
            else:
                logging.warning(
                    f"El archivo {csv_file} no se encontró en {INGESTA_PATH}. Se omite."
                )

        # Resumen final de la carga
        log_quality_metric(execution_id, "LOAD_SUMMARY", "STAGING_PROCESS", 
                         f"{success_count}/{len(existing_files)}", 
                         f"Archivos cargados exitosamente: {success_count} de {len(existing_files)} disponibles")

        # Validación post-carga: Verificar que todas las tablas TMP_ tengan datos
        validate_staging_completeness(execution_id, conn)

        if success_count == len(existing_files):
            update_process_execution(execution_id, "Exitoso", 
                                   f"Carga completada: {success_count} archivos procesados")
        else:
            update_process_execution(execution_id, "Parcialmente Exitoso", 
                                   f"Carga parcial: {success_count}/{len(existing_files)} archivos")

    except sqlite3.Error as e:
        logging.error(f"Error de base de datos durante la carga de staging: {e}")
        log_quality_metric(execution_id, "DATABASE_ERROR", "DB_OPERATION", "FAIL", f"Error SQL: {str(e)}")
        update_process_execution(execution_id, "Fallido", f"Error de base de datos: {str(e)}")
    finally:
        if conn:
            conn.close()
            logging.info("Conexión a la base de datos cerrada.")


def validate_staging_completeness(execution_id: int, conn: sqlite3.Connection):
    """
    Valida que todas las tablas de staging tengan datos después de la carga.
    """
    cursor = conn.cursor()
    
    # Obtener todas las tablas TMP_
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name LIKE 'TMP_%'")
    staging_tables = [row[0] for row in cursor.fetchall()]
    
    empty_tables = []
    total_records = 0
    
    for table in staging_tables:
        cursor.execute(f"SELECT COUNT(*) FROM {table}")
        count = cursor.fetchone()[0]
        total_records += count
        
        if count == 0:
            empty_tables.append(table)
    
    if empty_tables:
        log_quality_metric(execution_id, "EMPTY_STAGING_TABLES", "STAGING_VALIDATION", "WARNING", 
                         f"Tablas vacías: {', '.join(empty_tables)}")
    else:
        log_quality_metric(execution_id, "EMPTY_STAGING_TABLES", "STAGING_VALIDATION", "PASS", 
                         "Todas las tablas de staging contienen datos")
    
    log_quality_metric(execution_id, "TOTAL_STAGING_RECORDS", "STAGING_VALIDATION", str(total_records), 
                     f"Total de registros cargados en staging: {total_records}")


if __name__ == "__main__":
    load_all_staging_data()
