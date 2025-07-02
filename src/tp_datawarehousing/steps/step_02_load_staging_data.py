import pandas as pd
import sqlite3
import logging
import os
import re
import time
from pathlib import Path
from tp_datawarehousing.utils.quality_utils import (
    get_process_execution_id, 
    update_process_execution,
    log_quality_metric,
    validate_table_count,
    log_record_count,
    QualityResult,
    QualitySeverity,
    QualityThresholds,
    validate_completeness_score,
    validate_format_patterns,
    validate_business_key_uniqueness,
    validate_cross_field_logic,
    validate_domain_constraints
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


def load_csv_to_table(conn: sqlite3.Connection, file_path: Path, table_name: str, execution_id: int):
    """
    Carga los datos de un archivo CSV a una tabla específica en la base de datos,
    normalizando los nombres de las columnas antes de la inserción.
    Incluye controles de calidad y logging en DQM.
    """
    try:
        logging.info(f"Procesando archivo: {file_path.name} -> Tabla: {table_name}")

        # Limpiar la tabla de staging antes de la carga para evitar duplicados
        cursor = conn.cursor()
        
        # Usar transacción explícita con timeout
        conn.execute("BEGIN IMMEDIATE;")
        try:
            cursor.execute(f"DELETE FROM {table_name};")
            conn.commit()
            logging.info(f"Tabla {table_name} vaciada antes de la nueva carga.")
        except Exception as e:
            conn.rollback()
            raise e

        # Intenta leer con UTF-8, si falla, prueba con latin-1
        try:
            df = pd.read_csv(file_path)
            log_quality_metric(execution_id, "FILE_ENCODING", file_path.name, "UTF-8", 
                             "Archivo leído correctamente con encoding UTF-8", QualitySeverity.LOW.value)
        except UnicodeDecodeError:
            logging.warning(
                f"Error de decodificación UTF-8 en {file_path.name}. Intentando con 'latin-1'."
            )
            df = pd.read_csv(file_path, encoding="latin-1")
            log_quality_metric(execution_id, "FILE_ENCODING", file_path.name, "LATIN-1", 
                             "Archivo requirió encoding latin-1", QualitySeverity.MEDIUM.value)

        # Control de calidad: Validar que el DataFrame no esté vacío
        if df.empty:
            log_quality_metric(execution_id, "EMPTY_FILE_CHECK", file_path.name, QualityResult.FAIL.value, 
                             "El archivo CSV está vacío", QualitySeverity.HIGH.value)
            logging.error(f"El archivo {file_path.name} está vacío")
            return False
        else:
            log_quality_metric(execution_id, "EMPTY_FILE_CHECK", file_path.name, QualityResult.PASS.value, 
                             f"Archivo contiene {len(df)} registros", QualitySeverity.LOW.value)

        # Control de calidad: Validar estructura básica del archivo
        log_quality_metric(execution_id, "COLUMN_COUNT", file_path.name, str(len(df.columns)), 
                         f"Número de columnas: {len(df.columns)}", QualitySeverity.LOW.value)
        
        # Control de calidad: Verificar duplicados por filas completas con thresholds estandarizados
        duplicates = df.duplicated().sum()
        duplicate_percentage = (duplicates / len(df)) * 100 if len(df) > 0 else 0
        
        if duplicates == 0:
            log_quality_metric(execution_id, "DUPLICATE_ROWS", file_path.name, QualityResult.PASS.value, 
                             "No se encontraron filas duplicadas", QualitySeverity.LOW.value)
        elif duplicate_percentage < QualityThresholds.DUPLICATES_WARNING:
            log_quality_metric(execution_id, "DUPLICATE_ROWS", file_path.name, QualityResult.WARNING.value, 
                             f"Se encontraron {duplicates} filas duplicadas ({duplicate_percentage:.1f}%)", QualitySeverity.MEDIUM.value)
        else:
            log_quality_metric(execution_id, "DUPLICATE_ROWS", file_path.name, QualityResult.FAIL.value, 
                             f"Se encontraron {duplicates} filas duplicadas ({duplicate_percentage:.1f}%)", QualitySeverity.HIGH.value)

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
                             f"Columnas renombradas: {len(renamed_cols_map)}", QualitySeverity.LOW.value)
            # Registrar mapeo detallado para trazabilidad completa
            import json
            log_quality_metric(execution_id, "COLUMN_NORMALIZATION_DETAIL", file_path.name, "MAPPING", 
                             json.dumps(renamed_cols_map, ensure_ascii=False), QualitySeverity.LOW.value)

        # Control de calidad: Validar tipos de datos críticos
        validate_data_types(df, table_name, file_path.name, execution_id, conn)

        # Cargar datos en la tabla con transacción explícita
        records_before = len(df)
        
        conn.execute("BEGIN IMMEDIATE;")
        try:
            df.to_sql(table_name, conn, if_exists="append", index=False)
            conn.commit()
        except Exception as e:
            conn.rollback()
            raise e
        
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


def validate_data_types(df: pd.DataFrame, table_name: str, file_name: str, execution_id: int, conn: sqlite3.Connection):
    """
    Valida tipos de datos específicos según la tabla.
    """
    # Validaciones específicas por tabla
    if "order_details" in table_name:
        # Validar que unit_price y quantity sean numéricos positivos
        if 'unit_price' in df.columns:
            negative_prices = (df['unit_price'] < 0).sum()
            if negative_prices > 0:
                log_quality_metric(execution_id, "NEGATIVE_PRICE", table_name, QualityResult.FAIL.value, 
                                 f"Se encontraron {negative_prices} precios negativos", QualitySeverity.HIGH.value)
            else:
                log_quality_metric(execution_id, "NEGATIVE_PRICE", table_name, QualityResult.PASS.value, 
                                 "Todos los precios son válidos", QualitySeverity.LOW.value)
        
        if 'quantity' in df.columns:
            zero_quantities = (df['quantity'] <= 0).sum()
            if zero_quantities > 0:
                log_quality_metric(execution_id, "ZERO_QUANTITY", table_name, QualityResult.WARNING.value, 
                                 f"Se encontraron {zero_quantities} cantidades <= 0", QualitySeverity.MEDIUM.value)
            else:
                log_quality_metric(execution_id, "ZERO_QUANTITY", table_name, QualityResult.PASS.value, 
                                 "Todas las cantidades son válidas", QualitySeverity.LOW.value)
    
    elif "products" in table_name:
        # Validar precios de productos
        if 'unit_price' in df.columns:
            null_prices = df['unit_price'].isnull().sum()
            if null_prices > 0:
                log_quality_metric(execution_id, "NULL_PRICE", table_name, QualityResult.WARNING.value, 
                                 f"Se encontraron {null_prices} productos sin precio", QualitySeverity.MEDIUM.value)
            else:
                log_quality_metric(execution_id, "NULL_PRICE", table_name, QualityResult.PASS.value, 
                                 "Todos los productos tienen precio", QualitySeverity.LOW.value)
    
    elif "orders" in table_name:
        # Validar fechas de órdenes
        if 'order_date' in df.columns:
            null_dates = df['order_date'].isnull().sum()
            if null_dates > 0:
                log_quality_metric(execution_id, "NULL_ORDER_DATE", table_name, QualityResult.FAIL.value, 
                                 f"Se encontraron {null_dates} órdenes sin fecha", QualitySeverity.CRITICAL.value)
            else:
                log_quality_metric(execution_id, "NULL_ORDER_DATE", table_name, QualityResult.PASS.value, 
                                 "Todas las órdenes tienen fecha", QualitySeverity.LOW.value)
    
    # Validación general: Conteo de valores nulos por columna con thresholds estandarizados
    for column in df.columns:
        null_count = df[column].isnull().sum()
        if null_count > 0:
            percentage = (null_count / len(df)) * 100
            if percentage < QualityThresholds.NULL_VALUES_WARNING:
                result = QualityResult.WARNING.value
                severity = QualitySeverity.MEDIUM.value
            elif percentage < QualityThresholds.NULL_VALUES_FAIL:
                result = QualityResult.WARNING.value
                severity = QualitySeverity.MEDIUM.value
            else:
                result = QualityResult.FAIL.value
                severity = QualitySeverity.HIGH.value
            
            log_quality_metric(execution_id, "NULL_VALUES", f"{table_name}.{column}", result, 
                             f"Valores nulos: {null_count} ({percentage:.1f}%)", severity)
    
    # NUEVAS VALIDACIONES AVANZADAS - MEJORES PRÁCTICAS PROFESIONALES
    
    # 1. Completeness Score para campos críticos por tabla (usando DataFrame)
    critical_fields_map = {
        "customers": ["customer_id", "company_name"],
        "orders": ["order_id", "customer_id", "order_date"],
        "order_details": ["order_id", "product_id", "unit_price", "quantity"],
        "products": ["product_id", "product_name"],
        "employees": ["employee_id", "first_name", "last_name"]
    }
    
    for table_key, fields in critical_fields_map.items():
        if table_key in table_name.lower():
            # Solo validar campos que existen en el DataFrame
            existing_fields = [f for f in fields if f in df.columns]
            if existing_fields:
                validate_dataframe_completeness(execution_id, table_name, df, existing_fields)
            break
    
    # 2. Format Validation para campos específicos
    if "customers" in table_name.lower():
        if "customer_id" in df.columns:
            validate_dataframe_format(execution_id, table_name, df, "customer_id", "customer_id")
        if "postal_code" in df.columns:
            validate_dataframe_format(execution_id, table_name, df, "postal_code", "postal_code")
    
    # 3. Business Key Uniqueness
    business_keys_map = {
        "customers": ["customer_id"],
        "products": ["product_id"],
        "orders": ["order_id"],
        "employees": ["employee_id"],
        "categories": ["category_id"],
        "suppliers": ["supplier_id"]
    }
    
    for table_key, keys in business_keys_map.items():
        if table_key in table_name.lower():
            existing_keys = [k for k in keys if k in df.columns]
            if existing_keys:
                validate_dataframe_uniqueness(execution_id, table_name, df, existing_keys)
            break
    
    # 4. Cross-field Logic Validation
    if "orders" in table_name.lower() and "order_date" in df.columns and "shipped_date" in df.columns:
        validations = [
            {
                "rule": "shipped_date >= order_date",
                "description": "Fecha de envío debe ser posterior o igual a fecha de pedido"
            }
        ]
        validate_dataframe_logic(execution_id, table_name, df, validations)
    
    # 5. Domain Constraints para campos categóricos
    if "world_data" in table_name.lower():
        # Validar códigos de continente estándar
        if "continent" in df.columns:
            valid_continents = ["Asia", "Europe", "North America", "South America", "Africa", "Oceania", "Antarctica"]
            validate_dataframe_domain(execution_id, table_name, df, "continent", valid_continents, case_sensitive=False)


def validate_dataframe_completeness(execution_id: int, table_name: str, df: pd.DataFrame, critical_fields: list):
    """Valida completitud de campos críticos en un DataFrame."""
    try:
        total_records = len(df)
        if total_records == 0:
            log_quality_metric(execution_id, "COMPLETENESS_SCORE", table_name, "WARNING", "DataFrame vacío")
            return 0.0
        
        field_completeness = {}
        for field in critical_fields:
            if field in df.columns:
                complete_count = df[field].notna().sum()
                field_percentage = (complete_count / total_records) * 100
                field_completeness[field] = field_percentage
        
        avg_completeness = sum(field_completeness.values()) / len(field_completeness) if field_completeness else 0
        
        if avg_completeness >= 95:
            result = "PASS"
            severity = QualitySeverity.LOW.value
        elif avg_completeness >= 80:
            result = "WARNING"
            severity = QualitySeverity.MEDIUM.value
        else:
            result = "FAIL"
            severity = QualitySeverity.HIGH.value
        
        field_summary = ", ".join([f"{field}: {pct:.1f}%" for field, pct in field_completeness.items()])
        detalles = f"Completitud promedio: {avg_completeness:.1f}% ({field_summary})"
        
        log_quality_metric(execution_id, "COMPLETENESS_SCORE", table_name, result, detalles, severity)
        return avg_completeness
        
    except Exception as e:
        log_quality_metric(execution_id, "COMPLETENESS_SCORE", table_name, "ERROR", f"Error: {str(e)}", QualitySeverity.HIGH.value)
        return 0.0


def validate_dataframe_uniqueness(execution_id: int, table_name: str, df: pd.DataFrame, business_keys: list):
    """Valida unicidad de claves de negocio en un DataFrame."""
    try:
        # Filtrar solo columnas que existen
        existing_keys = [k for k in business_keys if k in df.columns]
        if not existing_keys:
            log_quality_metric(execution_id, "BUSINESS_KEY_UNIQUENESS", table_name, "WARNING", "No hay claves de negocio válidas")
            return False
        
        # Contar duplicados
        duplicates = df.duplicated(subset=existing_keys, keep=False)
        duplicate_count = duplicates.sum()
        total_records = len(df)
        
        if duplicate_count == 0:
            result = "PASS"
            severity = QualitySeverity.LOW.value
            detalles = f"Todas las claves de negocio son únicas ({total_records} registros)"
        else:
            duplicate_percentage = (duplicate_count / total_records) * 100
            if duplicate_percentage < 1:
                result = "WARNING"
                severity = QualitySeverity.MEDIUM.value
            else:
                result = "FAIL"
                severity = QualitySeverity.HIGH.value
            detalles = f"Duplicados encontrados: {duplicate_count}/{total_records} ({duplicate_percentage:.1f}%)"
        
        log_quality_metric(execution_id, "BUSINESS_KEY_UNIQUENESS", table_name, result, detalles, severity)
        return duplicate_count == 0
        
    except Exception as e:
        log_quality_metric(execution_id, "BUSINESS_KEY_UNIQUENESS", table_name, "ERROR", f"Error: {str(e)}", QualitySeverity.HIGH.value)
        return False


def validate_dataframe_format(execution_id: int, table_name: str, df: pd.DataFrame, column_name: str, pattern_type: str):
    """Valida formatos en un DataFrame."""
    import re
    patterns = {
        'customer_id': r'^[A-Z]{3,5}$',
        'postal_code': r'^[\d\w\s\-]{3,10}$'
    }
    
    try:
        if pattern_type not in patterns or column_name not in df.columns:
            log_quality_metric(execution_id, "FORMAT_VALIDATION", f"{table_name}.{column_name}", "WARNING", f"Patrón no definido: {pattern_type}")
            return False
        
        pattern = patterns[pattern_type]
        valid_values = df[column_name].dropna().astype(str)
        if len(valid_values) == 0:
            log_quality_metric(execution_id, "FORMAT_VALIDATION", f"{table_name}.{column_name}", "WARNING", "No hay valores para validar")
            return True
        
        invalid_count = (~valid_values.str.match(pattern)).sum()
        total_count = len(valid_values)
        
        if invalid_count == 0:
            result = "PASS"
            severity = QualitySeverity.LOW.value
            detalles = f"Todos los {total_count} valores siguen el patrón {pattern_type}"
        else:
            invalid_percentage = (invalid_count / total_count) * 100
            if invalid_percentage < 5:
                result = "WARNING"
                severity = QualitySeverity.MEDIUM.value
            else:
                result = "FAIL"
                severity = QualitySeverity.HIGH.value
            detalles = f"Formato inválido: {invalid_count}/{total_count} ({invalid_percentage:.1f}%)"
        
        log_quality_metric(execution_id, "FORMAT_VALIDATION", f"{table_name}.{column_name}", result, detalles, severity)
        return invalid_count == 0
        
    except Exception as e:
        log_quality_metric(execution_id, "FORMAT_VALIDATION", f"{table_name}.{column_name}", "ERROR", f"Error: {str(e)}", QualitySeverity.HIGH.value)
        return False


def validate_dataframe_logic(execution_id: int, table_name: str, df: pd.DataFrame, validations: list):
    """Valida lógica de negocio en un DataFrame."""
    try:
        all_passed = True
        violations_summary = []
        
        for validation in validations:
            rule = validation['rule']
            description = validation.get('description', rule)
            
            # Para shipped_date >= order_date
            if 'shipped_date >= order_date' in rule:
                violations = ((df['shipped_date'].notna()) & (df['order_date'].notna()) & 
                            (pd.to_datetime(df['shipped_date'], errors='coerce') < pd.to_datetime(df['order_date'], errors='coerce'))).sum()
            else:
                violations = 0  # Para otras reglas futuras
            
            if violations > 0:
                all_passed = False
                violations_summary.append(f"{description}: {violations} violaciones")
        
        if all_passed:
            result = "PASS"
            severity = QualitySeverity.LOW.value
            detalles = f"Todas las {len(validations)} validaciones lógicas pasaron"
        else:
            result = "FAIL"
            severity = QualitySeverity.HIGH.value
            detalles = "; ".join(violations_summary)
        
        log_quality_metric(execution_id, "CROSS_FIELD_LOGIC", table_name, result, detalles, severity)
        return all_passed
        
    except Exception as e:
        log_quality_metric(execution_id, "CROSS_FIELD_LOGIC", table_name, "ERROR", f"Error: {str(e)}", QualitySeverity.HIGH.value)
        return False


def validate_dataframe_domain(execution_id: int, table_name: str, df: pd.DataFrame, column_name: str, valid_values: list, case_sensitive: bool = True):
    """Valida dominios en un DataFrame."""
    try:
        if column_name not in df.columns:
            log_quality_metric(execution_id, "DOMAIN_CONSTRAINTS", f"{table_name}.{column_name}", "WARNING", "Columna no encontrada")
            return False
        
        column_data = df[column_name].dropna().astype(str)
        if len(column_data) == 0:
            log_quality_metric(execution_id, "DOMAIN_CONSTRAINTS", f"{table_name}.{column_name}", "WARNING", "No hay valores para validar")
            return True
        
        if not case_sensitive:
            column_data = column_data.str.upper()
            valid_values_processed = [str(v).upper() for v in valid_values]
        else:
            valid_values_processed = [str(v) for v in valid_values]
        
        invalid_mask = ~column_data.isin(valid_values_processed)
        invalid_count = invalid_mask.sum()
        total_count = len(column_data)
        
        if invalid_count == 0:
            result = "PASS"
            severity = QualitySeverity.LOW.value
            detalles = f"Todos los {total_count} valores están en el dominio permitido"
        else:
            invalid_percentage = (invalid_count / total_count) * 100
            if invalid_percentage < 5:
                result = "WARNING"
                severity = QualitySeverity.MEDIUM.value
            else:
                result = "FAIL"
                severity = QualitySeverity.HIGH.value
            
            examples = column_data[invalid_mask].unique()[:3].tolist()
            examples_str = ", ".join([f"'{v}'" for v in examples])
            detalles = f"Valores fuera de dominio: {invalid_count}/{total_count} ({invalid_percentage:.1f}%). Ejemplos: {examples_str}"
        
        log_quality_metric(execution_id, "DOMAIN_CONSTRAINTS", f"{table_name}.{column_name}", result, detalles, severity)
        return invalid_count == 0
        
    except Exception as e:
        log_quality_metric(execution_id, "DOMAIN_CONSTRAINTS", f"{table_name}.{column_name}", "ERROR", f"Error: {str(e)}", QualitySeverity.HIGH.value)
        return False


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
        conn = connect_with_retry(DB_PATH)
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
                success = load_csv_to_table(conn, file_path, table_name, execution_id)
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
