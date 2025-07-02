import pandas as pd
import sqlite3
import logging
import re
import json
from pathlib import Path
from ..quality_utils import (
    get_process_execution_id, 
    update_process_execution,
    log_quality_metric,
    validate_table_count,
    validate_completeness_score,
    validate_business_key_uniqueness,
    QualityResult,
    QualitySeverity
)

# --- Configuración de Logging ---
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)

# --- Constantes ---
DB_PATH = "db/tp_dwa.db"
INGESTA_PATH = ".data/ingesta2"

# Mapeo de archivos CSV de Ingesta2 a nombres de tablas temporales TMP2_
TABLE_MAPPING = {
    "customers - novedades.csv": "TMP2_customers",
    "orders - novedades.csv": "TMP2_orders",
    "order_details - novedades.csv": "TMP2_order_details",
}

# Definición del esquema para las nuevas tablas TMP2.
# Deben ser compatibles con las tablas de la capa de Ingesta (ING_).
TABLE_SCHEMAS = {
    "TMP2_customers": """
        CREATE TABLE IF NOT EXISTS TMP2_customers (
            customer_id TEXT PRIMARY KEY,
            company_name TEXT,
            contact_name TEXT,
            contact_title TEXT,
            address TEXT,
            city TEXT,
            region TEXT,
            postal_code TEXT,
            country TEXT,
            phone TEXT,
            fax TEXT
        );
    """,
    "TMP2_orders": """
        CREATE TABLE IF NOT EXISTS TMP2_orders (
            order_id INTEGER PRIMARY KEY,
            customer_id TEXT,
            employee_id INTEGER,
            order_date TEXT,
            required_date TEXT,
            shipped_date TEXT,
            ship_via INTEGER,
            freight REAL,
            ship_name TEXT,
            ship_address TEXT,
            ship_city TEXT,
            ship_region TEXT,
            ship_postal_code TEXT,
            ship_country TEXT
        );
    """,
    "TMP2_order_details": """
        CREATE TABLE IF NOT EXISTS TMP2_order_details (
            order_id INTEGER,
            product_id INTEGER,
            unit_price REAL,
            quantity INTEGER,
            discount REAL
        );
    """,
}


def normalize_column_name(col_name: str) -> str:
    """
    Convierte un nombre de columna a formato snake_case y limpia caracteres especiales.
    E.g., 'Contact Name' -> 'contact_name', 'OrderID' -> 'order_id'
    """
    name = re.sub(r"[-\s\./:]+", "_", col_name)
    name = re.sub(r"([a-z\d])([A-Z])", r"\1_\2", name)
    name = name.lower()
    name = re.sub(r"__+", "_", name)
    return name


def create_and_load_staging_tables(conn: sqlite3.Connection, execution_id: int):
    """
    Crea las tablas TMP2_ y carga los datos de los CSV de Ingesta2.
    """
    cursor = conn.cursor()
    logging.info("Creando tablas de Staging para Ingesta2 (TMP2_)...")
    for table, schema in TABLE_SCHEMAS.items():
        cursor.execute(f"DROP TABLE IF EXISTS {table};")  # Asegurar carga limpia
        cursor.execute(schema)
    conn.commit()
    logging.info("Tablas TMP2_ creadas/limpiadas con éxito.")

    ingesta_dir = Path(INGESTA_PATH)
    for csv_file, table_name in TABLE_MAPPING.items():
        file_path = ingesta_dir / csv_file
        if not file_path.exists():
            logging.warning(
                f"El archivo {csv_file} no se encontró en {INGESTA_PATH}. Se omite."
            )
            continue

        try:
            logging.info(f"Procesando archivo: {file_path.name} -> Tabla: {table_name}")
            df = pd.read_csv(file_path)

            original_columns = df.columns
            df.columns = [normalize_column_name(col) for col in original_columns]
            renamed_cols_map = {
                orig: new
                for orig, new in zip(original_columns, df.columns)
                if orig != new
            }
            if renamed_cols_map:
                logging.info(f"Columnas renombradas: {renamed_cols_map}")
                # Registrar conteo de columnas renombradas
                log_quality_metric(execution_id, "COLUMN_NORMALIZATION", file_path.name, "PERFORMED", 
                                 f"Columnas renombradas: {len(renamed_cols_map)}")
                # Registrar mapeo detallado para trazabilidad completa
                log_quality_metric(execution_id, "COLUMN_NORMALIZATION_DETAIL", file_path.name, "MAPPING", 
                                 json.dumps(renamed_cols_map, ensure_ascii=False))

            df.to_sql(table_name, conn, if_exists="append", index=False)
            logging.info(
                f"Carga exitosa de {len(df)} registros en la tabla {table_name}."
            )
            
            # AGREGAR VALIDACIONES DE CALIDAD ESPECÍFICAS
            perform_data_quality_validations(execution_id, table_name, file_path.name, conn)
            
        except Exception as e:
            logging.error(
                f"Error al cargar el archivo {file_path.name} a la tabla {table_name}: {e}"
            )


def perform_data_quality_validations(execution_id: int, table_name: str, file_name: str, conn: sqlite3.Connection):
    """
    Realiza validaciones específicas de calidad de datos para cada tabla de Ingesta2.
    Detecta los problemas específicos que mencionaste.
    """
    cursor = conn.cursor()
    
    # Validación 1: Conteo básico
    validate_table_count(execution_id, table_name, 1, conn)
    
    # Validaciones específicas por tabla
    if table_name == "TMP2_customers":
        # Validar completitud de campos críticos para customers
        validate_completeness_score(
            execution_id, 
            table_name, 
            ["customer_id", "company_name", "country", "region"],  # region es crítico
            conn
        )
        
        # Validar unicidad de customer_id
        validate_business_key_uniqueness(
            execution_id,
            table_name,
            ["customer_id"],
            conn
        )
        
        # Validación específica para region (problema detectado)
        validate_null_percentage(execution_id, table_name, "region", file_name, conn)
        
    elif table_name == "TMP2_orders":
        # Validar completitud de campos críticos para orders
        validate_completeness_score(
            execution_id,
            table_name,
            ["order_id", "customer_id", "order_date", "shipped_date", "ship_region", "ship_postal_code"],
            conn
        )
        
        # Validar unicidad de order_id
        validate_business_key_uniqueness(
            execution_id,
            table_name,
            ["order_id"],
            conn
        )
        
        # Validaciones específicas para problemas detectados
        validate_null_percentage(execution_id, table_name, "shipped_date", file_name, conn)
        validate_null_percentage(execution_id, table_name, "ship_region", file_name, conn)
        validate_null_percentage(execution_id, table_name, "ship_postal_code", file_name, conn)
        
        # Validar lógica de negocio: shipped_date >= order_date
        validate_shipping_logic(execution_id, table_name, conn)
        
    elif table_name == "TMP2_order_details":
        # Validar completitud de campos críticos
        validate_completeness_score(
            execution_id,
            table_name,
            ["order_id", "product_id", "unit_price", "quantity"],
            conn
        )
        
        # Validar unicidad de la clave compuesta
        validate_business_key_uniqueness(
            execution_id,
            table_name,
            ["order_id", "product_id"],
            conn
        )
        
        # Validar rangos de valores para campos numéricos
        validate_numeric_ranges(execution_id, table_name, conn)


def validate_null_percentage(execution_id: int, table_name: str, column_name: str, file_name: str, conn: sqlite3.Connection):
    """
    Calcula y reporta el porcentaje de valores nulos en una columna específica.
    """
    cursor = conn.cursor()
    
    try:
        # Contar total de registros
        cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
        total_records = cursor.fetchone()[0]
        
        # Contar valores nulos
        cursor.execute(f"SELECT COUNT(*) FROM {table_name} WHERE {column_name} IS NULL OR TRIM({column_name}) = ''")
        null_count = cursor.fetchone()[0]
        
        if total_records == 0:
            percentage = 0
        else:
            percentage = (null_count / total_records) * 100
        
        # Determinar resultado basado en umbrales
        if percentage == 0:
            result = QualityResult.PASS.value
            severity = QualitySeverity.LOW.value
        elif percentage < 10:
            result = QualityResult.WARNING.value
            severity = QualitySeverity.MEDIUM.value
        else:
            result = QualityResult.FAIL.value
            severity = QualitySeverity.HIGH.value
        
        detalles = f"Valores nulos/vacíos: {null_count}/{total_records} ({percentage:.1f}%)"
        
        log_quality_metric(
            execution_id,
            "NULL_PERCENTAGE",
            f"{table_name}.{column_name}",
            result,
            detalles,
            severity
        )
        
        # Log específico del problema detectado
        if column_name == "region" and table_name == "TMP2_customers" and percentage > 0:
            log_quality_metric(
                execution_id,
                "BUSINESS_ISSUE_DETECTED",
                f"{file_name}.{column_name}",
                "CRITICAL",
                f"🔴 PROBLEMA DETECTADO: {null_count} clientes sin región en archivo con solo {total_records} registros",
                QualitySeverity.CRITICAL.value
            )
        elif column_name in ["shipped_date", "ship_region", "ship_postal_code"] and table_name == "TMP2_orders":
            expected_reason = {
                "shipped_date": "órdenes aún no despachadas",
                "ship_region": "regiones no aplicables", 
                "ship_postal_code": "códigos postales omitidos"
            }
            log_quality_metric(
                execution_id,
                "BUSINESS_ISSUE_DETECTED", 
                f"{file_name}.{column_name}",
                "WARNING" if percentage < 50 else "CRITICAL",
                f"🟡 REVISAR: {null_count} valores faltantes en {column_name} - posiblemente {expected_reason[column_name]}",
                QualitySeverity.MEDIUM.value if percentage < 50 else QualitySeverity.HIGH.value
            )
    
    except sqlite3.Error as e:
        log_quality_metric(
            execution_id,
            "NULL_PERCENTAGE_ERROR",
            f"{table_name}.{column_name}",
            QualityResult.ERROR.value,
            f"Error calculando porcentaje de nulos: {str(e)}",
            QualitySeverity.HIGH.value
        )


def validate_shipping_logic(execution_id: int, table_name: str, conn: sqlite3.Connection):
    """
    Valida que las fechas de envío sean posteriores o iguales a las fechas de pedido.
    """
    cursor = conn.cursor()
    
    try:
        cursor.execute(f"""
            SELECT COUNT(*) FROM {table_name} 
            WHERE shipped_date IS NOT NULL 
            AND order_date IS NOT NULL 
            AND shipped_date < order_date
        """)
        
        violations = cursor.fetchone()[0]
        
        if violations == 0:
            result = QualityResult.PASS.value
            severity = QualitySeverity.LOW.value
            detalles = "Todas las fechas de envío son posteriores a las fechas de pedido"
        else:
            result = QualityResult.FAIL.value
            severity = QualitySeverity.HIGH.value
            detalles = f"🔴 LÓGICA DE NEGOCIO VIOLADA: {violations} órdenes con fecha_envío < fecha_pedido"
        
        log_quality_metric(
            execution_id,
            "SHIPPING_DATE_LOGIC",
            table_name,
            result,
            detalles,
            severity
        )
        
    except sqlite3.Error as e:
        log_quality_metric(
            execution_id,
            "SHIPPING_DATE_LOGIC_ERROR",
            table_name,
            QualityResult.ERROR.value,
            f"Error validando lógica de fechas: {str(e)}",
            QualitySeverity.HIGH.value
        )


def validate_numeric_ranges(execution_id: int, table_name: str, conn: sqlite3.Connection):
    """
    Valida que los valores numéricos estén en rangos esperados.
    """
    cursor = conn.cursor()
    
    numeric_validations = [
        ("unit_price", 0, None, "Precio unitario debe ser positivo"),
        ("quantity", 1, 1000, "Cantidad debe estar entre 1 y 1000"),
        ("discount", 0, 1, "Descuento debe estar entre 0 y 1")
    ]
    
    for column, min_val, max_val, description in numeric_validations:
        try:
            conditions = []
            if min_val is not None:
                conditions.append(f"{column} < {min_val}")
            if max_val is not None:
                conditions.append(f"{column} > {max_val}")
            
            if conditions:
                where_clause = " OR ".join(conditions)
                cursor.execute(f"SELECT COUNT(*) FROM {table_name} WHERE {where_clause}")
                violations = cursor.fetchone()[0]
                
                if violations == 0:
                    result = QualityResult.PASS.value
                    severity = QualitySeverity.LOW.value
                    detalles = f"{description} - Sin violaciones"
                else:
                    result = QualityResult.FAIL.value
                    severity = QualitySeverity.MEDIUM.value
                    detalles = f"🟡 {description} - {violations} violaciones encontradas"
                
                log_quality_metric(
                    execution_id,
                    "NUMERIC_RANGE_VALIDATION",
                    f"{table_name}.{column}",
                    result,
                    detalles,
                    severity
                )
        
        except sqlite3.Error as e:
            log_quality_metric(
                execution_id,
                "NUMERIC_RANGE_ERROR",
                f"{table_name}.{column}",
                QualityResult.ERROR.value,
                f"Error validando rango numérico: {str(e)}",
                QualitySeverity.HIGH.value
            )


def main():
    """
    Orquesta la creación de las tablas de staging para Ingesta2 y la carga de datos.
    """
    logging.info("Iniciando el Paso 8: Carga de Ingesta2 al área de Staging (TMP2).")
    
    # Obtener ID de ejecución para métricas de calidad
    execution_id = get_process_execution_id("step_08_load_ingesta2_to_staging")
    
    conn = None
    success = False
    try:
        conn = sqlite3.connect(DB_PATH)
        logging.info(f"Conexión exitosa a la base de datos {DB_PATH}.")
        create_and_load_staging_tables(conn, execution_id)
        success = True
        logging.info("Paso 8 completado exitosamente.")
        
        # Marcar proceso como exitoso
        update_process_execution(execution_id, "Exitoso", "Carga de Ingesta2 completada exitosamente")
        
    except sqlite3.Error as e:
        logging.error(f"Error de base de datos en el Paso 8: {e}")
        update_process_execution(execution_id, "Fallido", f"Error de base de datos: {str(e)}")
    except Exception as e:
        logging.error(f"Error inesperado en el Paso 8: {e}")
        update_process_execution(execution_id, "Fallido", f"Error inesperado: {str(e)}")
    finally:
        if conn:
            conn.close()
            logging.info("Conexión a la base de datos cerrada.")


if __name__ == "__main__":
    main()
