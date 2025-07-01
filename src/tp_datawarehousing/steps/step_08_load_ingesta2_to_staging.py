import pandas as pd
import sqlite3
import logging
import re
import json
from pathlib import Path
from ..quality_utils import get_process_execution_id, log_quality_metric

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
        except Exception as e:
            logging.error(
                f"Error al cargar el archivo {file_path.name} a la tabla {table_name}: {e}"
            )


def main():
    """
    Orquesta la creación de las tablas de staging para Ingesta2 y la carga de datos.
    """
    logging.info("Iniciando el Paso 8: Carga de Ingesta2 al área de Staging (TMP2).")
    
    # Obtener ID de ejecución para métricas de calidad
    execution_id = get_process_execution_id("step_08_load_ingesta2_to_staging")
    
    conn = None
    try:
        conn = sqlite3.connect(DB_PATH)
        logging.info(f"Conexión exitosa a la base de datos {DB_PATH}.")
        create_and_load_staging_tables(conn, execution_id)
    except sqlite3.Error as e:
        logging.error(f"Error de base de datos en el Paso 8: {e}")
    finally:
        if conn:
            conn.close()
            logging.info("Conexión a la base de datos cerrada.")


if __name__ == "__main__":
    main()
