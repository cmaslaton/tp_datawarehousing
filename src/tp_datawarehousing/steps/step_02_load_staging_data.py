import pandas as pd
import sqlite3
import logging
import os
import re
from pathlib import Path

# --- Configuración de Logging ---
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)

# --- Constantes ---
DB_PATH = ".data/tp_dwa.db"
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


def load_csv_to_table(file_path: Path, table_name: str, conn: sqlite3.Connection):
    """
    Carga los datos de un archivo CSV a una tabla específica en la base de datos,
    normalizando los nombres de las columnas antes de la inserción.
    """
    try:
        logging.info(f"Procesando archivo: {file_path.name} -> Tabla: {table_name}")
        # Intenta leer con UTF-8, si falla, prueba con latin-1
        try:
            df = pd.read_csv(file_path)
        except UnicodeDecodeError:
            logging.warning(
                f"Error de decodificación UTF-8 en {file_path.name}. Intentando con 'latin-1'."
            )
            df = pd.read_csv(file_path, encoding="latin-1")

        # Normalizar los nombres de las columnas del DataFrame
        original_columns = df.columns
        df.columns = [normalize_column_name(col) for col in original_columns]
        renamed_cols_map = {
            orig: new for orig, new in zip(original_columns, df.columns) if orig != new
        }
        if renamed_cols_map:
            logging.info(f"Columnas renombradas: {renamed_cols_map}")

        # Cargar datos en la tabla. 'append' añade los datos, 'replace' borraría la tabla primero.
        df.to_sql(table_name, conn, if_exists="append", index=False)
        logging.info(f"Carga exitosa de {len(df)} registros en la tabla {table_name}.")
        return True
    except Exception as e:
        logging.error(
            f"Error al cargar el archivo {file_path.name} a la tabla {table_name}: {e}"
        )
        return False


def load_all_staging_data():
    """
    Orquesta la carga de todos los archivos CSV de Ingesta1 a sus tablas de staging.
    """
    logging.info("Iniciando la carga de datos de Ingesta1 al área de Staging.")

    if not Path(DB_PATH).exists():
        logging.error(
            f"La base de datos {DB_PATH} no existe. Ejecute el paso 01 primero."
        )
        return

    try:
        conn = sqlite3.connect(DB_PATH)
        logging.info(f"Conexión exitosa a la base de datos {DB_PATH}.")

        ingesta_dir = Path(INGESTA_PATH)
        if not ingesta_dir.exists():
            logging.error(f"El directorio de ingesta {INGESTA_PATH} no se encuentra.")
            return

        for csv_file, table_name in TABLE_MAPPING.items():
            file_path = ingesta_dir / csv_file
            if file_path.exists():
                load_csv_to_table(file_path, table_name, conn)
            else:
                logging.warning(
                    f"El archivo {csv_file} no se encontró en {INGESTA_PATH}. Se omite."
                )

        logging.info("Proceso de carga de datos de Staging finalizado.")

    except sqlite3.Error as e:
        logging.error(f"Error de base de datos durante la carga de staging: {e}")
    finally:
        if conn:
            conn.close()
            logging.info("Conexión a la base de datos cerrada.")


if __name__ == "__main__":
    load_all_staging_data()
