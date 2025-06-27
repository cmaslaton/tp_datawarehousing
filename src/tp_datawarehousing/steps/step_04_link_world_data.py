import sqlite3
import logging
import pandas as pd

# --- Configuración de Logging ---
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)

# --- Constantes ---
DB_PATH = "db/tp_dwa.db"

# --- Mapeo de inconsistencias conocidas ---
# Este mapeo se usará para estandarizar los nombres de los países.
COUNTRY_NAME_MAPPING = {
    "UK": "United Kingdom",
    "USA": "United States",
    # Mapeos directos para asegurar consistencia con la tabla world_data.
    # Se incluyen todos los países de Northwind para asegurar un JOIN exitoso.
    "Argentina": "Argentina",
    "Australia": "Australia",
    "Austria": "Austria",
    "Belgium": "Belgium",
    "Brazil": "Brazil",
    "Canada": "Canada",
    "Denmark": "Denmark",
    "Finland": "Finland",
    "France": "France",
    "Germany": "Germany",
    "Ireland": "Republic of Ireland",
    "Italy": "Italy",
    "Japan": "Japan",
    "Mexico": "Mexico",
    "Netherlands": "Netherlands",
    "Norway": "Norway",
    "Poland": "Poland",
    "Portugal": "Portugal",
    "Singapore": "Singapore",
    "Spain": "Spain",
    "Sweden": "Sweden",
    "Switzerland": "Switzerland",
    "Venezuela": "Venezuela",
}


def standardize_country_names():
    """
    Estandariza los nombres de los países en las tablas de Northwind utilizando
    el mapeo definido en COUNTRY_NAME_MAPPING.
    """
    logging.info("--- Iniciando estandarización de nombres de países ---")
    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        logging.info(f"Conexión exitosa a la base de datos {DB_PATH}.")

        northwind_tables = {
            "ING_customers": "country",
            "ING_employees": "country",
            "ING_suppliers": "country",
            "ING_orders": "ship_country",
        }

        for old_name, new_name in COUNTRY_NAME_MAPPING.items():
            for table, column in northwind_tables.items():
                query = f"UPDATE {table} SET {column} = ? WHERE {column} = ?"
                cursor.execute(query, (new_name, old_name))

        conn.commit()
        logging.info("Nombres de países estandarizados con éxito.")

    except sqlite3.Error as e:
        logging.error(f"Error de base de datos durante la estandarización: {e}")
        conn.rollback()
    finally:
        if conn:
            conn.close()
            logging.info("Conexión a la base de datos cerrada.")


def analyze_country_data_consistency():
    """
    Analiza la consistencia de los nombres de países entre las tablas de Northwind
    y la tabla de datos mundiales. Imprime un reporte de los países que no coinciden.
    """
    logging.info("--- Iniciando análisis de consistencia de datos de países ---")
    try:
        conn = sqlite3.connect(DB_PATH)
        logging.info(f"Conexión exitosa a la base de datos {DB_PATH}.")

        # Obtener todos los nombres de países únicos de la tabla de datos mundiales
        world_countries_df = pd.read_sql_query(
            "SELECT DISTINCT country FROM ING_world_data_2023", conn
        )
        world_countries_set = set(world_countries_df["country"].str.strip())

        # Tablas y columnas de Northwind que contienen información de países
        northwind_tables = {
            "ING_customers": "country",
            "ING_employees": "country",
            "ING_suppliers": "country",
            "ING_orders": "ship_country",
        }

        logging.info("Comparando nombres de países en las tablas de Northwind...")
        all_mismatched_countries = set()

        for table, column in northwind_tables.items():
            query = f"SELECT DISTINCT {column} FROM {table} WHERE {column} IS NOT NULL"
            northwind_countries_df = pd.read_sql_query(query, conn)
            northwind_countries_set = set(northwind_countries_df[column].str.strip())

            # Encontrar los países que están en Northwind pero no en la tabla mundial
            mismatched = northwind_countries_set - world_countries_set
            if mismatched:
                logging.info(f"Países a estandarizar en '{table}': {mismatched}")
                all_mismatched_countries.update(mismatched)

        if not all_mismatched_countries:
            logging.info("¡Excelente! Todos los nombres de países coinciden.")
        else:
            logging.info(f"Total de países a estandarizar: {all_mismatched_countries}")
            logging.info(
                "Se procederá a la corrección con el mapeo 'COUNTRY_NAME_MAPPING'."
            )

    except sqlite3.Error as e:
        logging.error(f"Error de base de datos durante el análisis: {e}")
    finally:
        if conn:
            conn.close()
            logging.info("Conexión a la base de datos cerrada.")


def main():
    """
    Orquesta el proceso de análisis y estandarización de datos de países.
    """
    analyze_country_data_consistency()  # Primero, vemos qué está mal
    standardize_country_names()  # Luego, lo corregimos
    logging.info("--- Verificación post-corrección ---")
    analyze_country_data_consistency()  # Finalmente, verificamos que todo esté bien


if __name__ == "__main__":
    main()
