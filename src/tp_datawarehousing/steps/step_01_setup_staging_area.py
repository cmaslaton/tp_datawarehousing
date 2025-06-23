import sqlite3
import logging

# --- Configuración de Logging ---
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)

# --- Constantes ---
DB_PATH = "db/tp_dwa.db"


def create_database_and_tables():
    """
    Crea la base de datos y todas las tablas para el área de staging y metadatos.
    Las tablas de staging se prefijan con TMP_ y la de metadatos con MET_.
    """
    try:
        logging.info(f"Conectando a la base de datos en {DB_PATH}...")
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        logging.info("Conexión exitosa.")

        # --- Crear Tablas de Staging (TMP_) ---
        logging.info("Creando tablas del área de Staging (TMP_)...")

        # Basado en el DER de Northwind
        cursor.execute(
            """
        CREATE TABLE IF NOT EXISTS TMP_categories (
            category_id INTEGER PRIMARY KEY,
            category_name TEXT NOT NULL,
            description TEXT,
            picture BLOB
        )"""
        )

        cursor.execute(
            """
        CREATE TABLE IF NOT EXISTS TMP_products (
            product_id INTEGER PRIMARY KEY,
            product_name TEXT NOT NULL,
            supplier_id INTEGER,
            category_id INTEGER,
            quantity_per_unit TEXT,
            unit_price REAL,
            units_in_stock INTEGER,
            units_on_order INTEGER,
            reorder_level INTEGER,
            discontinued INTEGER
        )"""
        )

        cursor.execute(
            """
        CREATE TABLE IF NOT EXISTS TMP_suppliers (
            supplier_id INTEGER PRIMARY KEY,
            company_name TEXT NOT NULL,
            contact_name TEXT,
            contact_title TEXT,
            address TEXT,
            city TEXT,
            region TEXT,
            postal_code TEXT,
            country TEXT,
            phone TEXT,
            fax TEXT,
            home_page TEXT
        )"""
        )

        cursor.execute(
            """
        CREATE TABLE IF NOT EXISTS TMP_order_details (
            order_id INTEGER NOT NULL,
            product_id INTEGER NOT NULL,
            unit_price REAL NOT NULL,
            quantity INTEGER NOT NULL,
            discount REAL NOT NULL,
            PRIMARY KEY (order_id, product_id)
        )"""
        )

        cursor.execute(
            """
        CREATE TABLE IF NOT EXISTS TMP_orders (
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
        )"""
        )

        cursor.execute(
            """
        CREATE TABLE IF NOT EXISTS TMP_customers (
            customer_id TEXT PRIMARY KEY,
            company_name TEXT NOT NULL,
            contact_name TEXT,
            contact_title TEXT,
            address TEXT,
            city TEXT,
            region TEXT,
            postal_code TEXT,
            country TEXT,
            phone TEXT,
            fax TEXT
        )"""
        )

        cursor.execute(
            """
        CREATE TABLE IF NOT EXISTS TMP_employees (
            employee_id INTEGER PRIMARY KEY,
            last_name TEXT NOT NULL,
            first_name TEXT NOT NULL,
            title TEXT,
            title_of_courtesy TEXT,
            birth_date TEXT,
            hire_date TEXT,
            address TEXT,
            city TEXT,
            region TEXT,
            postal_code TEXT,
            country TEXT,
            home_phone TEXT,
            extension TEXT,
            photo BLOB,
            notes TEXT,
            reports_to INTEGER,
            photo_path TEXT
        )"""
        )

        cursor.execute(
            """
        CREATE TABLE IF NOT EXISTS TMP_shippers (
            shipper_id INTEGER PRIMARY KEY,
            company_name TEXT NOT NULL,
            phone TEXT
        )"""
        )

        cursor.execute(
            """
        CREATE TABLE IF NOT EXISTS TMP_territories (
            territory_id TEXT PRIMARY KEY,
            territory_description TEXT NOT NULL,
            region_id INTEGER NOT NULL
        )"""
        )

        cursor.execute(
            """
        CREATE TABLE IF NOT EXISTS TMP_employee_territories (
            employee_id INTEGER NOT NULL,
            territory_id TEXT NOT NULL,
            PRIMARY KEY (employee_id, territory_id)
        )"""
        )

        cursor.execute(
            """
        CREATE TABLE IF NOT EXISTS TMP_regions (
            region_id INTEGER PRIMARY KEY,
            region_description TEXT NOT NULL
        )"""
        )

        # Tabla para datos de países
        cursor.execute(
            """
        CREATE TABLE IF NOT EXISTS TMP_world_data_2023 (
            country TEXT,
            density REAL,
            abbreviation TEXT,
            agricultural_land REAL,
            land_area REAL,
            armed_forces_size INTEGER,
            birth_rate REAL,
            calling_code INTEGER,
            capital_major_city TEXT,
            co2_emissions REAL,
            cpi REAL,
            cpi_change REAL,
            currency_code TEXT,
            fertility_rate REAL,
            forested_area REAL,
            gasoline_price REAL,
            gdp REAL,
            gross_primary_education_enrollment REAL,
            gross_tertiary_education_enrollment REAL,
            infant_mortality REAL,
            largest_city TEXT,
            life_expectancy REAL,
            maternal_mortality_ratio REAL,
            minimum_wage REAL,
            official_language TEXT,
            out_of_pocket_health_expenditure REAL,
            physicians_per_thousand REAL,
            population INTEGER,
            population_labor_force_participation REAL,
            tax_revenue REAL,
            total_tax_rate REAL,
            unemployment_rate REAL,
            urban_population INTEGER,
            latitude REAL,
            longitude REAL
        )"""
        )

        logging.info("Tablas de Staging creadas con éxito.")

        # --- Crear Tabla de Metadatos (MET_) ---
        logging.info("Creando tabla de Metadatos (MET_)...")
        cursor.execute(
            """
        CREATE TABLE IF NOT EXISTS MET_entidades (
            nombre_entidad TEXT PRIMARY KEY,
            descripcion TEXT,
            capa TEXT, -- E.g., 'Staging', 'DWH', 'DataMart'
            fecha_creacion TEXT,
            usuario_creacion TEXT
        )"""
        )
        logging.info("Tabla de Metadatos creada con éxito.")

        conn.commit()
        logging.info("Cambios confirmados en la base de datos.")

    except sqlite3.Error as e:
        logging.error(f"Error en la base de datos: {e}")
    finally:
        if conn:
            conn.close()
            logging.info("Conexión a la base de datos cerrada.")


if __name__ == "__main__":
    create_database_and_tables()
