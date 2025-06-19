import sqlite3
import logging

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)

DB_PATH = ".data/tp_dwa.db"

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
    """
    conn = None
    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        cursor.execute("PRAGMA foreign_keys = ON;")

        logging.info("Creando tablas de la capa de Ingesta (ING_)...")
        for table in INSERTION_ORDER:
            cursor.execute(TABLE_CREATION_QUERIES[table])
        conn.commit()
        logging.info("Tablas ING_ creadas con éxito.")

        logging.info("Vaciando tablas ING_ antes de la carga...")
        for table in reversed(
            INSERTION_ORDER
        ):  # Vaciar en orden inverso para no violar FKs
            cursor.execute(f"DELETE FROM {table};")
        conn.commit()

        logging.info("Cargando datos de TMP_ a ING_...")
        for table in INSERTION_ORDER:
            source_table = table.replace("ING_", "TMP_")
            logging.info(f"Cargando {source_table} -> {table}")

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
            else:
                # Caso general para el resto de las tablas
                cursor.execute(f"INSERT INTO {table} SELECT * FROM {source_table};")

            conn.commit()
            logging.info(f"Carga de {table} completada.")

        logging.info("Capa de Ingesta (ING_) creada y cargada exitosamente.")

    except sqlite3.Error as e:
        logging.error(
            f"Error en la base de datos durante la creación de la capa de ingesta: {e}"
        )
        if conn:
            conn.rollback()
    finally:
        if conn:
            conn.close()
            logging.info("Conexión a la base de datos cerrada.")


def main():
    create_and_load_ingestion_layer()


if __name__ == "__main__":
    main()
