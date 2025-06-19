import sqlite3
import logging

# --- Configuración de Logging ---
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)

# --- Constantes ---
DB_PATH = ".data/tp_dwa.db"

# --- Definiciones de las nuevas tablas con Foreign Keys ---
TABLE_DEFINITIONS = {
    "TMP_employees": """
        CREATE TABLE TMP_employees (
            employee_id INTEGER PRIMARY KEY,
            last_name TEXT NOT NULL, first_name TEXT NOT NULL, title TEXT,
            title_of_courtesy TEXT, birth_date TEXT, hire_date TEXT,
            address TEXT, city TEXT, region TEXT, postal_code TEXT,
            country TEXT, home_phone TEXT, extension TEXT, photo BLOB,
            notes TEXT, reports_to INTEGER, photo_path TEXT,
            FOREIGN KEY (reports_to) REFERENCES TMP_employees(employee_id)
        )
    """,
    "TMP_territories": """
        CREATE TABLE TMP_territories (
            territory_id TEXT PRIMARY KEY,
            territory_description TEXT NOT NULL,
            region_id INTEGER NOT NULL,
            FOREIGN KEY (region_id) REFERENCES TMP_regions(region_id)
        )
    """,
    "TMP_employee_territories": """
        CREATE TABLE TMP_employee_territories (
            employee_id INTEGER NOT NULL,
            territory_id TEXT NOT NULL,
            PRIMARY KEY (employee_id, territory_id),
            FOREIGN KEY (employee_id) REFERENCES TMP_employees(employee_id),
            FOREIGN KEY (territory_id) REFERENCES TMP_territories(territory_id)
        )
    """,
    "TMP_orders": """
        CREATE TABLE TMP_orders (
            order_id INTEGER PRIMARY KEY, customer_id TEXT, employee_id INTEGER,
            order_date TEXT, required_date TEXT, shipped_date TEXT,
            ship_via INTEGER, freight REAL, ship_name TEXT, ship_address TEXT,
            ship_city TEXT, ship_region TEXT, ship_postal_code TEXT, ship_country TEXT,
            FOREIGN KEY (customer_id) REFERENCES TMP_customers(customer_id),
            FOREIGN KEY (employee_id) REFERENCES TMP_employees(employee_id),
            FOREIGN KEY (ship_via) REFERENCES TMP_shippers(shipper_id)
        )
    """,
    "TMP_products": """
        CREATE TABLE TMP_products (
            product_id INTEGER PRIMARY KEY, product_name TEXT NOT NULL,
            supplier_id INTEGER, category_id INTEGER, quantity_per_unit TEXT,
            unit_price REAL, units_in_stock INTEGER, units_on_order INTEGER,
            reorder_level INTEGER, discontinued INTEGER,
            FOREIGN KEY (category_id) REFERENCES TMP_categories(category_id),
            FOREIGN KEY (supplier_id) REFERENCES TMP_suppliers(supplier_id)
        )
    """,
    "TMP_order_details": """
        CREATE TABLE TMP_order_details (
            order_id INTEGER NOT NULL, product_id INTEGER NOT NULL,
            unit_price REAL NOT NULL, quantity INTEGER NOT NULL, discount REAL NOT NULL,
            PRIMARY KEY (order_id, product_id),
            FOREIGN KEY (order_id) REFERENCES TMP_orders(order_id),
            FOREIGN KEY (product_id) REFERENCES TMP_products(product_id)
        )
    """,
}


def add_foreign_keys_and_verify():
    """
    Recrea las tablas para añadir las restricciones de Foreign Key y luego verifica
    la integridad referencial de la base de datos.
    """
    try:
        logging.info(f"Conectando a la base de datos en {DB_PATH}...")
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        logging.info("Conexión exitosa.")

        # Es crucial para que las FKs se tengan en cuenta
        cursor.execute("PRAGMA foreign_keys = ON;")

        # --- Recrear tablas con Foreign Keys ---
        # El orden es importante para evitar problemas con las dependencias al recrear.
        table_order = [
            "TMP_employees",
            "TMP_territories",
            "TMP_employee_territories",
            "TMP_orders",
            "TMP_products",
            "TMP_order_details",
        ]

        for table_name in table_order:
            logging.info(
                f"Recreando la tabla '{table_name}' para añadir Foreign Keys..."
            )

            # 1. Renombrar tabla original
            cursor.execute(f"ALTER TABLE {table_name} RENAME TO {table_name}_old;")

            # 2. Crear nueva tabla con la definición correcta
            new_table_sql = TABLE_DEFINITIONS[table_name]
            cursor.execute(new_table_sql)

            # 3. Copiar datos de la tabla antigua a la nueva
            cursor.execute(f"INSERT INTO {table_name} SELECT * FROM {table_name}_old;")

            # 4. Eliminar la tabla antigua
            cursor.execute(f"DROP TABLE {table_name}_old;")

            logging.info(f"Tabla '{table_name}' recreada con éxito.")

        conn.commit()
        logging.info(
            "Todas las tablas han sido recreadas con FKs. Cambios confirmados."
        )

        # --- Verificación de Integridad ---
        logging.info(
            "Ejecutando PRAGMA foreign_key_check para verificar la integridad referencial..."
        )
        fk_check = cursor.execute("PRAGMA foreign_key_check;").fetchall()

        if not fk_check:
            logging.info(
                "✅ ¡Verificación de integridad exitosa! No se encontraron errores de claves foráneas."
            )
        else:
            logging.warning(
                f"🚨 ¡Se encontraron {len(fk_check)} errores de integridad de claves foráneas!"
            )
            for error in fk_check:
                table, rowid, parent, fkid = error
                logging.warning(
                    f"  -> Tabla '{table}': Fila con rowid {rowid} viola la FK hacia la tabla '{parent}'."
                )

    except sqlite3.Error as e:
        logging.error(f"Error en la base de datos: {e}")
        # Si hay un error, intentamos revertir los cambios.
        if conn:
            conn.rollback()
            logging.info("Se han revertido los cambios por el error.")
    finally:
        if conn:
            conn.close()
            logging.info("Conexión a la base de datos cerrada.")


if __name__ == "__main__":
    add_foreign_keys_and_verify()
