import sqlite3
import logging
from datetime import datetime

# --- Configuración de Logging ---
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)

# --- Constantes ---
DB_PATH = ".data/tp_dwa.db"
USER = "data_engineer"  # Para la metadata


def create_dwh_tables():
    """
    Crea las tablas del modelo dimensional (esquema en estrella) en el Data Warehouse.
    Las tablas se prefijan con DWA_.
    También registra las nuevas tablas en la tabla de metadatos.
    """
    try:
        logging.info(f"Conectando a la base de datos en {DB_PATH}...")
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        logging.info("Conexión exitosa.")

        # --- Crear Tablas de Dimensiones (DWA_DIM_*) ---
        logging.info("Creando tablas de Dimensiones (DWA_)...")

        # --- Dimensión Tiempo (Generada) ---
        cursor.execute(
            """
        CREATE TABLE IF NOT EXISTS DWA_DIM_Tiempo (
            sk_tiempo INTEGER PRIMARY KEY,
            fecha DATE NOT NULL,
            anio INTEGER NOT NULL,
            mes INTEGER NOT NULL,
            dia INTEGER NOT NULL,
            trimestre INTEGER NOT NULL,
            nombre_mes TEXT NOT NULL,
            nombre_dia TEXT NOT NULL,
            es_fin_de_semana INTEGER NOT NULL -- 1 para sí, 0 para no
        )"""
        )

        # --- Dimensión Clientes (con SCD Tipo 2) ---
        # sk: Surrogate Key
        # nk: Natural Key
        cursor.execute(
            """
        CREATE TABLE IF NOT EXISTS DWA_DIM_Clientes (
            sk_cliente INTEGER PRIMARY KEY AUTOINCREMENT,
            nk_cliente_id TEXT NOT NULL,
            nombre_compania TEXT NOT NULL,
            nombre_contacto TEXT,
            titulo_contacto TEXT,
            direccion TEXT,
            ciudad TEXT,
            region TEXT,
            codigo_postal TEXT,
            pais TEXT,
            -- Campos para SCD Tipo 2 (Capa de Memoria)
            fecha_inicio_validez DATE NOT NULL,
            fecha_fin_validez DATE,
            es_vigente INTEGER NOT NULL -- 1 para vigente, 0 para histórico
        )"""
        )

        # --- Dimensión Productos ---
        cursor.execute(
            """
        CREATE TABLE IF NOT EXISTS DWA_DIM_Productos (
            sk_producto INTEGER PRIMARY KEY AUTOINCREMENT,
            nk_producto_id INTEGER NOT NULL,
            nombre_producto TEXT NOT NULL,
            cantidad_por_unidad TEXT,
            precio_unitario REAL,
            -- Datos desnormalizados de otras tablas
            nombre_categoria TEXT,
            descripcion_categoria TEXT,
            nombre_proveedor TEXT,
            pais_proveedor TEXT,
            -- Bandera de estado
            descontinuado INTEGER NOT NULL -- 1 para sí, 0 para no
        )"""
        )

        # --- Dimensión Empleados (con Enriquecimiento) ---
        cursor.execute(
            """
        CREATE TABLE IF NOT EXISTS DWA_DIM_Empleados (
            sk_empleado INTEGER PRIMARY KEY AUTOINCREMENT,
            nk_empleado_id INTEGER NOT NULL,
            nombre_completo TEXT NOT NULL,
            titulo TEXT,
            fecha_nacimiento DATE,
            fecha_contratacion DATE,
            -- Campo de Enriquecimiento
            edad_en_contratacion INTEGER,
            ciudad TEXT,
            region TEXT,
            pais TEXT,
            nombre_jefe TEXT -- Auto-referencia desnormalizada
        )"""
        )

        # --- Dimensión Geografía (Consolidada) ---
        cursor.execute(
            """
        CREATE TABLE IF NOT EXISTS DWA_DIM_Geografia (
            sk_geografia INTEGER PRIMARY KEY AUTOINCREMENT,
            direccion TEXT,
            ciudad TEXT,
            region TEXT,
            codigo_postal TEXT,
            pais TEXT,
            -- Datos enriquecidos desde world_data
            densidad_poblacion REAL,
            pib REAL,
            esperanza_de_vida REAL
        )"""
        )

        # --- Dimensión Shippers ---
        cursor.execute(
            """
        CREATE TABLE IF NOT EXISTS DWA_DIM_Shippers (
            sk_shipper INTEGER PRIMARY KEY AUTOINCREMENT,
            nk_shipper_id INTEGER NOT NULL,
            nombre_compania TEXT NOT NULL,
            telefono TEXT
        )"""
        )

        logging.info("Tablas de Dimensiones creadas con éxito.")

        # --- Crear Tabla de Hechos (DWA_FACT_*) ---
        logging.info("Creando tabla de Hechos (DWA_)...")
        cursor.execute(
            """
        CREATE TABLE IF NOT EXISTS DWA_FACT_Ventas (
            sk_venta INTEGER PRIMARY KEY AUTOINCREMENT,
            -- Claves foráneas a las dimensiones
            sk_cliente INTEGER NOT NULL,
            sk_producto INTEGER NOT NULL,
            sk_empleado INTEGER NOT NULL,
            sk_tiempo INTEGER NOT NULL,
            sk_geografia_envio INTEGER NOT NULL,
            sk_shipper INTEGER NOT NULL,
            -- Métricas del negocio
            precio_unitario REAL NOT NULL,
            cantidad INTEGER NOT NULL,
            descuento REAL NOT NULL,
            flete REAL,
            -- Métrica Derivada/Enriquecida
            monto_total REAL NOT NULL,
            -- Claves Naturales para referencia
            nk_orden_id INTEGER NOT NULL,
            FOREIGN KEY (sk_cliente) REFERENCES DWA_DIM_Clientes(sk_cliente),
            FOREIGN KEY (sk_producto) REFERENCES DWA_DIM_Productos(sk_producto),
            FOREIGN KEY (sk_empleado) REFERENCES DWA_DIM_Empleados(sk_empleado),
            FOREIGN KEY (sk_tiempo) REFERENCES DWA_DIM_Tiempo(sk_tiempo),
            FOREIGN KEY (sk_geografia_envio) REFERENCES DWA_DIM_Geografia(sk_geografia),
            FOREIGN KEY (sk_shipper) REFERENCES DWA_DIM_Shippers(sk_shipper)
        )"""
        )
        logging.info("Tabla de Hechos creada con éxito.")

        # --- Registrar en Metadata ---
        logging.info("Registrando nuevas tablas en Metadata (MET_)...")
        dwh_tables = {
            "DWA_DIM_Tiempo": "Dimensión de Tiempo generada.",
            "DWA_DIM_Clientes": "Dimensión de Clientes con historia (SCD Tipo 2).",
            "DWA_DIM_Productos": "Dimensión de Productos desnormalizada con categorías y proveedores.",
            "DWA_DIM_Empleados": "Dimensión de Empleados con datos enriquecidos (edad).",
            "DWA_DIM_Geografia": "Dimensión consolidada de Geografía enriquecida con datos mundiales.",
            "DWA_DIM_Shippers": "Dimensión de Transportistas (Shippers).",
            "DWA_FACT_Ventas": "Tabla de Hechos central que registra las transacciones de ventas.",
        }

        current_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        for table_name, description in dwh_tables.items():
            cursor.execute(
                """
            INSERT OR REPLACE INTO MET_entidades (nombre_entidad, descripcion, capa, fecha_creacion, usuario_creacion)
            VALUES (?, ?, ?, ?, ?)
            """,
                (table_name, description, "DWH", current_date, USER),
            )

        logging.info("Metadata actualizada para las tablas del DWH.")

        conn.commit()
        logging.info("Cambios confirmados en la base de datos.")

    except sqlite3.Error as e:
        logging.error(f"Error en la base de datos: {e}")
        if conn:
            conn.rollback()
    finally:
        if conn:
            conn.close()
            logging.info("Conexión a la base de datos cerrada.")


def main():
    """Orquesta la creación de las tablas del DWH."""
    create_dwh_tables()


if __name__ == "__main__":
    main()
