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


def create_dqm_tables():
    """
    Crea las tablas del Data Quality Mart (DQM) para monitorear procesos y calidad de datos.
    Las tablas se prefijan con DQM_.
    También registra las nuevas tablas en la tabla de metadatos.
    """
    try:
        logging.info(f"Conectando a la base de datos en {DB_PATH}...")
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        logging.info("Conexión exitosa.")

        # --- Crear Tablas del DQM ---
        logging.info("Creando tablas del Data Quality Mart (DQM_)...")

        # Tabla para registrar la ejecución de procesos
        cursor.execute(
            """
        CREATE TABLE IF NOT EXISTS DQM_ejecucion_procesos (
            id_ejecucion INTEGER PRIMARY KEY AUTOINCREMENT,
            nombre_proceso TEXT NOT NULL,
            fecha_inicio DATETIME NOT NULL,
            fecha_fin DATETIME,
            estado TEXT NOT NULL, -- Ej: 'Exitoso', 'Fallido', 'En Progreso'
            registros_procesados INTEGER,
            comentarios TEXT
        )"""
        )

        # Tabla para persistir los descriptivos de cada entidad procesada
        cursor.execute(
            """
        CREATE TABLE IF NOT EXISTS DQM_descriptivos_entidad (
            id_descriptivo INTEGER PRIMARY KEY AUTOINCREMENT,
            id_ejecucion INTEGER NOT NULL,
            nombre_entidad TEXT NOT NULL,
            nombre_columna TEXT, -- Puede ser N/A para métricas de tabla completa
            metrica TEXT NOT NULL, -- Ej: 'conteo_filas', 'valores_nulos', 'media', 'max', 'min'
            valor TEXT NOT NULL,
            FOREIGN KEY (id_ejecucion) REFERENCES DQM_ejecucion_procesos(id_ejecucion)
        )"""
        )

        # Tabla para los indicadores de calidad
        cursor.execute(
            """
        CREATE TABLE IF NOT EXISTS DQM_indicadores_calidad (
            id_indicador INTEGER PRIMARY KEY AUTOINCREMENT,
            id_ejecucion INTEGER NOT NULL,
            nombre_entidad TEXT NOT NULL,
            nombre_regla_calidad TEXT NOT NULL, -- Ej: 'unicidad_pk', 'integridad_referencial'
            resultado TEXT NOT NULL, -- Ej: 'Pasa', 'Falla'
            registros_fallidos INTEGER DEFAULT 0,
            descripcion_regla TEXT,
            FOREIGN KEY (id_ejecucion) REFERENCES DQM_ejecucion_procesos(id_ejecucion)
        )"""
        )

        logging.info("Tablas del DQM creadas con éxito.")

        # --- Registrar en Metadata ---
        logging.info("Registrando nuevas tablas en Metadata (MET_)...")
        dqm_tables = {
            "DQM_ejecucion_procesos": "Registra la ejecución de los diferentes procesos ETL/ELT del DWA.",
            "DQM_descriptivos_entidad": "Almacena estadísticas descriptivas de las entidades procesadas en cada ejecución.",
            "DQM_indicadores_calidad": "Guarda los resultados de las reglas de calidad de datos aplicadas durante los procesos.",
        }

        current_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        for table_name, description in dqm_tables.items():
            cursor.execute(
                """
                INSERT OR REPLACE INTO MET_entidades (nombre_entidad, descripcion, capa, fecha_creacion, usuario_creacion)
                VALUES (?, ?, ?, ?, ?)
                """,
                (table_name, description, "DQM", current_date, USER),
            )

        logging.info("Metadata actualizada para las tablas del DQM.")

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
    """Orquesta la creación de las tablas del DQM."""
    create_dqm_tables()


if __name__ == "__main__":
    main()
