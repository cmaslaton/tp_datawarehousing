import sqlite3
import logging
from datetime import datetime

# --- Configuración de Logging ---
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)

# --- Constantes ---
DB_PATH = "db/tp_dwa.db"
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

        # --- Borrado de Tablas (para asegurar recreación con esquema correcto) ---
        logging.info("Asegurando la recreación de las tablas del DQM...")
        cursor.execute("DROP TABLE IF EXISTS DQM_indicadores_calidad;")
        cursor.execute("DROP TABLE IF EXISTS DQM_descriptivos_entidad;")
        cursor.execute("DROP TABLE IF EXISTS DQM_ejecucion_procesos;")
        conn.commit()

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
            comentarios TEXT,
            duracion_seg REAL
        )"""
        )

        # Tabla para persistir los descriptivos de cada entidad procesada
        cursor.execute(
            """
        CREATE TABLE IF NOT EXISTS DQM_descriptivos_entidad (
            id_descriptivo INTEGER PRIMARY KEY AUTOINCREMENT,
            id_ejecucion INTEGER NOT NULL,
            nombre_entidad TEXT NOT NULL,
            nombre_metrica TEXT,
            valor_metrica TEXT,
            FOREIGN KEY (id_ejecucion) REFERENCES DQM_ejecucion_procesos(id_ejecucion)
        )"""
        )

        # Tabla para los indicadores de calidad
        cursor.execute(
            """
        CREATE TABLE IF NOT EXISTS DQM_indicadores_calidad (
            id_indicador INTEGER PRIMARY KEY AUTOINCREMENT,
            id_ejecucion INTEGER,
            nombre_indicador TEXT,
            entidad_asociada TEXT,
            resultado TEXT,
            detalles TEXT,
            FOREIGN KEY (id_ejecucion) REFERENCES DQM_ejecucion_procesos(id_ejecucion)
        )"""
        )

        logging.info("Tablas del DQM creadas con éxito.")

        # --- Registrar en Metadata ---
        logging.info("Registrando nuevas tablas en Metadata (MET_)...")
        dqm_tables = [
            (
                "DQM_ejecucion_procesos",
                "Almacena el log de ejecución de cada proceso ETL.",
                "id_ejecucion (PK), nombre_proceso, fecha_inicio, fecha_fin, estado, comentarios, duracion_seg",
                USER,
            ),
            (
                "DQM_descriptivos_entidad",
                "Almacena métricas descriptivas de las entidades en cada ejecución.",
                "id_descriptivo (PK), id_ejecucion (FK), nombre_entidad, nombre_metrica, valor_metrica",
                USER,
            ),
            (
                "DQM_indicadores_calidad",
                "Registra los resultados de los controles de calidad ejecutados.",
                "id_indicador (PK), id_ejecucion (FK), nombre_indicador, entidad_asociada, resultado, detalles",
                USER,
            ),
        ]
        current_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        for table_name, description, columns, user in dqm_tables:
            cursor.execute(
                """
                INSERT OR REPLACE INTO MET_entidades (nombre_entidad, descripcion, capa, fecha_creacion, usuario_creacion)
                VALUES (?, ?, ?, ?, ?)
                """,
                (table_name, description, "DQM", current_date, user),
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
