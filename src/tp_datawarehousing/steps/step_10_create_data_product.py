import sqlite3
import logging
from datetime import datetime

# --- Configuración de Logging ---
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)

# --- Constantes ---
DB_PATH = ".data/tp_dwa.db"
USER = "data_analyst"


# --- Funciones Auxiliares para el DQM (reutilizadas de pasos anteriores) ---
def log_process_start(conn, process_name, comments=""):
    cursor = conn.cursor()
    start_time = datetime.now()
    cursor.execute(
        "INSERT INTO DQM_ejecucion_procesos (nombre_proceso, fecha_inicio, estado, comentarios) VALUES (?, ?, ?, ?)",
        (process_name, start_time, "En Progreso", comments),
    )
    conn.commit()
    return cursor.lastrowid, start_time


def log_process_end(conn, process_id, start_time, status, comments=""):
    cursor = conn.cursor()
    end_time = datetime.now()
    duration = (end_time - start_time).total_seconds()
    cursor.execute(
        "UPDATE DQM_ejecucion_procesos SET fecha_fin = ?, estado = ?, duracion_seg = ?, comentarios = ? WHERE id_ejecucion = ?",
        (end_time, status, duration, comments, process_id),
    )
    conn.commit()


def create_sales_data_product(conn, process_id):
    """
    Crea un producto de datos agregado para el análisis de ventas.
    """
    logging.info(
        "--- Iniciando creación del producto de datos: Ventas Mensuales por Categoría y País ---"
    )
    cursor = conn.cursor()

    table_name = "DP_Ventas_Mensuales_Categoria_Pais"

    # 1. Crear la tabla del producto de datos
    cursor.execute(f"DROP TABLE IF EXISTS {table_name};")
    cursor.execute(
        f"""
        CREATE TABLE {table_name} (
            anio INTEGER,
            mes INTEGER,
            nombre_categoria TEXT,
            pais TEXT,
            total_ventas REAL
        );
    """
    )
    logging.info(f"Tabla {table_name} creada con éxito.")

    # 2. Poblar la tabla con datos agregados del DWH
    insert_query = f"""
        INSERT INTO {table_name} (anio, mes, nombre_categoria, pais, total_ventas)
        SELECT
            t.anio,
            t.mes,
            p.nombre_categoria,
            g.pais,
            SUM(fv.monto_total) as total_ventas
        FROM DWA_FACT_Ventas fv
        JOIN DWA_DIM_Tiempo t ON fv.sk_tiempo = t.sk_tiempo
        JOIN DWA_DIM_Productos p ON fv.sk_producto = p.sk_producto
        JOIN DWA_DIM_Geografia g ON fv.sk_geografia_envio = g.sk_geografia
        WHERE g.pais IS NOT NULL AND p.nombre_categoria IS NOT NULL
        GROUP BY t.anio, t.mes, p.nombre_categoria, g.pais
        ORDER BY t.anio, t.mes, total_ventas DESC;
    """
    cursor.execute(insert_query)
    inserted_count = cursor.rowcount
    logging.info(f"{inserted_count} registros agregados insertados en {table_name}.")

    # 3. Registrar el nuevo producto de datos en la metadata
    description = "Producto de datos que resume las ventas totales mensuales por categoría de producto y país de envío."
    current_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    cursor.execute(
        "INSERT OR REPLACE INTO MET_entidades (nombre_entidad, descripcion, capa, fecha_creacion, usuario_creacion) VALUES (?, ?, ?, ?, ?)",
        (table_name, description, "DataProduct", current_date, USER),
    )
    logging.info(f"Producto de datos {table_name} registrado en la metadata.")

    conn.commit()
    logging.info(f"--- Creación del producto de datos {table_name} finalizada ---")
    return inserted_count


def main():
    """
    Orquesta la creación de productos de datos.
    """
    logging.info("Iniciando el Paso 10: Creación de Productos de Datos.")
    conn = None
    process_id = -1
    start_time = datetime.now()

    try:
        conn = sqlite3.connect(DB_PATH)
        process_id, start_time = log_process_start(
            conn, "CreacionDataProduct", "Genera DPs para explotación."
        )

        create_sales_data_product(conn, process_id)

        log_process_end(
            conn, process_id, start_time, "Exitoso", "Productos de datos generados."
        )

    except sqlite3.Error as e:
        logging.error(f"Error de base de datos en el Paso 10: {e}")
        if conn and process_id != -1:
            log_process_end(conn, process_id, start_time, "Fallido", str(e))
    finally:
        if conn:
            conn.close()
            logging.info("Conexión a la base de datos cerrada.")


if __name__ == "__main__":
    main()
