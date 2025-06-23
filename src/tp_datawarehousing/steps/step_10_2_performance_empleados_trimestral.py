import sqlite3
import logging
from datetime import datetime

# --- Configuración de Logging ---
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)

# --- Constantes ---
DB_PATH = "db/tp_dwa.db"
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


def create_dp2_performance_empleados_trimestral(conn, process_id):
    """
    Crea el Producto de Datos 2: Análisis de performance de empleados por trimestre.
    """
    logging.info(
        "--- Iniciando creación del DP2: Performance de Empleados Trimestral ---"
    )
    cursor = conn.cursor()

    table_name = "DP2_Performance_Empleados_Trimestral"

    # 1. Crear la tabla del producto de datos
    cursor.execute(f"DROP TABLE IF EXISTS {table_name};")
    cursor.execute(
        f"""
        CREATE TABLE {table_name} (
            anio INTEGER,
            trimestre INTEGER,
            sk_empleado INTEGER,
            nombre_completo TEXT,
            titulo TEXT,
            edad_en_contratacion INTEGER,
            ciudad TEXT,
            pais TEXT,
            ordenes_procesadas INTEGER,
            ventas_totales REAL,
            venta_promedio_por_orden REAL,
            productos_diferentes_vendidos INTEGER,
            clientes_diferentes_atendidos INTEGER,
            ranking_ventas_trimestre INTEGER
        );
    """
    )
    logging.info(f"Tabla {table_name} creada con éxito.")

    # 2. Poblar la tabla con datos agregados del DWH
    insert_query = f"""
        INSERT INTO {table_name} (
            anio, trimestre, sk_empleado, nombre_completo, titulo, 
            edad_en_contratacion, ciudad, pais,
            ordenes_procesadas, ventas_totales, venta_promedio_por_orden,
            productos_diferentes_vendidos, clientes_diferentes_atendidos,
            ranking_ventas_trimestre
        )
        WITH performance_data AS (
            SELECT 
                t.anio,
                t.trimestre,
                e.sk_empleado,
                e.nombre_completo,
                e.titulo,
                e.edad_en_contratacion,
                e.ciudad,
                e.pais,
                COUNT(DISTINCT fv.nk_orden_id) as ordenes_procesadas,
                SUM(fv.monto_total) as ventas_totales,
                AVG(fv.monto_total) as venta_promedio_por_orden,
                COUNT(DISTINCT fv.sk_producto) as productos_diferentes_vendidos,
                COUNT(DISTINCT fv.sk_cliente) as clientes_diferentes_atendidos
            FROM DWA_FACT_Ventas fv
            JOIN DWA_DIM_Empleados e ON fv.sk_empleado = e.sk_empleado
            JOIN DWA_DIM_Tiempo t ON fv.sk_tiempo = t.sk_tiempo
            WHERE e.nombre_completo IS NOT NULL
            GROUP BY t.anio, t.trimestre, e.sk_empleado, e.nombre_completo, 
                     e.titulo, e.edad_en_contratacion, e.ciudad, e.pais
        )
        SELECT 
            *,
            RANK() OVER (
                PARTITION BY anio, trimestre 
                ORDER BY ventas_totales DESC
            ) as ranking_ventas_trimestre
        FROM performance_data
        ORDER BY anio, trimestre, ventas_totales DESC;
    """
    cursor.execute(insert_query)
    inserted_count = cursor.rowcount
    logging.info(f"{inserted_count} registros de performance insertados en {table_name}.")

    # 3. Registrar el nuevo producto de datos en la metadata
    description = "DP2: Producto de datos que analiza la performance trimestral de empleados incluyendo ventas, órdenes procesadas, ranking y diversidad de productos/clientes."
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
    Orquesta la creación del DP2: Performance de Empleados Trimestral.
    """
    logging.info("Iniciando el Paso 10.2: Creación del DP2 - Performance de Empleados Trimestral.")
    conn = None
    process_id = -1
    start_time = datetime.now()

    try:
        conn = sqlite3.connect(DB_PATH)
        process_id, start_time = log_process_start(
            conn, "CreacionDP2_PerformanceEmpleados", "Genera DP2 para análisis de performance de empleados por trimestre."
        )

        create_dp2_performance_empleados_trimestral(conn, process_id)

        log_process_end(
            conn, process_id, start_time, "Exitoso", "DP2 - Performance de Empleados Trimestral generado."
        )

    except sqlite3.Error as e:
        logging.error(f"Error de base de datos en el Paso 10.2: {e}")
        if conn and process_id != -1:
            log_process_end(conn, process_id, start_time, "Fallido", str(e))
    finally:
        if conn:
            conn.close()
            logging.info("Conexión a la base de datos cerrada.")


if __name__ == "__main__":
    main() 