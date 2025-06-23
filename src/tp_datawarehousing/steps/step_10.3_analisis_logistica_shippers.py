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


def create_dp3_analisis_logistica_shippers(conn, process_id):
    """
    Crea el Producto de Datos 3: Análisis de logística y performance de shippers.
    """
    logging.info(
        "--- Iniciando creación del DP3: Análisis de Logística y Shippers ---"
    )
    cursor = conn.cursor()

    table_name = "DP3_Analisis_Logistica_Shippers"

    # 1. Crear la tabla del producto de datos
    cursor.execute(f"DROP TABLE IF EXISTS {table_name};")
    cursor.execute(
        f"""
        CREATE TABLE {table_name} (
            anio INTEGER,
            trimestre INTEGER,
            sk_shipper INTEGER,
            nombre_shipper TEXT,
            telefono_shipper TEXT,
            pais_destino TEXT,
            region_destino TEXT,
            ciudad_destino TEXT,
            envios_realizados INTEGER,
            costo_flete_total REAL,
            costo_flete_promedio REAL,
            volumen_productos_enviados INTEGER,
            ventas_totales_asociadas REAL,
            ratio_flete_vs_ventas REAL,
            densidad_poblacion_promedio REAL,
            pib_promedio_destino REAL,
            ranking_eficiencia_costo INTEGER,
            ranking_volumen_trimestre INTEGER
        );
    """
    )
    logging.info(f"Tabla {table_name} creada con éxito.")

    # 2. Poblar la tabla con datos agregados del DWH
    insert_query = f"""
        INSERT INTO {table_name} (
            anio, trimestre, sk_shipper, nombre_shipper, telefono_shipper,
            pais_destino, region_destino, ciudad_destino,
            envios_realizados, costo_flete_total, costo_flete_promedio,
            volumen_productos_enviados, ventas_totales_asociadas,
            ratio_flete_vs_ventas, densidad_poblacion_promedio, pib_promedio_destino,
            ranking_eficiencia_costo, ranking_volumen_trimestre
        )
        WITH logistica_data AS (
            SELECT 
                t.anio,
                t.trimestre,
                s.sk_shipper,
                s.nombre_compania as nombre_shipper,
                s.telefono as telefono_shipper,
                g.pais as pais_destino,
                g.region as region_destino,
                g.ciudad as ciudad_destino,
                COUNT(DISTINCT fv.nk_orden_id) as envios_realizados,
                SUM(fv.flete) as costo_flete_total,
                AVG(fv.flete) as costo_flete_promedio,
                SUM(fv.cantidad) as volumen_productos_enviados,
                SUM(fv.monto_total) as ventas_totales_asociadas,
                CASE 
                    WHEN SUM(fv.monto_total) > 0 
                    THEN (SUM(fv.flete) / SUM(fv.monto_total)) * 100 
                    ELSE 0 
                END as ratio_flete_vs_ventas,
                AVG(g.densidad_poblacion) as densidad_poblacion_promedio,
                AVG(g.pib) as pib_promedio_destino
            FROM DWA_FACT_Ventas fv
            JOIN DWA_DIM_Shippers s ON fv.sk_shipper = s.sk_shipper
            JOIN DWA_DIM_Geografia g ON fv.sk_geografia_envio = g.sk_geografia
            JOIN DWA_DIM_Tiempo t ON fv.sk_tiempo = t.sk_tiempo
            WHERE s.nombre_compania IS NOT NULL 
                AND g.pais IS NOT NULL
                AND fv.flete IS NOT NULL
                AND fv.flete > 0
            GROUP BY t.anio, t.trimestre, s.sk_shipper, s.nombre_compania, s.telefono,
                     g.pais, g.region, g.ciudad
        )
        SELECT 
            *,
            RANK() OVER (
                PARTITION BY anio, trimestre 
                ORDER BY ratio_flete_vs_ventas ASC
            ) as ranking_eficiencia_costo,
            RANK() OVER (
                PARTITION BY anio, trimestre 
                ORDER BY volumen_productos_enviados DESC
            ) as ranking_volumen_trimestre
        FROM logistica_data
        ORDER BY anio, trimestre, ranking_eficiencia_costo, volumen_productos_enviados DESC;
    """
    cursor.execute(insert_query)
    inserted_count = cursor.rowcount
    logging.info(f"{inserted_count} registros de análisis logístico insertados en {table_name}.")

    # 3. Registrar el nuevo producto de datos en la metadata
    description = "DP3: Producto de datos que analiza la performance logística de shippers incluyendo costos, volúmenes, eficiencia y rankings por destino geográfico."
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
    Orquesta la creación del DP3: Análisis de Logística y Shippers.
    """
    logging.info("Iniciando el Paso 10.3: Creación del DP3 - Análisis de Logística y Shippers.")
    conn = None
    process_id = -1
    start_time = datetime.now()

    try:
        conn = sqlite3.connect(DB_PATH)
        process_id, start_time = log_process_start(
            conn, "CreacionDP3_AnalisisLogistica", "Genera DP3 para análisis de performance logística y shippers."
        )

        create_dp3_analisis_logistica_shippers(conn, process_id)

        log_process_end(
            conn, process_id, start_time, "Exitoso", "DP3 - Análisis de Logística y Shippers generado."
        )

    except sqlite3.Error as e:
        logging.error(f"Error de base de datos en el Paso 10.3: {e}")
        if conn and process_id != -1:
            log_process_end(conn, process_id, start_time, "Fallido", str(e))
    finally:
        if conn:
            conn.close()
            logging.info("Conexión a la base de datos cerrada.")


if __name__ == "__main__":
    main() 