import sqlite3
import logging
from datetime import datetime

# --- Configuración de Logging ---
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)

# --- Constantes ---
DB_PATH = ".data/tp_dwa.db"
USER = "data_engineer"


# --- Funciones Auxiliares para el DQM ---
def log_process_start(conn, process_name, comments=""):
    """Registra el inicio de un proceso en el DQM."""
    cursor = conn.cursor()
    start_time = datetime.now()
    cursor.execute(
        """
        INSERT INTO DQM_ejecucion_procesos (nombre_proceso, fecha_inicio, estado, comentarios)
        VALUES (?, ?, ?, ?)
        """,
        (process_name, start_time, "En Progreso", comments),
    )
    conn.commit()
    logging.info(f"Proceso '{process_name}' iniciado y registrado en el DQM.")
    return cursor.lastrowid, start_time


def log_process_end(conn, process_id, start_time, status, comments=""):
    """Registra la finalización de un proceso en el DQM."""
    cursor = conn.cursor()
    end_time = datetime.now()
    duration = (end_time - start_time).total_seconds()
    cursor.execute(
        """
        UPDATE DQM_ejecucion_procesos
        SET fecha_fin = ?, estado = ?, duracion_seg = ?, comentarios = ?
        WHERE id_ejecucion = ?
        """,
        (end_time, status, duration, comments, process_id),
    )
    conn.commit()
    logging.info(
        f"Proceso ID {process_id} finalizado. Estado: {status}. Duración: {duration:.2f}s."
    )


def log_dq_metric(conn, process_id, table_name, metric_name, metric_value):
    """Registra una métrica descriptiva de una entidad en el DQM."""
    cursor = conn.cursor()
    cursor.execute(
        """
        INSERT INTO DQM_descriptivos_entidad (id_ejecucion, nombre_entidad, nombre_metrica, valor_metrica)
        VALUES (?, ?, ?, ?)
        """,
        (process_id, table_name, metric_name, metric_value),
    )
    conn.commit()


def log_dq_check(conn, process_id, check_name, table_name, status, details):
    """Registra el resultado de un control de calidad en el DQM."""
    cursor = conn.cursor()
    cursor.execute(
        """
        INSERT INTO DQM_indicadores_calidad (id_ejecucion, nombre_indicador, entidad_asociada, resultado, detalles)
        VALUES (?, ?, ?, ?, ?)
        """,
        (process_id, check_name, table_name, status, details),
    )
    conn.commit()


# --- Lógica de Controles de Calidad ---
def perform_ingestion_quality_checks(conn, process_id):
    """
    Ejecuta una serie de controles de calidad sobre las tablas de Ingesta (ING_).
    """
    logging.info("--- Iniciando Controles de Calidad de Ingesta (Punto 8a) ---")
    cursor = conn.cursor()

    tables_to_check = {
        "ING_orders": "order_id",
        "ING_order_details": "order_id",
        "ING_products": "product_id",
        "ING_customers": "customer_id",
        "ING_employees": "employee_id",
    }

    overall_status = "OK"

    for table, pk_column in tables_to_check.items():
        # Conteo de Filas
        cursor.execute(f"SELECT COUNT(*) FROM {table}")
        row_count = cursor.fetchone()[0]
        log_dq_metric(conn, process_id, table, "conteo_filas", row_count)

        # Chequeo de Nulos en PK
        cursor.execute(f"SELECT COUNT(*) FROM {table} WHERE {pk_column} IS NULL")
        null_pk_count = cursor.fetchone()[0]
        status = "OK" if null_pk_count == 0 else "FALLIDO"
        if status == "FALLIDO":
            overall_status = "FALLIDO"
        log_dq_check(
            conn,
            process_id,
            "nulos_en_pk",
            table,
            status,
            f"Se encontraron {null_pk_count} claves primarias nulas.",
        )

    # Chequeo de valores negativos en order_details
    cursor.execute(
        "SELECT COUNT(*) FROM ING_order_details WHERE unit_price < 0 OR quantity < 0"
    )
    negative_values_count = cursor.fetchone()[0]
    status = "OK" if negative_values_count == 0 else "FALLIDO"
    if status == "FALLIDO":
        overall_status = "FALLIDO"
    log_dq_check(
        conn,
        process_id,
        "valores_negativos_order_details",
        "ING_order_details",
        status,
        f"Se encontraron {negative_values_count} registros con precios o cantidades negativas.",
    )

    logging.info(
        f"--- Controles de Calidad de Ingesta Finalizados. Estado General: {overall_status} ---"
    )
    return overall_status


def perform_integration_quality_checks(conn, process_id):
    """
    Ejecuta controles de calidad post-carga para verificar la integridad del DWH.
    """
    logging.info("--- Iniciando Controles de Calidad de Integración (Punto 8b) ---")
    cursor = conn.cursor()
    overall_status = "OK"

    # 1. Verificar claves foráneas nulas en la tabla de hechos
    fact_table = "DWA_FACT_Ventas"
    sk_columns = [
        "sk_cliente",
        "sk_producto",
        "sk_empleado",
        "sk_tiempo",
        "sk_geografia_envio",
        "sk_shipper",
    ]

    for sk_column in sk_columns:
        cursor.execute(f"SELECT COUNT(*) FROM {fact_table} WHERE {sk_column} IS NULL")
        null_sk_count = cursor.fetchone()[0]

        status = "OK" if null_sk_count == 0 else "ADVERTENCIA"
        if status != "OK":
            overall_status = "ADVERTENCIA"

        log_dq_check(
            conn,
            process_id,
            "fk_nulas_en_hechos",
            fact_table,
            status,
            f"La columna '{sk_column}' tiene {null_sk_count} valores nulos.",
        )

    # 2. Comparar conteo de filas entre Staging y DWH
    cursor.execute("SELECT COUNT(*) FROM ING_order_details")
    ing_count = cursor.fetchone()[0]
    cursor.execute(f"SELECT COUNT(*) FROM {fact_table}")
    dwh_count = cursor.fetchone()[0]

    log_dq_metric(conn, process_id, "ING_order_details", "conteo_filas", ing_count)
    log_dq_metric(conn, process_id, fact_table, "conteo_filas", dwh_count)

    status = "OK" if ing_count == dwh_count else "ADVERTENCIA"
    if status != "OK":
        overall_status = "ADVERTENCIA"
    log_dq_check(
        conn,
        process_id,
        "comparacion_conteo_filas",
        f"ING_order_details vs {fact_table}",
        status,
        f"ING: {ing_count} filas, DWH: {dwh_count} filas. La diferencia puede indicar duplicados por el JOIN con geografía.",
    )

    logging.info(
        f"--- Controles de Calidad de Integración Finalizados. Estado General: {overall_status} ---"
    )
    return overall_status


# --- Lógica de Carga de Dimensiones ---


def load_dim_shippers(conn):
    """Carga la dimensión de Shippers desde la tabla de staging."""
    logging.info("Iniciando la carga de DWA_DIM_Shippers...")
    cursor = conn.cursor()
    cursor.execute("DELETE FROM DWA_DIM_Shippers;")
    cursor.execute(
        """
        INSERT INTO DWA_DIM_Shippers (nk_shipper_id, nombre_compania, telefono)
        SELECT 
            shipper_id,
            company_name,
            phone
        FROM ING_shippers;
    """
    )
    count = cursor.rowcount
    conn.commit()
    logging.info(f"Carga de DWA_DIM_Shippers completada. {count} registros insertados.")
    return count


def load_dim_tiempo(conn):
    """Genera y carga la dimensión de Tiempo a partir de las fechas de las órdenes."""
    logging.info("Iniciando la carga de DWA_DIM_Tiempo...")
    cursor = conn.cursor()
    cursor.execute("DELETE FROM DWA_DIM_Tiempo;")

    # Se utiliza STRFTIME para asegurar compatibilidad con SQLite
    cursor.execute(
        """
        INSERT INTO DWA_DIM_Tiempo (sk_tiempo, fecha, anio, mes, dia, trimestre, nombre_mes, nombre_dia, es_fin_de_semana)
        WITH RECURSIVE dates(date_val) AS (
            SELECT MIN(DATE(order_date)) FROM ING_orders
            UNION ALL
            SELECT DATE(date_val, '+1 day')
            FROM dates
            WHERE date_val < (SELECT MAX(DATE(order_date)) FROM ING_orders)
        )
        SELECT
            CAST(STRFTIME('%Y%m%d', date_val) AS INTEGER),
            date_val,
            CAST(STRFTIME('%Y', date_val) AS INTEGER),
            CAST(STRFTIME('%m', date_val) AS INTEGER),
            CAST(STRFTIME('%d', date_val) AS INTEGER),
            CAST((STRFTIME('%m', date_val) - 1) / 3 + 1 AS INTEGER),
            CASE STRFTIME('%m', date_val)
                WHEN '01' THEN 'Enero' WHEN '02' THEN 'Febrero' WHEN '03' THEN 'Marzo'
                WHEN '04' THEN 'Abril' WHEN '05' THEN 'Mayo' WHEN '06' THEN 'Junio'
                WHEN '07' THEN 'Julio' WHEN '08' THEN 'Agosto' WHEN '09' THEN 'Septiembre'
                WHEN '10' THEN 'Octubre' WHEN '11' THEN 'Noviembre' WHEN '12' THEN 'Diciembre'
            END,
            CASE STRFTIME('%w', date_val)
                WHEN '0' THEN 'Domingo' WHEN '1' THEN 'Lunes' WHEN '2' THEN 'Martes'
                WHEN '3' THEN 'Miércoles' WHEN '4' THEN 'Jueves' WHEN '5' THEN 'Viernes'
                WHEN '6' THEN 'Sábado'
            END,
            CASE WHEN STRFTIME('%w', date_val) IN ('0', '6') THEN 1 ELSE 0 END
        FROM dates;
    """
    )
    count = cursor.rowcount
    conn.commit()
    logging.info(f"Carga de DWA_DIM_Tiempo completada. {count} registros insertados.")
    return count


def load_dim_productos(conn):
    """Carga la dimensión de Productos desnormalizando desde staging."""
    logging.info("Iniciando la carga de DWA_DIM_Productos...")
    cursor = conn.cursor()
    cursor.execute("DELETE FROM DWA_DIM_Productos;")
    cursor.execute(
        """
        INSERT INTO DWA_DIM_Productos (
            nk_producto_id, nombre_producto, cantidad_por_unidad, precio_unitario,
            nombre_categoria, descripcion_categoria, nombre_proveedor, pais_proveedor,
            descontinuado
        )
        SELECT
            p.product_id,
            p.product_name,
            p.quantity_per_unit,
            p.unit_price,
            c.category_name,
            c.description,
            s.company_name,
            s.country,
            p.discontinued
        FROM ING_products p
        LEFT JOIN ING_categories c ON p.category_id = c.category_id
        LEFT JOIN ING_suppliers s ON p.supplier_id = s.supplier_id;
    """
    )
    count = cursor.rowcount
    conn.commit()
    logging.info(
        f"Carga de DWA_DIM_Productos completada. {count} registros insertados."
    )
    return count


def load_dim_empleados(conn):
    """Carga la dimensión de Empleados con datos enriquecidos y desnormalizados."""
    logging.info("Iniciando la carga de DWA_DIM_Empleados...")
    cursor = conn.cursor()
    cursor.execute("DELETE FROM DWA_DIM_Empleados;")
    # JULIANDAY es una función de SQLite para manejar fechas
    cursor.execute(
        """
        INSERT INTO DWA_DIM_Empleados (
            nk_empleado_id, nombre_completo, titulo, fecha_nacimiento, fecha_contratacion,
            edad_en_contratacion, ciudad, region, pais, nombre_jefe
        )
        SELECT
            e.employee_id,
            e.first_name || ' ' || e.last_name,
            e.title,
            e.birth_date,
            e.hire_date,
            CAST( (STRFTIME('%s', e.hire_date) - STRFTIME('%s', e.birth_date)) / 31557600 AS INTEGER),
            e.city,
            e.region,
            e.country,
            j.first_name || ' ' || j.last_name
        FROM ING_employees e
        LEFT JOIN ING_employees j ON e.reports_to = j.employee_id;
    """
    )
    count = cursor.rowcount
    conn.commit()
    logging.info(
        f"Carga de DWA_DIM_Empleados completada. {count} registros insertados."
    )
    return count


def load_dim_clientes(conn):
    """Carga la dimensión de Clientes (SCD Tipo 2) para la carga inicial."""
    logging.info("Iniciando la carga de DWA_DIM_Clientes...")
    cursor = conn.cursor()
    cursor.execute("DELETE FROM DWA_DIM_Clientes;")

    current_date = datetime.now().strftime("%Y-%m-%d")

    cursor.execute(
        """
        INSERT INTO DWA_DIM_Clientes (
            nk_cliente_id, nombre_compania, nombre_contacto, titulo_contacto,
            direccion, ciudad, region, codigo_postal, pais,
            fecha_inicio_validez, fecha_fin_validez, es_vigente
        )
        SELECT
            customer_id,
            company_name,
            contact_name,
            contact_title,
            address,
            city,
            region,
            postal_code,
            country,
            ?, -- fecha_inicio_validez
            NULL, -- fecha_fin_validez
            1 -- es_vigente
        FROM ING_customers;
    """,
        (current_date,),
    )
    count = cursor.rowcount
    conn.commit()
    logging.info(f"Carga de DWA_DIM_Clientes completada. {count} registros insertados.")
    return count


def load_dim_geografia(conn):
    """Consolida y carga la dimensión de Geografía desde múltiples fuentes y la enriquece."""
    logging.info("Iniciando la carga de DWA_DIM_Geografia...")
    cursor = conn.cursor()
    cursor.execute("DELETE FROM DWA_DIM_Geografia;")

    cursor.execute(
        """
        INSERT INTO DWA_DIM_Geografia (
            direccion, ciudad, region, codigo_postal, pais,
            densidad_poblacion, pib, esperanza_de_vida
        )
        WITH geolocations AS (
            SELECT address, city, region, postal_code, country FROM ING_customers
            UNION
            SELECT address, city, region, postal_code, country FROM ING_employees
            UNION
            SELECT address, city, region, postal_code, country FROM ING_suppliers
            UNION
            SELECT ship_address, ship_city, ship_region, ship_postal_code, ship_country FROM ING_orders
        )
        SELECT DISTINCT
            g.address,
            g.city,
            g.region,
            g.postal_code,
            g.country,
            wd.density,
            wd.gdp,
            wd.life_expectancy
        FROM geolocations g
        LEFT JOIN ING_world_data_2023 wd ON g.country = wd.country
        WHERE g.address IS NOT NULL OR g.city IS NOT NULL OR g.country IS NOT NULL;
    """
    )
    count = cursor.rowcount
    conn.commit()
    logging.info(
        f"Carga de DWA_DIM_Geografia completada. {count} registros insertados."
    )
    return count


# --- Lógica de Carga de la Tabla de Hechos ---
def load_fact_ventas(conn):
    """Carga la tabla de hechos de Ventas uniendo staging y dimensiones."""
    logging.info("Iniciando la carga de DWA_FACT_Ventas...")
    cursor = conn.cursor()
    cursor.execute("DELETE FROM DWA_FACT_Ventas;")

    cursor.execute(
        """
        INSERT INTO DWA_FACT_Ventas (
            sk_cliente, sk_producto, sk_empleado, sk_tiempo, sk_geografia_envio, sk_shipper,
            precio_unitario, cantidad, descuento, flete, monto_total, nk_orden_id
        )
        SELECT
            dc.sk_cliente,
            dp.sk_producto,
            de.sk_empleado,
            CAST(STRFTIME('%Y%m%d', o.order_date) AS INTEGER),
            dg.sk_geografia,
            ds.sk_shipper,
            od.unit_price,
            od.quantity,
            od.discount,
            o.freight,
            (od.unit_price * od.quantity * (1 - od.discount)),
            o.order_id
        FROM ING_orders o
        JOIN ING_order_details od ON o.order_id = od.order_id
        LEFT JOIN DWA_DIM_Clientes dc ON o.customer_id = dc.nk_cliente_id AND dc.es_vigente = 1
        LEFT JOIN DWA_DIM_Productos dp ON od.product_id = dp.nk_producto_id
        LEFT JOIN DWA_DIM_Empleados de ON o.employee_id = de.nk_empleado_id
        LEFT JOIN DWA_DIM_Geografia dg ON o.ship_address = dg.direccion AND o.ship_city = dg.ciudad AND o.ship_country = dg.pais
        LEFT JOIN DWA_DIM_Shippers ds ON o.ship_via = ds.nk_shipper_id;
    """
    )
    count = cursor.rowcount
    conn.commit()
    logging.info(f"Carga de DWA_FACT_Ventas completada. {count} registros insertados.")
    return count


# --- Orquestador Principal ---


def main():
    """
    Orquesta todo el proceso de carga del DWH, incluyendo los controles de calidad.
    """
    logging.info("Iniciando el Paso 8: Carga Inicial del DWH.")
    conn = None
    process_id, start_time = None, None
    try:
        conn = sqlite3.connect(DB_PATH)

        # 1. Iniciar el log del proceso principal
        process_id, start_time = log_process_start(
            conn, "CargaInicialDWH", "Proceso completo del Step 7"
        )

        # 2. Ejecutar Controles de Calidad de Ingesta (Punto 8a)
        ingestion_status = perform_ingestion_quality_checks(conn, process_id)

        # Si los controles de ingesta fallan, detenemos el proceso.
        # En un escenario real, esto podría enviar una alerta.
        if ingestion_status == "FALLIDO":
            raise Exception(
                "Los controles de calidad de ingesta han fallado. Abortando la carga del DWH."
            )

        # 3. Cargar el DWH (Dimensiones y Hechos)
        logging.info("--- Iniciando Carga de Dimensiones ---")
        load_dim_shippers(conn)
        load_dim_tiempo(conn)
        load_dim_productos(conn)
        load_dim_empleados(conn)
        load_dim_clientes(conn)
        load_dim_geografia(conn)
        logging.info("--- Carga de Dimensiones Finalizada ---")

        # Cargar Tabla de Hechos (Ventas)
        logging.info("--- Iniciando Carga de la Tabla de Hechos ---")
        ventas_count = load_fact_ventas(conn)
        logging.info("--- Carga de la Tabla de Hechos Finalizada ---")

        # 4. Ejecutar Controles de Calidad de Integración (Punto 8b)
        integration_status = perform_integration_quality_checks(conn, process_id)

        final_status = "Exitoso"
        # Si cualquier chequeo da ADVERTENCIA o FALLIDO, el estado final lo reflejará.
        if "ADVERTENCIA" in (ingestion_status, integration_status):
            final_status = "Exitoso con Advertencias"
        if "FALLIDO" in (ingestion_status, integration_status):
            final_status = "FALLIDO"

        # 5. Finalizar el log del proceso con el estado final
        log_process_end(
            conn,
            process_id,
            start_time,
            final_status,
            "El DWH se ha cargado y verificado.",
        )

    except Exception as e:
        logging.error(f"Error en la base de datos durante la carga del DWH: {e}")
        if conn and process_id:
            # Registrar el fallo en el DQM
            log_process_end(conn, process_id, start_time, "FALLIDO", str(e))
    finally:
        if conn:
            conn.close()
            logging.info("Conexión a la base de datos cerrada.")

    logging.info("--- Paso 7: Finalizado ---")


if __name__ == "__main__":
    main()
