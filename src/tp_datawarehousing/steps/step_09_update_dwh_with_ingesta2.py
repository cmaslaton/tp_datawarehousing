import sqlite3
import logging
from datetime import datetime, timedelta

# --- Configuración de Logging ---
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)

# --- Constantes ---
DB_PATH = "db/tp_dwa.db"
USER = "data_engineer_updater"


# --- Funciones Auxiliares para el DQM (reutilizables) ---
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


def log_dq_metric(conn, process_id, table_name, metric_name, metric_value):
    """Registra una métrica descriptiva de una entidad en el DQM."""
    cursor = conn.cursor()
    cursor.execute(
        """
        INSERT INTO DQM_descriptivos_entidad (id_ejecucion, nombre_entidad, nombre_metrica, valor_metrica)
        VALUES (?, ?, ?, ?)
        """,
        (process_id, table_name, metric_name, str(metric_value)),
    )
    conn.commit()


# --- Lógica de Actualización (SCD Tipo 2 para Clientes) ---
def update_scd2_clientes(conn, process_id):
    """
    Actualiza la dimensión de clientes usando la lógica de Slowly Changing Dimension Tipo 2.
    """
    logging.info("--- Iniciando actualización de DWA_DIM_Clientes (SCD Tipo 2) ---")
    cursor = conn.cursor()

    # 1. Identificar clientes con cambios (dirección, contacto, etc.)
    # Comparamos los registros vigentes en el DWH con los datos de Ingesta2
    query_changed_customers = """
        SELECT
            d.sk_cliente,
            t.customer_id,
            t.company_name,
            t.contact_name,
            t.contact_title,
            t.address,
            t.city,
            t.region,
            t.postal_code,
            t.country
        FROM TMP2_customers t
        JOIN DWA_DIM_Clientes d ON t.customer_id = d.nk_cliente_id
        WHERE d.es_vigente = 1 AND (
            d.direccion != t.address OR
            d.ciudad != t.city OR
            d.region != t.region OR
            d.codigo_postal != t.postal_code OR
            d.pais != t.country OR
            d.nombre_contacto != t.contact_name OR
            d.titulo_contacto != t.contact_title
        );
    """
    cursor.execute(query_changed_customers)
    changed_customers = cursor.fetchall()

    today = datetime.now().date()
    yesterday = today - timedelta(days=1)

    # 2. Expirar los registros viejos
    for customer in changed_customers:
        sk_cliente_to_expire = customer[0]
        logging.info(
            f"Expirando registro antiguo para cliente con sk_cliente = {sk_cliente_to_expire}"
        )
        cursor.execute(
            """
            UPDATE DWA_DIM_Clientes
            SET fecha_fin_validez = ?, es_vigente = 0
            WHERE sk_cliente = ?
            """,
            (yesterday.isoformat(), sk_cliente_to_expire),
        )

    # 3. Insertar los registros nuevos (actualizados)
    for customer_data in changed_customers:
        # sk_cliente (customer_data[0]) no se usa para la inserción
        logging.info(
            f"Insertando nuevo registro para cliente con nk_cliente_id = {customer_data[1]}"
        )
        cursor.execute(
            """
            INSERT INTO DWA_DIM_Clientes (
                nk_cliente_id, nombre_compania, nombre_contacto, titulo_contacto,
                direccion, ciudad, region, codigo_postal, pais,
                fecha_inicio_validez, fecha_fin_validez, es_vigente
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                customer_data[1],
                customer_data[2],
                customer_data[3],
                customer_data[4],
                customer_data[5],
                customer_data[6],
                customer_data[7],
                customer_data[8],
                customer_data[9],
                today.isoformat(),
                None,
                1,
            ),
        )

    # 4. Identificar y insertar clientes completamente nuevos
    query_new_customers = """
        SELECT * FROM TMP2_customers t
        WHERE NOT EXISTS (
            SELECT 1 FROM DWA_DIM_Clientes d WHERE d.nk_cliente_id = t.customer_id
        );
    """
    cursor.execute(query_new_customers)
    new_customers = cursor.fetchall()

    for new_customer in new_customers:
        logging.info(f"Insertando cliente completamente nuevo: {new_customer[0]}")
        cursor.execute(
            """
            INSERT INTO DWA_DIM_Clientes (
                nk_cliente_id, nombre_compania, nombre_contacto, titulo_contacto,
                direccion, ciudad, region, codigo_postal, pais,
                fecha_inicio_validez, fecha_fin_validez, es_vigente
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                new_customer[0],
                new_customer[1],
                new_customer[2],
                new_customer[3],
                new_customer[4],
                new_customer[5],
                new_customer[6],
                new_customer[7],
                new_customer[8],
                today.isoformat(),
                None,
                1,
            ),
        )

    conn.commit()

    # Registrar métricas en DQM
    log_dq_metric(
        conn,
        process_id,
        "DWA_DIM_Clientes",
        "registros_modificados",
        len(changed_customers),
    )
    log_dq_metric(
        conn, process_id, "DWA_DIM_Clientes", "registros_nuevos", len(new_customers)
    )

    logging.info(
        f"Actualización de DWA_DIM_Clientes (SCD2) completada. {len(changed_customers)} registros actualizados, {len(new_customers)} nuevos."
    )


def update_fact_ventas(conn, process_id):
    """
    Actualiza la tabla de hechos con los datos de Ingesta2.
    Maneja modificaciones de hechos existentes e inserción de nuevos hechos.
    """
    logging.info("--- Iniciando actualización de DWA_FACT_Ventas ---")
    cursor = conn.cursor()

    # 1. Identificar y actualizar los hechos existentes (modificaciones)
    # Se actualizan las métricas de las filas de hechos que coinciden en order_id y product_id.
    update_query = """
        UPDATE DWA_FACT_Ventas
        SET
            precio_unitario = (SELECT unit_price FROM TMP2_order_details t WHERE t.order_id = nk_orden_id AND t.product_id = (SELECT nk_producto_id FROM DWA_DIM_Productos dp WHERE dp.sk_producto = DWA_FACT_Ventas.sk_producto)),
            cantidad = (SELECT quantity FROM TMP2_order_details t WHERE t.order_id = nk_orden_id AND t.product_id = (SELECT nk_producto_id FROM DWA_DIM_Productos dp WHERE dp.sk_producto = DWA_FACT_Ventas.sk_producto)),
            descuento = (SELECT discount FROM TMP2_order_details t WHERE t.order_id = nk_orden_id AND t.product_id = (SELECT nk_producto_id FROM DWA_DIM_Productos dp WHERE dp.sk_producto = DWA_FACT_Ventas.sk_producto)),
            monto_total = (SELECT unit_price * quantity * (1 - discount) FROM TMP2_order_details t WHERE t.order_id = nk_orden_id AND t.product_id = (SELECT nk_producto_id FROM DWA_DIM_Productos dp WHERE dp.sk_producto = DWA_FACT_Ventas.sk_producto))
        WHERE EXISTS (
            SELECT 1 FROM TMP2_order_details t
            JOIN DWA_DIM_Productos dp ON t.product_id = dp.nk_producto_id
            WHERE DWA_FACT_Ventas.nk_orden_id = t.order_id AND DWA_FACT_Ventas.sk_producto = dp.sk_producto
        );
    """
    cursor.execute(update_query)
    updated_count = cursor.rowcount
    log_dq_metric(
        conn, process_id, "DWA_FACT_Ventas", "registros_modificados", updated_count
    )
    logging.info(f"{updated_count} hechos existentes fueron actualizados.")

    # 2. Insertar nuevos hechos (altas)
    # Se insertan las filas de TMP2_order_details que no tienen una contraparte en DWA_FACT_Ventas.
    insert_query = """
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
        FROM TMP2_orders o
        JOIN TMP2_order_details od ON o.order_id = od.order_id
        LEFT JOIN DWA_DIM_Clientes dc ON o.customer_id = dc.nk_cliente_id AND dc.es_vigente = 1
        LEFT JOIN DWA_DIM_Productos dp ON od.product_id = dp.nk_producto_id
        LEFT JOIN DWA_DIM_Empleados de ON o.employee_id = de.nk_empleado_id
        LEFT JOIN DWA_DIM_Geografia dg ON o.ship_address = dg.direccion AND o.ship_city = dg.ciudad AND o.ship_country = dg.pais
        LEFT JOIN DWA_DIM_Shippers ds ON o.ship_via = ds.nk_shipper_id
        WHERE NOT EXISTS (
            SELECT 1 FROM DWA_FACT_Ventas fv
            WHERE fv.nk_orden_id = o.order_id AND fv.sk_producto = dp.sk_producto
        );
    """
    cursor.execute(insert_query)
    inserted_count = cursor.rowcount
    log_dq_metric(
        conn, process_id, "DWA_FACT_Ventas", "registros_nuevos", inserted_count
    )
    logging.info(f"{inserted_count} nuevos hechos fueron insertados.")

    conn.commit()
    logging.info("--- Actualización de DWA_FACT_Ventas completada ---")


def main():
    """
    Orquesta la actualización del DWH con los datos de Ingesta2.
    """
    logging.info("Iniciando el Paso 9: Actualización del DWH con Ingesta2.")
    conn = None
    process_id = -1
    start_time = datetime.now()

    try:
        conn = sqlite3.connect(DB_PATH)
        process_id, start_time = log_process_start(
            conn, "Actualizacion DWH con Ingesta2"
        )

        # --- Actualizar Dimensiones ---
        update_scd2_clientes(conn, process_id)

        # --- Actualizar Tabla de Hechos ---
        update_fact_ventas(conn, process_id)

        log_process_end(
            conn,
            process_id,
            start_time,
            "Exitoso",
            "Actualización con Ingesta2 completada.",
        )

    except sqlite3.Error as e:
        logging.error(f"Error de base de datos en el Paso 9: {e}")
        if conn and process_id != -1:
            log_process_end(conn, process_id, start_time, "Fallido", str(e))
    finally:
        if conn:
            conn.close()
            logging.info("Conexión a la base de datos cerrada.")


if __name__ == "__main__":
    main()
