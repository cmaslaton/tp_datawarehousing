import sqlite3
import logging
from datetime import datetime, timedelta
from tp_datawarehousing.quality_utils import (
    get_process_execution_id,
    update_process_execution,
    log_quality_metric,
    validate_table_count,
    validate_no_nulls,
    validate_referential_integrity,
    log_record_count,
)

# --- Configuración de Logging ---
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)

# --- Constantes ---
DB_PATH = "db/tp_dwa.db"
USER = "data_engineer_updater"


# --- Funciones de Compatibilidad (mantenidas para no romper código existente) ---
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

    # Registrar métricas usando framework unificado
    log_record_count(
        process_id, "MODIFIED_SCD2", "DWA_DIM_Clientes", len(changed_customers)
    )
    log_record_count(process_id, "NEW_SCD2", "DWA_DIM_Clientes", len(new_customers))

    # Registrar métricas en DQM (compatibilidad)
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

    # Retornar resultados para el framework de calidad
    return {
        "modificados": len(changed_customers),
        "nuevos": len(new_customers),
        "total_procesados": len(changed_customers) + len(new_customers),
    }


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

    # Registrar métricas usando framework unificado
    log_record_count(process_id, "UPDATED_FACTS", "DWA_FACT_Ventas", updated_count)

    # Registrar métricas en DQM (compatibilidad)
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

    # Registrar métricas usando framework unificado
    log_record_count(process_id, "INSERTED_FACTS", "DWA_FACT_Ventas", inserted_count)

    # Registrar métricas en DQM (compatibilidad)
    log_dq_metric(
        conn, process_id, "DWA_FACT_Ventas", "registros_nuevos", inserted_count
    )
    logging.info(f"{inserted_count} nuevos hechos fueron insertados.")

    conn.commit()
    logging.info("--- Actualización de DWA_FACT_Ventas completada ---")

    # Retornar resultados para el framework de calidad
    return {
        "actualizados": updated_count,
        "nuevos": inserted_count,
        "total_procesados": updated_count + inserted_count,
    }


def main():
    """
    Orquesta la actualización del DWH con los datos de Ingesta2.
    Incluye controles de calidad completos para operaciones SCD2 e incrementales.
    """
    logging.info("Iniciando el Paso 9: Actualización del DWH con Ingesta2.")

    # Inicializar tracking de calidad con framework unificado
    execution_id = get_process_execution_id("STEP_09_UPDATE_DWH_INGESTA2")

    conn = None
    try:
        conn = sqlite3.connect(DB_PATH)
        log_quality_metric(
            execution_id,
            "DATABASE_CONNECTION",
            "DB_FILE",
            "PASS",
            "Conexión exitosa a la base de datos",
        )

        # Validaciones pre-proceso: Verificar que Ingesta2 esté disponible
        validate_ingesta2_availability(execution_id, conn)

        # Validaciones pre-proceso: Estado del DWH antes de actualización
        validate_dwh_state_before_update(execution_id, conn)

        # --- Actualizar Dimensiones SCD2 ---
        logging.info("--- Iniciando Actualización de Dimensiones SCD2 ---")
        try:
            scd2_results = update_scd2_clientes(conn, execution_id)
            log_quality_metric(
                execution_id,
                "SCD2_UPDATE",
                "DWA_DIM_Clientes",
                "PASS",
                f"Clientes modificados: {scd2_results['modificados']}, Nuevos: {scd2_results['nuevos']}",
            )
        except Exception as e:
            log_quality_metric(
                execution_id,
                "SCD2_UPDATE",
                "DWA_DIM_Clientes",
                "FAIL",
                f"Error en SCD2: {str(e)}",
            )
            raise

        # --- Actualizar Tabla de Hechos ---
        logging.info("--- Iniciando Actualización de Tabla de Hechos ---")
        try:
            fact_results = update_fact_ventas(conn, execution_id)
            log_quality_metric(
                execution_id,
                "FACT_UPDATE",
                "DWA_FACT_Ventas",
                "PASS",
                f"Hechos actualizados: {fact_results['actualizados']}, Nuevos: {fact_results['nuevos']}",
            )
        except Exception as e:
            log_quality_metric(
                execution_id,
                "FACT_UPDATE",
                "DWA_FACT_Ventas",
                "FAIL",
                f"Error en tabla de hechos: {str(e)}",
            )
            raise

        # 5. Validaciones post-actualización
        validate_dwh_integrity_after_update(execution_id, conn)
        validate_temporal_consistency(execution_id, conn)

        # Finalizar la ejecución del proceso
        update_process_execution(
            execution_id, "Exitoso", "Actualización desde Ingesta2 completada."
        )
        logging.info("Paso 9 completado exitosamente.")

    except sqlite3.Error as e:
        logging.error(f"Error de base de datos en el Paso 9: {e}")
        log_quality_metric(
            execution_id, "DATABASE_ERROR", "PROCESS", "FAIL", f"Error SQL: {str(e)}"
        )
        update_process_execution(
            execution_id, "Fallido", f"Error de base de datos: {str(e)}"
        )
    except Exception as e:
        logging.error(f"Error general en el Paso 9: {e}")
        log_quality_metric(
            execution_id, "PROCESS_ERROR", "PROCESS", "FAIL", f"Error: {str(e)}"
        )
        update_process_execution(execution_id, "Fallido", f"Error: {str(e)}")
    finally:
        if conn:
            conn.close()
            logging.info("Conexión a la base de datos cerrada.")


def validate_ingesta2_availability(execution_id: int, conn: sqlite3.Connection):
    """
    Valida que los datos de Ingesta2 estén disponibles y sean válidos.
    """
    cursor = conn.cursor()

    # Verificar que las tablas TMP2_ existan y tengan datos
    ingesta2_tables = ["TMP2_customers", "TMP2_orders", "TMP2_order_details"]

    for table in ingesta2_tables:
        try:
            cursor.execute(f"SELECT COUNT(*) FROM {table}")
            count = cursor.fetchone()[0]

            if count == 0:
                log_quality_metric(
                    execution_id,
                    "INGESTA2_EMPTY",
                    table,
                    "WARNING",
                    f"Tabla {table} está vacía",
                )
            else:
                log_quality_metric(
                    execution_id,
                    "INGESTA2_AVAILABLE",
                    table,
                    str(count),
                    f"Tabla {table} disponible con {count} registros",
                )
        except sqlite3.Error as e:
            log_quality_metric(
                execution_id,
                "INGESTA2_ERROR",
                table,
                "FAIL",
                f"Error accediendo a {table}: {str(e)}",
            )

    # Validar fechas de Ingesta2
    try:
        cursor.execute(
            """
            SELECT MIN(order_date), MAX(order_date), COUNT(*) 
            FROM TMP2_orders 
            WHERE order_date IS NOT NULL
        """
        )
        result = cursor.fetchone()
        if result and result[2] > 0:
            min_date, max_date, count = result
            log_quality_metric(
                execution_id,
                "INGESTA2_DATE_RANGE",
                "TMP2_orders",
                "INFO",
                f"Rango de fechas: {min_date} a {max_date}, {count} órdenes",
            )
        else:
            log_quality_metric(
                execution_id,
                "INGESTA2_DATES",
                "TMP2_orders",
                "WARNING",
                "No hay fechas válidas en TMP2_orders",
            )
    except sqlite3.Error as e:
        log_quality_metric(
            execution_id,
            "INGESTA2_DATE_ERROR",
            "TMP2_orders",
            "ERROR",
            f"Error validando fechas: {str(e)}",
        )


def validate_dwh_state_before_update(execution_id: int, conn: sqlite3.Connection):
    """Valida el estado del DWH antes de la actualización."""
    # Contar registros antes de la actualización
    tables_to_check = {
        "DWA_DIM_Clientes": "sk_cliente",
        "DWA_FACT_Ventas": "nk_orden_id",
    }

    for table, pkey in tables_to_check.items():
        try:
            # Usar la función de validación genérica para registrar la métrica
            validate_table_count(execution_id, table, 0, conn)
        except sqlite3.Error as e:
            log_quality_metric(
                execution_id,
                "PRE_UPDATE_ERROR",
                table,
                "ERROR",
                f"No se pudo verificar la tabla antes de la actualización: {str(e)}",
            )


def validate_dwh_integrity_after_update(execution_id: int, conn: sqlite3.Connection):
    """
    Valida la integridad del DWH después de la actualización.
    """
    cursor = conn.cursor()

    # Validar integridad referencial de la tabla de hechos actualizada
    validate_referential_integrity(
        execution_id,
        "DWA_FACT_Ventas",
        "DWA_DIM_Clientes",
        "sk_cliente",
        "sk_cliente",
        conn,
    )

    # Validar que no haya FK nulas en campos críticos
    validate_no_nulls(execution_id, "DWA_FACT_Ventas", "sk_cliente", conn)
    validate_no_nulls(execution_id, "DWA_FACT_Ventas", "sk_tiempo", conn)

    # Contar registros después de la actualización
    try:
        cursor.execute("SELECT COUNT(*) FROM DWA_FACT_Ventas")
        fact_count = cursor.fetchone()[0]
        log_quality_metric(
            execution_id,
            "POST_UPDATE_FACT_COUNT",
            "DWA_FACT_Ventas",
            str(fact_count),
            f"Total registros en tabla de hechos después de actualización: {fact_count}",
        )

        cursor.execute("SELECT COUNT(*) FROM DWA_DIM_Clientes")
        dim_count = cursor.fetchone()[0]
        log_quality_metric(
            execution_id,
            "POST_UPDATE_DIM_COUNT",
            "DWA_DIM_Clientes",
            str(dim_count),
            f"Total registros en dimensión después de actualización: {dim_count}",
        )

    except sqlite3.Error as e:
        log_quality_metric(
            execution_id,
            "POST_UPDATE_COUNT_ERROR",
            "DWA",
            "ERROR",
            f"Error contando registros post-actualización: {str(e)}",
        )


def validate_temporal_consistency(execution_id: int, conn: sqlite3.Connection):
    """
    Valida la consistencia temporal en dimensiones SCD2.
    """
    cursor = conn.cursor()

    # Verificar que no haya solapamientos de fechas en SCD2
    try:
        cursor.execute(
            """
            SELECT nk_cliente_id, COUNT(*) as overlaps
            FROM DWA_DIM_Clientes 
            WHERE es_vigente = 1
            GROUP BY nk_cliente_id
            HAVING COUNT(*) > 1
        """
        )
        overlaps = cursor.fetchall()

        if overlaps:
            overlap_count = len(overlaps)
            log_quality_metric(
                execution_id,
                "SCD2_TEMPORAL_OVERLAPS",
                "DWA_DIM_Clientes",
                "FAIL",
                f"Se encontraron {overlap_count} clientes con múltiples registros vigentes",
            )
        else:
            log_quality_metric(
                execution_id,
                "SCD2_TEMPORAL_OVERLAPS",
                "DWA_DIM_Clientes",
                "PASS",
                "No hay solapamientos temporales en SCD2",
            )
    except sqlite3.Error as e:
        log_quality_metric(
            execution_id,
            "SCD2_TEMPORAL_ERROR",
            "DWA_DIM_Clientes",
            "ERROR",
            f"Error validando consistencia temporal: {str(e)}",
        )

    # Verificar fechas de inicio <= fechas de fin
    try:
        cursor.execute(
            """
            SELECT COUNT(*) FROM DWA_DIM_Clientes 
            WHERE fecha_fin_validez IS NOT NULL AND fecha_inicio_validez > fecha_fin_validez
        """
        )
        invalid_dates = cursor.fetchone()[0]

        if invalid_dates > 0:
            log_quality_metric(
                execution_id,
                "SCD2_DATE_LOGIC",
                "DWA_DIM_Clientes",
                "FAIL",
                f"{invalid_dates} registros con fechas de inicio > fin",
            )
        else:
            log_quality_metric(
                execution_id,
                "SCD2_DATE_LOGIC",
                "DWA_DIM_Clientes",
                "PASS",
                "Lógica de fechas SCD2 es consistente",
            )
    except sqlite3.Error as e:
        log_quality_metric(
            execution_id,
            "SCD2_DATE_LOGIC_ERROR",
            "DWA_DIM_Clientes",
            "ERROR",
            f"Error validando lógica de fechas: {str(e)}",
        )


if __name__ == "__main__":
    main()
