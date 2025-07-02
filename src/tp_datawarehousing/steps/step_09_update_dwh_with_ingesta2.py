import sqlite3
import logging
from datetime import datetime, timedelta
from tp_datawarehousing.utils.quality_utils import (
    get_process_execution_id,
    update_process_execution,
    log_quality_metric,
    validate_table_count,
    validate_no_nulls,
    validate_referential_integrity,
    log_record_count,
    execute_transaction_with_retry,
    get_db_connection,
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
    # NO hacer commit aquí - será manejado por la transacción padre


# --- Lógica de Actualización Optimizada (SCD Tipo 2 para Clientes) ---
def update_scd2_clientes_transaction(conn, process_id):
    """
    Actualiza la dimensión de clientes usando SCD Tipo 2 en una sola transacción.
    Esta función NO hace commit - debe ser llamada desde execute_transaction_with_retry.
    """
    logging.info("--- Iniciando actualización de DWA_DIM_Clientes (SCD Tipo 2) ---")
    cursor = conn.cursor()

    # 1. Identificar clientes con cambios en una sola query optimizada
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
            COALESCE(d.direccion, '') != COALESCE(t.address, '') OR
            COALESCE(d.ciudad, '') != COALESCE(t.city, '') OR
            COALESCE(d.region, '') != COALESCE(t.region, '') OR
            COALESCE(d.codigo_postal, '') != COALESCE(t.postal_code, '') OR
            COALESCE(d.pais, '') != COALESCE(t.country, '') OR
            COALESCE(d.nombre_contacto, '') != COALESCE(t.contact_name, '') OR
            COALESCE(d.titulo_contacto, '') != COALESCE(t.contact_title, '')
        );
    """
    cursor.execute(query_changed_customers)
    changed_customers = cursor.fetchall()

    today = datetime.now().date()
    yesterday = today - timedelta(days=1)

    # 2. Actualizar todos los registros viejos en una sola operación
    if changed_customers:
        expired_customer_ids = [str(customer[0]) for customer in changed_customers]
        placeholders = ",".join(["?" for _ in expired_customer_ids])

        cursor.execute(
            f"""
            UPDATE DWA_DIM_Clientes
            SET fecha_fin_validez = ?, es_vigente = 0
            WHERE sk_cliente IN ({placeholders})
        """,
            [yesterday.isoformat()] + expired_customer_ids,
        )

        logging.info(f"Expirados {len(changed_customers)} registros de clientes")

        # 3. Insertar todos los registros nuevos (actualizados) en lotes
        new_customer_data = []
        for customer_data in changed_customers:
            new_customer_data.append(
                (
                    customer_data[1],  # nk_cliente_id
                    customer_data[2],  # company_name
                    customer_data[3],  # contact_name
                    customer_data[4],  # contact_title
                    customer_data[5],  # address
                    customer_data[6],  # city
                    customer_data[7],  # region
                    customer_data[8],  # postal_code
                    customer_data[9],  # country
                    today.isoformat(),
                    None,
                    1,
                )
            )

        cursor.executemany(
            """
            INSERT INTO DWA_DIM_Clientes (
                nk_cliente_id, nombre_compania, nombre_contacto, titulo_contacto,
                direccion, ciudad, region, codigo_postal, pais,
                fecha_inicio_validez, fecha_fin_validez, es_vigente
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
            new_customer_data,
        )

        logging.info(
            f"Insertados {len(changed_customers)} registros actualizados de clientes"
        )

    # 4. Identificar e insertar clientes completamente nuevos en lotes
    query_new_customers = """
        SELECT customer_id, company_name, contact_name, contact_title,
               address, city, region, postal_code, country
        FROM TMP2_customers t
        WHERE NOT EXISTS (
            SELECT 1 FROM DWA_DIM_Clientes d WHERE d.nk_cliente_id = t.customer_id
        );
    """
    cursor.execute(query_new_customers)
    new_customers = cursor.fetchall()

    if new_customers:
        new_customer_data = []
        for new_customer in new_customers:
            new_customer_data.append(
                (
                    new_customer[0],  # customer_id
                    new_customer[1],  # company_name
                    new_customer[2],  # contact_name
                    new_customer[3],  # contact_title
                    new_customer[4],  # address
                    new_customer[5],  # city
                    new_customer[6],  # region
                    new_customer[7],  # postal_code
                    new_customer[8],  # country
                    today.isoformat(),
                    None,
                    1,
                )
            )

        cursor.executemany(
            """
            INSERT INTO DWA_DIM_Clientes (
                nk_cliente_id, nombre_compania, nombre_contacto, titulo_contacto,
                direccion, ciudad, region, codigo_postal, pais,
                fecha_inicio_validez, fecha_fin_validez, es_vigente
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
            new_customer_data,
        )

        logging.info(f"Insertados {len(new_customers)} clientes completamente nuevos")

    # Registrar métricas en DQM (compatibilidad) - sin commit
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


def update_fact_ventas_transaction(conn, process_id):
    """
    Actualiza la tabla de hechos con los datos de Ingesta2 en una sola transacción optimizada.
    Esta función NO hace commit - debe ser llamada desde execute_transaction_with_retry.
    """
    logging.info("--- Iniciando actualización de DWA_FACT_Ventas ---")
    cursor = conn.cursor()

    # 1. Actualizar hechos existentes en una sola operación
    # Nota: SQLite no soporta FROM en UPDATE directamente, usamos subqueries
    update_query = """
        UPDATE DWA_FACT_Ventas 
        SET 
            cantidad = (
                SELECT od.quantity 
                FROM TMP2_order_details od 
                JOIN DWA_DIM_Productos dp ON od.product_id = dp.nk_producto_id 
                WHERE od.order_id = DWA_FACT_Ventas.nk_orden_id 
                AND dp.sk_producto = DWA_FACT_Ventas.sk_producto
            ),
            precio_unitario = (
                SELECT od.unit_price 
                FROM TMP2_order_details od 
                JOIN DWA_DIM_Productos dp ON od.product_id = dp.nk_producto_id 
                WHERE od.order_id = DWA_FACT_Ventas.nk_orden_id 
                AND dp.sk_producto = DWA_FACT_Ventas.sk_producto
            ),
            descuento = (
                SELECT od.discount 
                FROM TMP2_order_details od 
                JOIN DWA_DIM_Productos dp ON od.product_id = dp.nk_producto_id 
                WHERE od.order_id = DWA_FACT_Ventas.nk_orden_id 
                AND dp.sk_producto = DWA_FACT_Ventas.sk_producto
            ),
            monto_total = (
                SELECT od.unit_price * od.quantity * (1 - od.discount) 
                FROM TMP2_order_details od 
                JOIN DWA_DIM_Productos dp ON od.product_id = dp.nk_producto_id 
                WHERE od.order_id = DWA_FACT_Ventas.nk_orden_id 
                AND dp.sk_producto = DWA_FACT_Ventas.sk_producto
            )
        WHERE EXISTS (
            SELECT 1 FROM TMP2_order_details od 
            JOIN DWA_DIM_Productos dp ON od.product_id = dp.nk_producto_id 
            WHERE od.order_id = DWA_FACT_Ventas.nk_orden_id 
            AND dp.sk_producto = DWA_FACT_Ventas.sk_producto
        )
    """

    cursor.execute(update_query)
    updated_facts = cursor.rowcount

    logging.info(f"{updated_facts} hechos existentes fueron actualizados.")

    # 2. Insertar nuevos hechos en lotes usando la estructura correcta
    insert_query = """
        INSERT INTO DWA_FACT_Ventas (
            sk_cliente, sk_producto, sk_empleado, sk_tiempo, sk_geografia_envio, sk_shipper,
            precio_unitario, cantidad, descuento, flete, monto_total, nk_orden_id
        )
        SELECT DISTINCT
            dc.sk_cliente,
            dp.sk_producto,
            de.sk_empleado,
            dt.sk_tiempo,
            dg.sk_geografia,
            ds.sk_shipper,
            od.unit_price,
            od.quantity,
            od.discount,
            o.freight,
            (od.unit_price * od.quantity * (1 - od.discount)),
            o.order_id
        FROM TMP2_order_details od
        JOIN TMP2_orders o ON od.order_id = o.order_id
        LEFT JOIN DWA_DIM_Clientes dc ON o.customer_id = dc.nk_cliente_id AND dc.es_vigente = 1
        LEFT JOIN DWA_DIM_Productos dp ON od.product_id = dp.nk_producto_id
        LEFT JOIN DWA_DIM_Empleados de ON o.employee_id = de.nk_empleado_id
        LEFT JOIN DWA_DIM_Tiempo dt ON DATE(o.order_date) = dt.fecha
        LEFT JOIN DWA_DIM_Geografia dg ON o.ship_address = dg.direccion AND o.ship_city = dg.ciudad AND o.ship_country = dg.pais
        LEFT JOIN DWA_DIM_Shippers ds ON o.ship_via = ds.nk_shipper_id
        WHERE NOT EXISTS (
            SELECT 1 FROM DWA_FACT_Ventas f 
            WHERE f.nk_orden_id = o.order_id 
            AND f.sk_producto = dp.sk_producto
        )
        AND dc.sk_cliente IS NOT NULL  -- Solo insertar si tenemos cliente válido
        AND dp.sk_producto IS NOT NULL  -- Solo insertar si tenemos producto válido
        AND dt.sk_tiempo IS NOT NULL   -- Solo insertar si tenemos fecha válida
    """

    cursor.execute(insert_query)
    inserted_facts = cursor.rowcount

    logging.info(f"{inserted_facts} nuevos hechos fueron insertados.")

    # Registrar métricas en DQM (compatibilidad) - sin commit
    log_dq_metric(
        conn, process_id, "DWA_FACT_Ventas", "hechos_actualizados", updated_facts
    )
    log_dq_metric(conn, process_id, "DWA_FACT_Ventas", "hechos_nuevos", inserted_facts)

    logging.info("--- Actualización de DWA_FACT_Ventas completada ---")

    return {
        "actualizados": updated_facts,
        "nuevos": inserted_facts,
        "total_procesados": updated_facts + inserted_facts,
    }


def main():
    """
    Orquesta la actualización del DWH con los datos de Ingesta2.
    Incluye controles de calidad completos para operaciones SCD2 e incrementales.
    """
    logging.info("Iniciando el Paso 9: Actualización del DWH con Ingesta2.")

    # Inicializar tracking de calidad con framework unificado
    execution_id = get_process_execution_id("STEP_09_UPDATE_DWH_INGESTA2")

    try:
        # Crear conexión temporal para validaciones pre-proceso
        conn = get_db_connection()
        if conn is None:
            raise sqlite3.Error("No se pudo obtener conexión a la base de datos")

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

        # Cerrar conexión temporal antes de las transacciones
        conn.close()
        conn = None

        # --- Actualizar Dimensiones SCD2 ---
        logging.info("--- Iniciando Actualización de Dimensiones SCD2 ---")
        try:
            scd2_results = execute_transaction_with_retry(
                update_scd2_clientes_transaction, execution_id
            )
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
            fact_results = execute_transaction_with_retry(
                update_fact_ventas_transaction, execution_id
            )
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
        # Crear nueva conexión temporal para validaciones finales
        conn = get_db_connection()
        if conn:
            validate_dwh_integrity_after_update(execution_id, conn)
            validate_temporal_consistency(execution_id, conn)
            conn.close()

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
        # Solo cerrar si conn no es None y aún está abierta
        if "conn" in locals() and conn:
            try:
                conn.close()
                logging.info("Conexión de validación cerrada.")
            except:
                pass


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
