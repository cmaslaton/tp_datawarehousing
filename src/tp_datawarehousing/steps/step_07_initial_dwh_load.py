import sqlite3
import logging
from datetime import datetime
from tp_datawarehousing.utils.quality_utils import (
    get_process_execution_id,
    update_process_execution,
    log_quality_metric,
    validate_table_count,
    validate_no_nulls,
    validate_referential_integrity,
    validate_data_range,
    log_record_count,
)

# --- Configuración de Logging ---
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)

# --- Constantes ---
DB_PATH = "db/tp_dwa.db"
USER = "data_engineer"


# --- Funciones de Compatibilidad (mantenidas para no romper código existente) ---
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
    """Registra el resultado de un control de calidad en el DQM utilizando el framework unificado."""
    log_quality_metric(process_id, check_name, table_name, status, details)


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

    # Obtener rango de fechas primero
    try:
        cursor.execute("SELECT MIN(DATE(order_date)), MAX(DATE(order_date)), COUNT(*) FROM ING_orders WHERE order_date IS NOT NULL;")
        result = cursor.fetchone()
        min_date, max_date, order_count = result
        
        if not min_date or not max_date:
            logging.warning("No hay fechas válidas en ING_orders. Cargando dimensión tiempo mínima.")
            # Cargar solo el año actual como fallback
            from datetime import datetime
            current_year = datetime.now().year
            cursor.execute(f"""
                INSERT INTO DWA_DIM_Tiempo (sk_tiempo, fecha, anio, mes, dia, trimestre, nombre_mes, nombre_dia, es_fin_de_semana)
                VALUES ({current_year}0101, '{current_year}-01-01', {current_year}, 1, 1, 1, 'Enero', 'Lunes', 0);
            """)
            conn.commit()
            logging.info("Carga de DWA_DIM_Tiempo completada con datos mínimos. 1 registro insertado.")
            return 1
        
        logging.info(f"Rango de fechas detectado: {min_date} a {max_date} ({order_count} órdenes)")
        
        # Calcular días entre fechas para validar el rango
        cursor.execute("SELECT julianday(?) - julianday(?);", (max_date, min_date))
        days_diff = cursor.fetchone()[0]
        
        if days_diff > 10000:  # Más de ~27 años
            logging.error(f"Rango de fechas demasiado amplio: {days_diff} días. Limitando a 10 años desde fecha mínima.")
            cursor.execute("SELECT DATE(?, '+10 years');", (min_date,))
            max_date = cursor.fetchone()[0]
            days_diff = 10 * 365
        
        logging.info(f"Generando dimensión tiempo para {int(days_diff)} días...")
        
        # CTE optimizada con límite
        cursor.execute(
            """
            INSERT INTO DWA_DIM_Tiempo (sk_tiempo, fecha, anio, mes, dia, trimestre, nombre_mes, nombre_dia, es_fin_de_semana)
            WITH RECURSIVE dates(date_val, counter) AS (
                SELECT DATE(?), 0
                UNION ALL
                SELECT DATE(date_val, '+1 day'), counter + 1
                FROM dates
                WHERE date_val < DATE(?) AND counter < 15000
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
            """, (min_date, max_date)
        )
        
        count = cursor.rowcount
        conn.commit()
        logging.info(f"Carga de DWA_DIM_Tiempo completada. {count} registros insertados.")
        
        # Validar la carga
        cursor.execute("SELECT COUNT(*) FROM DWA_DIM_Tiempo;")
        final_count = cursor.fetchone()[0]
        
        if final_count != count:
            logging.warning(f"Discrepancia en conteo: insertados={count}, tabla contiene={final_count}")
        
        return final_count
        
    except Exception as e:
        logging.error(f"Error en load_dim_tiempo: {e}")
        conn.rollback()
        raise


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
    Utiliza el framework unificado de calidad.
    """
    logging.info("Iniciando el Paso 7: Carga Inicial del DWH.")

    # Inicializar tracking de calidad con framework unificado
    execution_id = get_process_execution_id("STEP_07_INITIAL_DWH_LOAD")

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

        # 1. Ejecutar Controles de Calidad de Ingesta (Punto 8a)
        logging.info("--- Iniciando Controles de Calidad de Ingesta ---")
        ingestion_status = perform_ingestion_quality_checks(conn, execution_id)

        log_quality_metric(
            execution_id,
            "INGESTION_QUALITY_STATUS",
            "VALIDATION",
            ingestion_status,
            f"Resultado de validaciones de ingesta: {ingestion_status}",
        )

        # Si los controles de ingesta fallan, detenemos el proceso.
        if ingestion_status == "FALLIDO":
            update_process_execution(
                execution_id,
                "Fallido",
                "Los controles de calidad de ingesta han fallado",
            )
            raise Exception(
                "Los controles de calidad de ingesta han fallado. Abortando la carga del DWH."
            )

        # 2. Cargar el DWH (Dimensiones y Hechos)
        logging.info("--- Iniciando Carga de Dimensiones ---")
        dimensions_loaded = 0

        try:
            load_dim_shippers(conn)
            dimensions_loaded += 1
            load_dim_tiempo(conn)
            dimensions_loaded += 1
            load_dim_productos(conn)
            dimensions_loaded += 1
            load_dim_empleados(conn)
            dimensions_loaded += 1
            load_dim_clientes(conn)
            dimensions_loaded += 1
            load_dim_geografia(conn)
            dimensions_loaded += 1

            log_quality_metric(
                execution_id,
                "DIMENSIONS_LOAD",
                "DWH_PROCESS",
                "PASS",
                f"Se cargaron {dimensions_loaded} dimensiones exitosamente",
            )
            logging.info("--- Carga de Dimensiones Finalizada ---")

        except Exception as e:
            log_quality_metric(
                execution_id,
                "DIMENSIONS_LOAD",
                "DWH_PROCESS",
                "FAIL",
                f"Error cargando dimensiones: {str(e)}",
            )
            raise

        # Cargar Tabla de Hechos (Ventas)
        logging.info("--- Iniciando Carga de la Tabla de Hechos ---")
        try:
            ventas_count = load_fact_ventas(conn)
            log_record_count(execution_id, "LOADED", "DWA_FACT_Ventas", ventas_count)
            log_quality_metric(
                execution_id,
                "FACT_TABLE_LOAD",
                "DWA_FACT_Ventas",
                "PASS",
                f"Cargados {ventas_count} registros en tabla de hechos",
            )
            logging.info("--- Carga de la Tabla de Hechos Finalizada ---")
        except Exception as e:
            log_quality_metric(
                execution_id,
                "FACT_TABLE_LOAD",
                "DWA_FACT_Ventas",
                "FAIL",
                f"Error cargando tabla de hechos: {str(e)}",
            )
            raise

        # 3. Ejecutar Controles de Calidad de Integración (Punto 8b)
        logging.info("--- Iniciando Controles de Calidad de Integración ---")
        integration_status = perform_integration_quality_checks(conn, execution_id)

        log_quality_metric(
            execution_id,
            "INTEGRATION_QUALITY_STATUS",
            "VALIDATION",
            integration_status,
            f"Resultado de validaciones de integración: {integration_status}",
        )

        # 4. Determinar estado final y registrar métricas finales
        final_status = "Exitoso"
        if "ADVERTENCIA" in (ingestion_status, integration_status):
            final_status = "Exitoso con Advertencias"
        if "FALLIDO" in (ingestion_status, integration_status):
            final_status = "FALLIDO"

        # Validaciones adicionales del DWH completo
        validate_dwh_completeness(execution_id, conn)

        # Finalizar proceso
        comments = (
            f"DWH cargado: {dimensions_loaded} dimensiones, {ventas_count} hechos"
        )
        if final_status == "FALLIDO":
            update_process_execution(execution_id, "Fallido", comments)
        elif final_status == "Exitoso con Advertencias":
            update_process_execution(
                execution_id, "Exitoso", f"{comments} (con advertencias)"
            )
        else:
            update_process_execution(execution_id, "Exitoso", comments)

        logging.info(f"Proceso completado con estado: {final_status}")

    except Exception as e:
        logging.error(f"Error en la base de datos durante la carga del DWH: {e}")
        log_quality_metric(
            execution_id,
            "PROCESS_ERROR",
            "DWH_LOAD",
            "FAIL",
            f"Error crítico: {str(e)}",
        )
        update_process_execution(execution_id, "Fallido", f"Error crítico: {str(e)}")
    finally:
        if conn:
            conn.close()
            logging.info("Conexión a la base de datos cerrada.")


def validate_dwh_completeness(execution_id: int, conn: sqlite3.Connection):
    """
    Valida que el DWH esté completo después de la carga inicial.
    """
    cursor = conn.cursor()
    fact_count = 0  # Inicializar la variable

    # Verificar que todas las dimensiones tengan datos
    dimensions = [
        "DWA_DIM_Shippers",
        "DWA_DIM_Tiempo",
        "DWA_DIM_Productos",
        "DWA_DIM_Empleados",
        "DWA_DIM_Clientes",
        "DWA_DIM_Geografia",
    ]

    empty_dimensions = []
    total_dim_records = 0

    for dim in dimensions:
        try:
            cursor.execute(f"SELECT COUNT(*) FROM {dim}")
            count = cursor.fetchone()[0]
            total_dim_records += count

            if count == 0:
                empty_dimensions.append(dim)
            else:
                log_quality_metric(
                    execution_id,
                    "DIMENSION_COUNT",
                    dim,
                    str(count),
                    f"Dimensión {dim} cargada con {count} registros",
                )
        except sqlite3.Error as e:
            log_quality_metric(
                execution_id,
                "DIMENSION_ERROR",
                dim,
                "ERROR",
                f"Error verificando dimensión: {str(e)}",
            )

    if empty_dimensions:
        log_quality_metric(
            execution_id,
            "EMPTY_DIMENSIONS",
            "DWH_VALIDATION",
            "WARNING",
            f"Dimensiones vacías: {', '.join(empty_dimensions)}",
        )
    else:
        log_quality_metric(
            execution_id,
            "EMPTY_DIMENSIONS",
            "DWH_VALIDATION",
            "PASS",
            "Todas las dimensiones contienen datos",
        )

    # Verificar tabla de hechos
    try:
        cursor.execute("SELECT COUNT(*) FROM DWA_FACT_Ventas")
        fact_count = cursor.fetchone()[0]

        if fact_count == 0:
            log_quality_metric(
                execution_id,
                "FACT_TABLE_POPULATED",
                "DWA_FACT_Ventas",
                "FAIL",
                "La tabla de hechos está vacía",
            )
        else:
            log_quality_metric(
                execution_id,
                "FACT_TABLE_POPULATED",
                "DWA_FACT_Ventas",
                "PASS",
                f"Tabla de hechos cargada con {fact_count} registros",
            )
    except sqlite3.Error as e:
        log_quality_metric(
            execution_id,
            "FACT_TABLE_ERROR",
            "DWA_FACT_Ventas",
            "ERROR",
            f"Error verificando tabla de hechos: {str(e)}",
        )

    # Resumen final
    log_quality_metric(
        execution_id,
        "DWH_LOAD_SUMMARY",
        "DWH_VALIDATION",
        f"DIM:{total_dim_records}, FACT:{fact_count}",
        f"DWH cargado: {total_dim_records} registros en dimensiones, {fact_count} en hechos",
    )

    # logging.info("--- Paso 7: Finalizado ---")


if __name__ == "__main__":
    main()
