"""
Utilidades para control de calidad de datos en el pipeline DWA.
Funciones auxiliares para registrar métricas y validaciones en el DQM.
"""

import sqlite3
import logging
import time
import random
from datetime import datetime
from typing import Optional, Any

# --- Configuración ---
DB_PATH = "db/tp_dwa.db"
MAX_RETRIES = 5  # Aumentar reintentos
RETRY_DELAY = 0.1  # Reducir delay inicial

# --- Configuración mejorada ---
MAX_CONNECTION_TIMEOUT = 30.0  # Timeout más largo para operaciones complejas
BATCH_SIZE = 100  # Para operaciones por lotes


def execute_with_retry(operation_func, *args, **kwargs):
    """
    Ejecuta una operación de base de datos con reintentos automáticos mejorados.

    Args:
        operation_func: Función a ejecutar
        *args, **kwargs: Argumentos para la función

    Returns:
        Resultado de la función o None si falla
    """
    for attempt in range(MAX_RETRIES):
        try:
            return operation_func(*args, **kwargs)
        except sqlite3.OperationalError as e:
            error_msg = str(e).lower()
            if (
                "database is locked" in error_msg
                or "disk i/o error" in error_msg
                or "database is busy" in error_msg
            ) and attempt < MAX_RETRIES - 1:
                # Esperar con backoff exponencial y jitter más efectivo
                base_delay = RETRY_DELAY * (2**attempt)
                jitter = random.uniform(0, base_delay * 0.5)  # Jitter más significativo
                delay = base_delay + jitter
                logging.warning(
                    f"Error de BD ({e}), reintentando en {delay:.2f}s (intento {attempt + 1}/{MAX_RETRIES})"
                )
                time.sleep(delay)
                continue
            else:
                # Si es el último intento o error no manejable, loggear y continuar
                logging.error(
                    f"Error persistente de BD después de {MAX_RETRIES} intentos: {e}"
                )
                return None
        except Exception as e:
            logging.error(f"Error no relacionado con BD: {e}")
            return None

    return None


def get_db_connection(timeout=None):
    """
    Obtiene una conexión a la base de datos con configuración optimizada para evitar bloqueos.

    Args:
        timeout: Timeout personalizado para la conexión

    Returns:
        Conexión SQLite configurada
    """
    try:
        connection_timeout = timeout or MAX_CONNECTION_TIMEOUT
        conn = sqlite3.connect(DB_PATH, timeout=connection_timeout)

        # Configuración optimizada para operaciones complejas
        conn.execute("PRAGMA busy_timeout=30000")  # 30 segundos de timeout
        conn.execute("PRAGMA synchronous=NORMAL")  # Balance entre velocidad y seguridad
        conn.execute(
            "PRAGMA journal_mode=WAL"
        )  # Write-Ahead Logging para mejor concurrencia
        conn.execute("PRAGMA temp_store=MEMORY")  # Usar memoria para temp tables
        conn.execute("PRAGMA cache_size=10000")  # Cache más grande
        conn.execute("PRAGMA locking_mode=NORMAL")  # Asegurar unlocking apropiado

        return conn
    except Exception as e:
        logging.error(f"Error creando conexión a BD: {e}")
        return None


def get_process_execution_id(proceso_nombre: str) -> int:
    """
    Obtiene o crea un ID de ejecución para el proceso actual.

    Args:
        proceso_nombre: Nombre del proceso en ejecución

    Returns:
        ID de ejecución para usar en las métricas de calidad
    """

    def _create_execution():
        conn = get_db_connection()
        try:
            cursor = conn.cursor()

            # Insertar nueva ejecución
            cursor.execute(
                """
                INSERT INTO DQM_ejecucion_procesos 
                (nombre_proceso, fecha_inicio, estado) 
                VALUES (?, ?, ?)
            """,
                (
                    proceso_nombre,
                    datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    "En Progreso",
                ),
            )

            execution_id = cursor.lastrowid
            conn.commit()
            return execution_id
        finally:
            conn.close()

    try:
        return execute_with_retry(_create_execution)
    except sqlite3.Error as e:
        logging.error(f"Error obteniendo ID de ejecución: {e}")
        return None


def update_process_execution(
    execution_id: int, estado: str, comentarios: Optional[str] = None
):
    """
    Actualiza el estado final de una ejecución de proceso.

    Args:
        execution_id: ID de la ejecución
        estado: Estado final ('Exitoso', 'Fallido')
        comentarios: Comentarios adicionales
    """

    def _update_execution():
        conn = get_db_connection()
        try:
            cursor = conn.cursor()

            fecha_fin = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

            # Calcular duración si existe fecha_inicio
            cursor.execute(
                """
                SELECT fecha_inicio FROM DQM_ejecucion_procesos 
                WHERE id_ejecucion = ?
            """,
                (execution_id,),
            )

            result = cursor.fetchone()
            duracion_seg = None
            if result:
                fecha_inicio_str = result[0]
                fecha_inicio = datetime.strptime(fecha_inicio_str, "%Y-%m-%d %H:%M:%S")
                fecha_fin_dt = datetime.strptime(fecha_fin, "%Y-%m-%d %H:%M:%S")
                duracion_seg = (fecha_fin_dt - fecha_inicio).total_seconds()

            cursor.execute(
                """
                UPDATE DQM_ejecucion_procesos 
                SET fecha_fin = ?, estado = ?, comentarios = ?, duracion_seg = ?
                WHERE id_ejecucion = ?
            """,
                (fecha_fin, estado, comentarios, duracion_seg, execution_id),
            )

            conn.commit()
        finally:
            conn.close()

    try:
        execute_with_retry(_update_execution)
    except sqlite3.Error as e:
        logging.error(f"Error actualizando ejecución: {e}")


def log_quality_metric(
    execution_id: int,
    nombre_indicador: str,
    entidad_asociada: str,
    resultado: str,
    detalles: Optional[str] = None,
):
    """
    Registra una métrica de calidad en la tabla DQM_indicadores_calidad.

    Args:
        execution_id: ID de ejecución del proceso
        nombre_indicador: Nombre del indicador de calidad
        entidad_asociada: Tabla o entidad siendo validada
        resultado: Resultado de la validación ('PASS', 'FAIL', 'WARNING', valor numérico)
        detalles: Detalles adicionales sobre el resultado
    """

    def _log_metric():
        conn = get_db_connection()
        try:
            cursor = conn.cursor()

            cursor.execute(
                """
                INSERT INTO DQM_indicadores_calidad 
                (id_ejecucion, nombre_indicador, entidad_asociada, resultado, detalles)
                VALUES (?, ?, ?, ?, ?)
            """,
                (execution_id, nombre_indicador, entidad_asociada, resultado, detalles),
            )

            conn.commit()
        finally:
            conn.close()

    try:
        execute_with_retry(_log_metric)
        logging.info(
            f"Métrica de calidad registrada: {nombre_indicador} - {entidad_asociada}: {resultado}"
        )
    except sqlite3.Error as e:
        logging.error(f"Error registrando métrica de calidad: {e}")


def validate_table_count(
    execution_id: int,
    table_name: str,
    expected_min: int = 0,
    conn: sqlite3.Connection = None,
) -> bool:
    """
    Valida que una tabla tenga al menos un número mínimo de registros.

    Args:
        execution_id: ID de ejecución
        table_name: Nombre de la tabla a validar
        expected_min: Número mínimo esperado de registros
        conn: Conexión existente (opcional)

    Returns:
        True si la validación pasa, False si falla
    """

    def _validate_count():
        if conn is None:
            connection = get_db_connection()
            should_close = True
        else:
            connection = conn
            should_close = False

        try:
            cursor = connection.cursor()
            cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
            count = cursor.fetchone()[0]
            return count
        finally:
            if should_close:
                connection.close()

    try:
        count = execute_with_retry(_validate_count)

        result = "PASS" if count >= expected_min else "FAIL"
        detalles = f"Registros encontrados: {count}, mínimo esperado: {expected_min}"

        log_quality_metric(
            execution_id=execution_id,
            nombre_indicador="COUNT_VALIDATION",
            entidad_asociada=table_name,
            resultado=result,
            detalles=detalles,
        )

        return count >= expected_min

    except sqlite3.Error as e:
        logging.error(f"Error validando conteo de tabla {table_name}: {e}")
        log_quality_metric(
            execution_id=execution_id,
            nombre_indicador="COUNT_VALIDATION",
            entidad_asociada=table_name,
            resultado="ERROR",
            detalles=f"Error SQL: {str(e)}",
        )
        return False


def validate_no_nulls(
    execution_id: int,
    table_name: str,
    column_name: str,
    conn: sqlite3.Connection = None,
) -> bool:
    """
    Valida que una columna no tenga valores NULL.

    Args:
        execution_id: ID de ejecución
        table_name: Nombre de la tabla
        column_name: Nombre de la columna a validar
        conn: Conexión existente (opcional)

    Returns:
        True si no hay NULLs, False si hay NULLs
    """

    def _validate_nulls():
        if conn is None:
            connection = get_db_connection()
            should_close = True
        else:
            connection = conn
            should_close = False

        try:
            cursor = connection.cursor()
            cursor.execute(
                f"SELECT COUNT(*) FROM {table_name} WHERE {column_name} IS NULL"
            )
            null_count = cursor.fetchone()[0]
            return null_count
        finally:
            if should_close:
                connection.close()

    try:
        null_count = execute_with_retry(_validate_nulls)

        result = "PASS" if null_count == 0 else "FAIL"
        detalles = f"Valores NULL encontrados: {null_count}"

        log_quality_metric(
            execution_id=execution_id,
            nombre_indicador="NULL_VALIDATION",
            entidad_asociada=f"{table_name}.{column_name}",
            resultado=result,
            detalles=detalles,
        )

        return null_count == 0

    except sqlite3.Error as e:
        logging.error(f"Error validando NULLs en {table_name}.{column_name}: {e}")
        log_quality_metric(
            execution_id=execution_id,
            nombre_indicador="NULL_VALIDATION",
            entidad_asociada=f"{table_name}.{column_name}",
            resultado="ERROR",
            detalles=f"Error SQL: {str(e)}",
        )
        return False


def validate_referential_integrity(
    execution_id: int,
    child_table: str,
    parent_table: str,
    fk_column: str,
    pk_column: str = "id",
    conn: sqlite3.Connection = None,
) -> bool:
    """
    Valida integridad referencial entre dos tablas.

    Args:
        execution_id: ID de ejecución
        child_table: Tabla hija (con FK)
        parent_table: Tabla padre (con PK)
        fk_column: Columna de foreign key
        pk_column: Columna de primary key (default: 'id')
        conn: Conexión existente (opcional)

    Returns:
        True si la integridad es válida, False si hay violaciones
    """

    def _validate_integrity():
        if conn is None:
            connection = get_db_connection()
            should_close = True
        else:
            connection = conn
            should_close = False

        try:
            cursor = connection.cursor()

            query = f"""
            SELECT COUNT(*) 
            FROM {child_table} c
            LEFT JOIN {parent_table} p ON c.{fk_column} = p.{pk_column}
            WHERE c.{fk_column} IS NOT NULL AND p.{pk_column} IS NULL
            """

            cursor.execute(query)
            orphan_count = cursor.fetchone()[0]
            return orphan_count
        finally:
            if should_close:
                connection.close()

    try:
        orphan_count = execute_with_retry(_validate_integrity)

        result = "PASS" if orphan_count == 0 else "FAIL"
        detalles = f"Registros huérfanos encontrados: {orphan_count}"

        log_quality_metric(
            execution_id=execution_id,
            nombre_indicador="REFERENTIAL_INTEGRITY",
            entidad_asociada=f"{child_table}.{fk_column} -> {parent_table}.{pk_column}",
            resultado=result,
            detalles=detalles,
        )

        return orphan_count == 0

    except sqlite3.Error as e:
        logging.error(f"Error validando integridad referencial: {e}")
        log_quality_metric(
            execution_id=execution_id,
            nombre_indicador="REFERENTIAL_INTEGRITY",
            entidad_asociada=f"{child_table}.{fk_column} -> {parent_table}.{pk_column}",
            resultado="ERROR",
            detalles=f"Error SQL: {str(e)}",
        )
        return False


def validate_data_range(
    execution_id: int,
    table_name: str,
    column_name: str,
    min_value: Any = None,
    max_value: Any = None,
) -> bool:
    """
    Valida que los valores de una columna estén dentro de un rango específico.

    Args:
        execution_id: ID de ejecución
        table_name: Nombre de la tabla
        column_name: Nombre de la columna
        min_value: Valor mínimo permitido
        max_value: Valor máximo permitido

    Returns:
        True si todos los valores están en rango, False si hay valores fuera de rango
    """

    def _validate_range():
        conditions = []
        if min_value is not None:
            conditions.append(f"{column_name} < {min_value}")
        if max_value is not None:
            conditions.append(f"{column_name} > {max_value}")

        if not conditions:
            return 0  # No hay restricciones

        where_clause = " OR ".join(conditions)
        query = f"SELECT COUNT(*) FROM {table_name} WHERE {where_clause}"

        conn = get_db_connection()
        try:
            cursor = conn.cursor()
            cursor.execute(query)
            out_of_range_count = cursor.fetchone()[0]
            return out_of_range_count
        finally:
            conn.close()

    try:
        out_of_range_count = execute_with_retry(_validate_range)

        result = "PASS" if out_of_range_count == 0 else "FAIL"
        detalles = f"Valores fuera de rango: {out_of_range_count} (min: {min_value}, max: {max_value})"

        log_quality_metric(
            execution_id=execution_id,
            nombre_indicador="RANGE_VALIDATION",
            entidad_asociada=f"{table_name}.{column_name}",
            resultado=result,
            detalles=detalles,
        )

        return out_of_range_count == 0

    except sqlite3.Error as e:
        logging.error(f"Error validando rango en {table_name}.{column_name}: {e}")
        log_quality_metric(
            execution_id=execution_id,
            nombre_indicador="RANGE_VALIDATION",
            entidad_asociada=f"{table_name}.{column_name}",
            resultado="ERROR",
            detalles=f"Error SQL: {str(e)}",
        )
        return False


def log_record_count(execution_id: int, operation: str, table_name: str, count: int):
    """
    Registra el conteo de registros de una operación específica.

    Args:
        execution_id: ID de ejecución
        operation: Tipo de operación (LOADED, INSERTED, UPDATED, etc.)
        table_name: Nombre de la tabla
        count: Número de registros
    """
    log_quality_metric(
        execution_id=execution_id,
        nombre_indicador=f"RECORD_COUNT_{operation}",
        entidad_asociada=table_name,
        resultado=str(count),
        detalles=f"Operación: {operation}, Registros: {count}",
    )


def execute_transaction_with_retry(transaction_func, *args, **kwargs):
    """
    Ejecuta una transacción completa con reintentos automáticos.
    Maneja commits y rollbacks de manera segura.

    Args:
        transaction_func: Función que contiene la lógica de transacción
        *args, **kwargs: Argumentos para la función

    Returns:
        Resultado de la transacción
    """
    for attempt in range(MAX_RETRIES):
        conn = None
        try:
            conn = get_db_connection()
            if conn is None:
                raise sqlite3.Error("No se pudo obtener conexión a la base de datos")

            # Comenzar transacción
            conn.execute("BEGIN IMMEDIATE")

            # Ejecutar la función de transacción
            result = transaction_func(conn, *args, **kwargs)

            # Commit solo si todo salió bien
            conn.commit()
            return result

        except sqlite3.OperationalError as e:
            error_msg = str(e).lower()
            if conn:
                try:
                    conn.rollback()
                except:
                    pass
                conn.close()

            if (
                "database is locked" in error_msg or "database is busy" in error_msg
            ) and attempt < MAX_RETRIES - 1:
                base_delay = RETRY_DELAY * (2**attempt)
                jitter = random.uniform(0, base_delay * 0.5)
                delay = base_delay + jitter
                logging.warning(
                    f"Transacción falló ({e}), reintentando en {delay:.2f}s (intento {attempt + 1}/{MAX_RETRIES})"
                )
                time.sleep(delay)
                continue
            else:
                logging.error(
                    f"Error persistente en transacción después de {MAX_RETRIES} intentos: {e}"
                )
                raise

        except Exception as e:
            if conn:
                try:
                    conn.rollback()
                except:
                    pass
                conn.close()
            logging.error(f"Error en transacción: {e}")
            raise
        finally:
            if conn:
                conn.close()

    raise sqlite3.Error(f"Transacción falló después de {MAX_RETRIES} intentos")
