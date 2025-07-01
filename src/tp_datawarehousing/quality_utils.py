"""
Utilidades para control de calidad de datos en el pipeline DWA.
Funciones auxiliares para registrar métricas y validaciones en el DQM.
"""

import sqlite3
import logging
import time
import random
import re
import json
from datetime import datetime, timedelta
from typing import Optional, Any, Dict, List, Union
from enum import Enum

# --- Configuración ---
DB_PATH = "db/tp_dwa.db"
MAX_RETRIES = 8  # Aumentar reintentos para bloqueos
RETRY_DELAY = 0.2  # Delay inicial más conservador

# --- Configuración mejorada para SQLite concurrency ---
MAX_CONNECTION_TIMEOUT = 60.0  # Timeout más largo para operaciones complejas
BATCH_SIZE = 50  # Reducir batch size para menos contención
WAL_CHECKPOINT_INTERVAL = 1000  # Checkpoint WAL cada 1000 páginas
CONNECTION_POOL_SIZE = 3  # Pool de conexiones limitado

# --- Enums para niveles de severidad y calidad ---
class QualitySeverity(Enum):
    """Niveles de severidad para métricas de calidad"""
    CRITICAL = "CRITICAL"
    HIGH = "HIGH" 
    MEDIUM = "MEDIUM"
    LOW = "LOW"

class QualityResult(Enum):
    """Resultados estandarizados para métricas de calidad"""
    PASS = "PASS"
    WARNING = "WARNING"
    FAIL = "FAIL"
    CRITICAL = "CRITICAL"
    ERROR = "ERROR"

# --- Thresholds estandarizados ---
class QualityThresholds:
    """Umbrales estandarizados para métricas de calidad"""
    NULL_VALUES_WARNING = 5.0    # < 5% nulls = WARNING
    NULL_VALUES_FAIL = 15.0      # >= 15% nulls = FAIL
    COMPLETENESS_HIGH = 95.0     # >= 95% completo = PASS
    COMPLETENESS_MEDIUM = 80.0   # >= 80% completo = WARNING
    DUPLICATES_WARNING = 1.0     # < 1% duplicados = WARNING
    DUPLICATES_FAIL = 5.0        # >= 5% duplicados = FAIL
    FRESHNESS_WARNING_HOURS = 24 # > 24h = WARNING
    FRESHNESS_FAIL_HOURS = 72    # > 72h = FAIL


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
                or "cannot start a transaction within a transaction" in error_msg
            ) and attempt < MAX_RETRIES - 1:
                # Backoff exponencial más agresivo para bloqueos persistentes
                base_delay = RETRY_DELAY * (3**attempt)  # Crecimiento más rápido
                jitter = random.uniform(0.5, 1.5) * base_delay  # Jitter más amplio
                delay = min(base_delay + jitter, 30.0)  # Cap máximo de 30 segundos
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


def force_wal_checkpoint():
    """
    Fuerza un checkpoint del WAL para liberar bloqueos y reducir el tamaño del WAL.
    Útil cuando hay bloqueos persistentes.
    """
    try:
        conn = sqlite3.connect(DB_PATH, timeout=10.0)
        conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
        conn.close()
        logging.info("WAL checkpoint forzado exitosamente")
        return True
    except Exception as e:
        logging.warning(f"No se pudo hacer WAL checkpoint: {e}")
        return False


def optimize_database():
    """
    Optimiza la base de datos para mejorar el rendimiento y reducir bloqueos.
    Incluye VACUUM, ANALYZE, y limpieza de WAL.
    """
    try:
        conn = sqlite3.connect(DB_PATH, timeout=30.0)
        
        # Checkpoint y optimización
        conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
        conn.execute("PRAGMA optimize")
        conn.execute("ANALYZE")
        
        # Información de estado
        cursor = conn.cursor()
        cursor.execute("PRAGMA wal_autocheckpoint")
        checkpoint_size = cursor.fetchone()[0]
        
        cursor.execute("PRAGMA page_count")
        page_count = cursor.fetchone()[0]
        
        cursor.execute("PRAGMA freelist_count")
        free_pages = cursor.fetchone()[0]
        
        conn.close()
        
        logging.info(f"Base de datos optimizada: {page_count} páginas, {free_pages} libres, checkpoint cada {checkpoint_size}")
        return True
        
    except Exception as e:
        logging.error(f"Error optimizando base de datos: {e}")
        return False


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

        # Configuración optimizada para máxima concurrencia y estabilidad
        conn.execute("PRAGMA busy_timeout=60000")  # 60 segundos de timeout
        conn.execute("PRAGMA synchronous=NORMAL")  # Balance entre velocidad y seguridad
        conn.execute("PRAGMA journal_mode=WAL")  # Write-Ahead Logging para mejor concurrencia
        conn.execute("PRAGMA wal_autocheckpoint=1000")  # Checkpoint automático
        conn.execute("PRAGMA temp_store=MEMORY")  # Usar memoria para temp tables
        conn.execute("PRAGMA cache_size=20000")  # Cache más grande (20MB aprox)
        conn.execute("PRAGMA locking_mode=NORMAL")  # Asegurar unlocking apropiado
        conn.execute("PRAGMA page_size=4096")  # Tamaño de página optimizado
        conn.execute("PRAGMA mmap_size=268435456")  # 256MB memory mapped
        conn.execute("PRAGMA optimize")  # Optimización automática

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
    severidad: Optional[str] = None,
):
    """
    Registra una métrica de calidad en la tabla DQM_indicadores_calidad.

    Args:
        execution_id: ID de ejecución del proceso
        nombre_indicador: Nombre del indicador de calidad
        entidad_asociada: Tabla o entidad siendo validada
        resultado: Resultado de la validación ('PASS', 'FAIL', 'WARNING', 'CRITICAL', valor numérico)
        detalles: Detalles adicionales sobre el resultado
        severidad: Nivel de severidad (CRITICAL, HIGH, MEDIUM, LOW)
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

        if count >= expected_min:
            result = QualityResult.PASS.value
            severity = QualitySeverity.LOW.value
        else:
            result = QualityResult.FAIL.value
            severity = QualitySeverity.HIGH.value
        
        detalles = f"Registros encontrados: {count}, mínimo esperado: {expected_min}"

        log_quality_metric(
            execution_id=execution_id,
            nombre_indicador="COUNT_VALIDATION",
            entidad_asociada=table_name,
            resultado=result,
            detalles=detalles,
            severidad=severity
        )

        return count >= expected_min

    except sqlite3.Error as e:
        logging.error(f"Error validando conteo de tabla {table_name}: {e}")
        log_quality_metric(
            execution_id=execution_id,
            nombre_indicador="COUNT_VALIDATION",
            entidad_asociada=table_name,
            resultado=QualityResult.ERROR.value,
            detalles=f"Error SQL: {str(e)}",
            severidad=QualitySeverity.HIGH.value
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

        if null_count == 0:
            result = QualityResult.PASS.value
            severity = QualitySeverity.LOW.value
        else:
            result = QualityResult.FAIL.value
            severity = QualitySeverity.HIGH.value
        
        detalles = f"Valores NULL encontrados: {null_count}"

        log_quality_metric(
            execution_id=execution_id,
            nombre_indicador="NULL_VALIDATION",
            entidad_asociada=f"{table_name}.{column_name}",
            resultado=result,
            detalles=detalles,
            severidad=severity
        )

        return null_count == 0

    except sqlite3.Error as e:
        logging.error(f"Error validando NULLs en {table_name}.{column_name}: {e}")
        log_quality_metric(
            execution_id=execution_id,
            nombre_indicador="NULL_VALIDATION",
            entidad_asociada=f"{table_name}.{column_name}",
            resultado=QualityResult.ERROR.value,
            detalles=f"Error SQL: {str(e)}",
            severidad=QualitySeverity.HIGH.value
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

        if orphan_count == 0:
            result = QualityResult.PASS.value
            severity = QualitySeverity.LOW.value
        else:
            result = QualityResult.FAIL.value
            severity = QualitySeverity.CRITICAL.value  # Integridad referencial es crítica
        
        detalles = f"Registros huérfanos encontrados: {orphan_count}"

        log_quality_metric(
            execution_id=execution_id,
            nombre_indicador="REFERENTIAL_INTEGRITY",
            entidad_asociada=f"{child_table}.{fk_column} -> {parent_table}.{pk_column}",
            resultado=result,
            detalles=detalles,
            severidad=severity
        )

        return orphan_count == 0

    except sqlite3.Error as e:
        logging.error(f"Error validando integridad referencial: {e}")
        log_quality_metric(
            execution_id=execution_id,
            nombre_indicador="REFERENTIAL_INTEGRITY",
            entidad_asociada=f"{child_table}.{fk_column} -> {parent_table}.{pk_column}",
            resultado=QualityResult.ERROR.value,
            detalles=f"Error SQL: {str(e)}",
            severidad=QualitySeverity.HIGH.value
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

        if out_of_range_count == 0:
            result = QualityResult.PASS.value
            severity = QualitySeverity.LOW.value
        else:
            result = QualityResult.FAIL.value
            severity = QualitySeverity.MEDIUM.value
        
        detalles = f"Valores fuera de rango: {out_of_range_count} (min: {min_value}, max: {max_value})"

        log_quality_metric(
            execution_id=execution_id,
            nombre_indicador="RANGE_VALIDATION",
            entidad_asociada=f"{table_name}.{column_name}",
            resultado=result,
            detalles=detalles,
            severidad=severity
        )

        return out_of_range_count == 0

    except sqlite3.Error as e:
        logging.error(f"Error validando rango en {table_name}.{column_name}: {e}")
        log_quality_metric(
            execution_id=execution_id,
            nombre_indicador="RANGE_VALIDATION",
            entidad_asociada=f"{table_name}.{column_name}",
            resultado=QualityResult.ERROR.value,
            detalles=f"Error SQL: {str(e)}",
            severidad=QualitySeverity.HIGH.value
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
        severidad=QualitySeverity.LOW.value  # Informacional
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
                "database is locked" in error_msg 
                or "database is busy" in error_msg
                or "cannot start a transaction within a transaction" in error_msg
            ) and attempt < MAX_RETRIES - 1:
                # Backoff más agresivo para transacciones
                base_delay = RETRY_DELAY * (3**attempt)
                jitter = random.uniform(1.0, 2.0) * base_delay
                delay = min(base_delay + jitter, 45.0)  # Cap más alto para transacciones
                
                # Intentar checkpoint WAL después de varios fallos
                if attempt >= 3:
                    logging.info("Intentando WAL checkpoint para resolver bloqueos...")
                    force_wal_checkpoint()
                
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


# =============================================================================
# NUEVAS FUNCIONES DE VALIDACIÓN AVANZADA - MEJORES PRÁCTICAS PROFESIONALES
# =============================================================================

def validate_completeness_score(
    execution_id: int,
    table_name: str,
    critical_fields: List[str],
    conn: sqlite3.Connection = None,
) -> float:
    """
    Calcula y registra un puntaje de completitud para campos críticos.
    
    Args:
        execution_id: ID de ejecución
        table_name: Nombre de la tabla
        critical_fields: Lista de campos críticos para el negocio
        conn: Conexión existente (opcional)
    
    Returns:
        Porcentaje de completitud (0-100)
    """
    def _calculate_completeness():
        if conn is None:
            connection = get_db_connection()
            should_close = True
        else:
            connection = conn
            should_close = False

        try:
            cursor = connection.cursor()
            
            # Obtener total de registros
            cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
            total_records = cursor.fetchone()[0]
            
            if total_records == 0:
                return 0.0
            
            complete_records = 0
            field_completeness = {}
            
            for field in critical_fields:
                # Contar registros completos para este campo
                cursor.execute(f"""
                    SELECT COUNT(*) FROM {table_name} 
                    WHERE {field} IS NOT NULL AND TRIM({field}) != ''
                """)
                field_complete = cursor.fetchone()[0]
                field_percentage = (field_complete / total_records) * 100
                field_completeness[field] = field_percentage
            
            # Calcular completitud promedio
            avg_completeness = sum(field_completeness.values()) / len(critical_fields)
            
            return avg_completeness, field_completeness, total_records
            
        finally:
            if should_close:
                connection.close()

    try:
        avg_completeness, field_details, total_records = execute_with_retry(_calculate_completeness)
        
        # Determinar resultado y severidad
        if avg_completeness >= QualityThresholds.COMPLETENESS_HIGH:
            result = QualityResult.PASS.value
            severity = QualitySeverity.LOW.value
        elif avg_completeness >= QualityThresholds.COMPLETENESS_MEDIUM:
            result = QualityResult.WARNING.value
            severity = QualitySeverity.MEDIUM.value
        else:
            result = QualityResult.FAIL.value
            severity = QualitySeverity.HIGH.value
        
        # Generar detalles
        field_summary = ", ".join([f"{field}: {pct:.1f}%" for field, pct in field_details.items()])
        detalles = f"Completitud promedio: {avg_completeness:.1f}% ({field_summary}), Registros: {total_records}"
        
        log_quality_metric(
            execution_id=execution_id,
            nombre_indicador="COMPLETENESS_SCORE",
            entidad_asociada=table_name,
            resultado=result,
            detalles=detalles,
            severidad=severity
        )
        
        return avg_completeness

    except sqlite3.Error as e:
        logging.error(f"Error calculando completitud de {table_name}: {e}")
        log_quality_metric(
            execution_id=execution_id,
            nombre_indicador="COMPLETENESS_SCORE",
            entidad_asociada=table_name,
            resultado=QualityResult.ERROR.value,
            detalles=f"Error SQL: {str(e)}",
            severidad=QualitySeverity.HIGH.value
        )
        return 0.0


def validate_format_patterns(
    execution_id: int,
    table_name: str,
    column_name: str,
    pattern_type: str,
    conn: sqlite3.Connection = None,
) -> bool:
    """
    Valida formatos de datos usando patrones regex predefinidos.
    
    Args:
        execution_id: ID de ejecución
        table_name: Nombre de la tabla
        column_name: Nombre de la columna
        pattern_type: Tipo de patrón ('email', 'phone', 'date', 'postal_code')
        conn: Conexión existente (opcional)
    
    Returns:
        True si todos los valores siguen el patrón, False si hay violaciones
    """
    # Patrones regex predefinidos
    patterns = {
        'email': r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$',
        'phone': r'^\+?[\d\s\-\(\)]{10,}$',
        'date_iso': r'^\d{4}-\d{2}-\d{2}$',
        'postal_code': r'^[\d\w\s\-]{3,10}$',
        'customer_id': r'^[A-Z]{3,5}$',
        'currency_code': r'^[A-Z]{3}$'
    }
    
    if pattern_type not in patterns:
        log_quality_metric(
            execution_id=execution_id,
            nombre_indicador="FORMAT_VALIDATION",
            entidad_asociada=f"{table_name}.{column_name}",
            resultado=QualityResult.ERROR.value,
            detalles=f"Tipo de patrón desconocido: {pattern_type}",
            severidad=QualitySeverity.MEDIUM.value
        )
        return False
    
    def _validate_pattern():
        if conn is None:
            connection = get_db_connection()
            should_close = True
        else:
            connection = conn
            should_close = False

        try:
            cursor = connection.cursor()
            
            # Obtener valores no nulos para validar
            cursor.execute(f"""
                SELECT {column_name}, COUNT(*) as count 
                FROM {table_name} 
                WHERE {column_name} IS NOT NULL AND TRIM({column_name}) != ''
                GROUP BY {column_name}
            """)
            
            pattern = patterns[pattern_type]
            invalid_count = 0
            total_count = 0
            invalid_samples = []
            
            for row in cursor.fetchall():
                value, count = row
                total_count += count
                if not re.match(pattern, str(value)):
                    invalid_count += count
                    if len(invalid_samples) < 5:  # Limitar ejemplos
                        invalid_samples.append(str(value))
            
            return invalid_count, total_count, invalid_samples
            
        finally:
            if should_close:
                connection.close()

    try:
        invalid_count, total_count, invalid_samples = execute_with_retry(_validate_pattern)
        
        if total_count == 0:
            result = QualityResult.WARNING.value
            severity = QualitySeverity.LOW.value
            detalles = "No hay valores para validar"
        elif invalid_count == 0:
            result = QualityResult.PASS.value
            severity = QualitySeverity.LOW.value
            detalles = f"Todos los {total_count} valores siguen el patrón {pattern_type}"
        else:
            invalid_percentage = (invalid_count / total_count) * 100
            if invalid_percentage < 5.0:
                result = QualityResult.WARNING.value
                severity = QualitySeverity.MEDIUM.value
            else:
                result = QualityResult.FAIL.value
                severity = QualitySeverity.HIGH.value
            
            samples_str = ", ".join(invalid_samples[:3]) + ("..." if len(invalid_samples) > 3 else "")
            detalles = f"Formato inválido: {invalid_count}/{total_count} ({invalid_percentage:.1f}%). Ejemplos: {samples_str}"
        
        log_quality_metric(
            execution_id=execution_id,
            nombre_indicador="FORMAT_VALIDATION",
            entidad_asociada=f"{table_name}.{column_name}",
            resultado=result,
            detalles=detalles,
            severidad=severity
        )
        
        return invalid_count == 0

    except sqlite3.Error as e:
        logging.error(f"Error validando formato en {table_name}.{column_name}: {e}")
        log_quality_metric(
            execution_id=execution_id,
            nombre_indicador="FORMAT_VALIDATION",
            entidad_asociada=f"{table_name}.{column_name}",
            resultado=QualityResult.ERROR.value,
            detalles=f"Error SQL: {str(e)}",
            severidad=QualitySeverity.HIGH.value
        )
        return False


def validate_business_key_uniqueness(
    execution_id: int,
    table_name: str,
    business_key_fields: List[str],
    conn: sqlite3.Connection = None,
) -> bool:
    """
    Valida la unicidad de claves de negocio (natural keys).
    
    Args:
        execution_id: ID de ejecución
        table_name: Nombre de la tabla
        business_key_fields: Lista de campos que forman la clave de negocio
        conn: Conexión existente (opcional)
    
    Returns:
        True si no hay duplicados, False si hay violaciones de unicidad
    """
    def _validate_uniqueness():
        if conn is None:
            connection = get_db_connection()
            should_close = True
        else:
            connection = conn
            should_close = False

        try:
            cursor = connection.cursor()
            
            # Construir cláusula WHERE para valores no nulos
            non_null_conditions = " AND ".join([f"{field} IS NOT NULL" for field in business_key_fields])
            fields_str = ", ".join(business_key_fields)
            
            # Encontrar duplicados
            query = f"""
                SELECT {fields_str}, COUNT(*) as duplicate_count
                FROM {table_name}
                WHERE {non_null_conditions}
                GROUP BY {fields_str}
                HAVING COUNT(*) > 1
                ORDER BY duplicate_count DESC
            """
            
            cursor.execute(query)
            duplicates = cursor.fetchall()
            
            # Contar total de registros con clave válida
            cursor.execute(f"SELECT COUNT(*) FROM {table_name} WHERE {non_null_conditions}")
            total_valid_records = cursor.fetchone()[0]
            
            return duplicates, total_valid_records
            
        finally:
            if should_close:
                connection.close()

    try:
        duplicates, total_valid_records = execute_with_retry(_validate_uniqueness)
        
        if len(duplicates) == 0:
            result = QualityResult.PASS.value
            severity = QualitySeverity.LOW.value
            detalles = f"Todas las claves de negocio son únicas ({total_valid_records} registros validados)"
        else:
            total_duplicate_records = sum([dup[-1] for dup in duplicates])
            duplicate_percentage = (total_duplicate_records / total_valid_records) * 100 if total_valid_records > 0 else 0
            
            if duplicate_percentage < QualityThresholds.DUPLICATES_WARNING:
                result = QualityResult.WARNING.value
                severity = QualitySeverity.MEDIUM.value
            else:
                result = QualityResult.FAIL.value
                severity = QualitySeverity.HIGH.value
            
            # Mostrar algunos ejemplos de duplicados
            examples = []
            for dup in duplicates[:3]:
                key_values = dup[:-1]
                count = dup[-1]
                key_str = ", ".join([str(v) for v in key_values])
                examples.append(f"({key_str}): {count} veces")
            
            examples_str = "; ".join(examples)
            detalles = f"Duplicados encontrados: {len(duplicates)} claves, {total_duplicate_records} registros ({duplicate_percentage:.1f}%). Ejemplos: {examples_str}"
        
        log_quality_metric(
            execution_id=execution_id,
            nombre_indicador="BUSINESS_KEY_UNIQUENESS",
            entidad_asociada=table_name,
            resultado=result,
            detalles=detalles,
            severidad=severity
        )
        
        return len(duplicates) == 0

    except sqlite3.Error as e:
        logging.error(f"Error validando unicidad de clave de negocio en {table_name}: {e}")
        log_quality_metric(
            execution_id=execution_id,
            nombre_indicador="BUSINESS_KEY_UNIQUENESS",
            entidad_asociada=table_name,
            resultado=QualityResult.ERROR.value,
            detalles=f"Error SQL: {str(e)}",
            severidad=QualitySeverity.HIGH.value
        )
        return False


def validate_data_freshness(
    execution_id: int,
    table_name: str,
    date_column: str,
    expected_freshness_hours: Optional[int] = None,
    conn: sqlite3.Connection = None,
) -> bool:
    """
    Valida la frescura de los datos comparando con SLAs temporales.
    
    Args:
        execution_id: ID de ejecución
        table_name: Nombre de la tabla
        date_column: Columna de fecha para medir frescura
        expected_freshness_hours: SLA esperado en horas (default: 24h)
        conn: Conexión existente (opcional)
    
    Returns:
        True si los datos están frescos, False si están obsoletos
    """
    if expected_freshness_hours is None:
        expected_freshness_hours = QualityThresholds.FRESHNESS_WARNING_HOURS
    
    def _validate_freshness():
        if conn is None:
            connection = get_db_connection()
            should_close = True
        else:
            connection = conn
            should_close = False

        try:
            cursor = connection.cursor()
            
            # Obtener la fecha más reciente en los datos
            cursor.execute(f"""
                SELECT MAX({date_column}) as max_date,
                       MIN({date_column}) as min_date,
                       COUNT(*) as total_records
                FROM {table_name}
                WHERE {date_column} IS NOT NULL
            """)
            
            result = cursor.fetchone()
            max_date_str, min_date_str, total_records = result
            
            if max_date_str is None:
                return None, None, 0, "No hay fechas válidas"
            
            # Intentar parsear la fecha (múltiples formatos)
            date_formats = ['%Y-%m-%d %H:%M:%S', '%Y-%m-%d', '%Y-%m-%d %H:%M:%S.%f']
            max_date = None
            min_date = None
            
            for fmt in date_formats:
                try:
                    max_date = datetime.strptime(max_date_str, fmt)
                    min_date = datetime.strptime(min_date_str, fmt)
                    break
                except ValueError:
                    continue
            
            if max_date is None:
                return None, None, total_records, f"Formato de fecha no reconocido: {max_date_str}"
            
            # Calcular antigüedad
            now = datetime.now()
            hours_old = (now - max_date).total_seconds() / 3600
            date_range_days = (max_date - min_date).days if min_date else 0
            
            return max_date, hours_old, total_records, date_range_days
            
        finally:
            if should_close:
                connection.close()

    try:
        max_date, hours_old, total_records, date_range_info = execute_with_retry(_validate_freshness)
        
        if max_date is None:
            result = QualityResult.WARNING.value
            severity = QualitySeverity.MEDIUM.value
            detalles = str(date_range_info)  # Error message
        elif hours_old <= expected_freshness_hours:
            result = QualityResult.PASS.value
            severity = QualitySeverity.LOW.value
            detalles = f"Datos frescos: {hours_old:.1f}h de antigüedad (SLA: {expected_freshness_hours}h). Rango: {date_range_info} días, {total_records} registros"
        elif hours_old <= QualityThresholds.FRESHNESS_FAIL_HOURS:
            result = QualityResult.WARNING.value
            severity = QualitySeverity.MEDIUM.value
            detalles = f"Datos con advertencia: {hours_old:.1f}h de antigüedad (SLA: {expected_freshness_hours}h). Rango: {date_range_info} días, {total_records} registros"
        else:
            result = QualityResult.FAIL.value
            severity = QualitySeverity.HIGH.value
            detalles = f"Datos obsoletos: {hours_old:.1f}h de antigüedad (SLA: {expected_freshness_hours}h). Rango: {date_range_info} días, {total_records} registros"
        
        log_quality_metric(
            execution_id=execution_id,
            nombre_indicador="DATA_FRESHNESS",
            entidad_asociada=f"{table_name}.{date_column}",
            resultado=result,
            detalles=detalles,
            severidad=severity
        )
        
        return max_date is not None and hours_old <= expected_freshness_hours

    except sqlite3.Error as e:
        logging.error(f"Error validando frescura en {table_name}.{date_column}: {e}")
        log_quality_metric(
            execution_id=execution_id,
            nombre_indicador="DATA_FRESHNESS",
            entidad_asociada=f"{table_name}.{date_column}",
            resultado=QualityResult.ERROR.value,
            detalles=f"Error SQL: {str(e)}",
            severidad=QualitySeverity.HIGH.value
        )
        return False


def validate_cross_field_logic(
    execution_id: int,
    table_name: str,
    validations: List[Dict[str, Any]],
    conn: sqlite3.Connection = None,
) -> bool:
    """
    Valida lógica de negocio entre campos (ej: ship_date >= order_date).
    
    Args:
        execution_id: ID de ejecución
        table_name: Nombre de la tabla
        validations: Lista de validaciones con formato:
                   [{'rule': 'ship_date >= order_date', 'description': 'Fecha envío posterior a pedido'}]
        conn: Conexión existente (opcional)
    
    Returns:
        True si todas las validaciones pasan, False si hay violaciones
    """
    def _validate_logic():
        if conn is None:
            connection = get_db_connection()
            should_close = True
        else:
            connection = conn
            should_close = False

        try:
            cursor = connection.cursor()
            all_passed = True
            violations_summary = []
            
            for validation in validations:
                rule = validation['rule']
                description = validation.get('description', rule)
                
                # Construir query para encontrar violaciones
                query = f"""
                    SELECT COUNT(*) as violations
                    FROM {table_name}
                    WHERE NOT ({rule}) AND 
                          {rule.split()[0]} IS NOT NULL AND 
                          {rule.split()[2]} IS NOT NULL
                """
                
                cursor.execute(query)
                violations = cursor.fetchone()[0]
                
                if violations > 0:
                    all_passed = False
                    violations_summary.append(f"{description}: {violations} violaciones")
            
            return all_passed, violations_summary
            
        finally:
            if should_close:
                connection.close()

    try:
        all_passed, violations_summary = execute_with_retry(_validate_logic)
        
        if all_passed:
            result = QualityResult.PASS.value
            severity = QualitySeverity.LOW.value
            detalles = f"Todas las {len(validations)} validaciones lógicas pasaron"
        else:
            result = QualityResult.FAIL.value
            severity = QualitySeverity.HIGH.value
            detalles = "; ".join(violations_summary)
        
        log_quality_metric(
            execution_id=execution_id,
            nombre_indicador="CROSS_FIELD_LOGIC",
            entidad_asociada=table_name,
            resultado=result,
            detalles=detalles,
            severidad=severity
        )
        
        return all_passed

    except sqlite3.Error as e:
        logging.error(f"Error validando lógica cruzada en {table_name}: {e}")
        log_quality_metric(
            execution_id=execution_id,
            nombre_indicador="CROSS_FIELD_LOGIC",
            entidad_asociada=table_name,
            resultado=QualityResult.ERROR.value,
            detalles=f"Error SQL: {str(e)}",
            severidad=QualitySeverity.HIGH.value
        )
        return False


def validate_domain_constraints(
    execution_id: int,
    table_name: str,
    column_name: str,
    valid_values: List[Any],
    case_sensitive: bool = True,
    conn: sqlite3.Connection = None,
) -> bool:
    """
    Valida que los valores estén dentro de un dominio permitido.
    
    Args:
        execution_id: ID de ejecución
        table_name: Nombre de la tabla
        column_name: Nombre de la columna
        valid_values: Lista de valores permitidos
        case_sensitive: Si la validación es sensible a mayúsculas/minúsculas
        conn: Conexión existente (opcional)
    
    Returns:
        True si todos los valores están en el dominio, False si hay violaciones
    """
    def _validate_domain():
        if conn is None:
            connection = get_db_connection()
            should_close = True
        else:
            connection = conn
            should_close = False

        try:
            cursor = connection.cursor()
            
            # Preparar valores válidos
            if case_sensitive:
                valid_values_processed = [str(v) for v in valid_values]
                placeholders = ", ".join(["?" for _ in valid_values])
                query = f"""
                    SELECT {column_name}, COUNT(*) as count
                    FROM {table_name}
                    WHERE {column_name} IS NOT NULL 
                      AND {column_name} NOT IN ({placeholders})
                    GROUP BY {column_name}
                    ORDER BY count DESC
                """
            else:
                valid_values_processed = [str(v).upper() for v in valid_values]
                placeholders = ", ".join(["?" for _ in valid_values])
                query = f"""
                    SELECT {column_name}, COUNT(*) as count
                    FROM {table_name}
                    WHERE {column_name} IS NOT NULL 
                      AND UPPER({column_name}) NOT IN ({placeholders})
                    GROUP BY {column_name}
                    ORDER BY count DESC
                """
            
            cursor.execute(query, valid_values_processed)
            invalid_values = cursor.fetchall()
            
            # Contar total de valores válidos
            cursor.execute(f"SELECT COUNT(*) FROM {table_name} WHERE {column_name} IS NOT NULL")
            total_values = cursor.fetchone()[0]
            
            return invalid_values, total_values
            
        finally:
            if should_close:
                connection.close()

    try:
        invalid_values, total_values = execute_with_retry(_validate_domain)
        
        if len(invalid_values) == 0:
            result = QualityResult.PASS.value
            severity = QualitySeverity.LOW.value
            detalles = f"Todos los {total_values} valores están en el dominio permitido ({len(valid_values)} valores válidos)"
        else:
            invalid_count = sum([count for _, count in invalid_values])
            invalid_percentage = (invalid_count / total_values) * 100 if total_values > 0 else 0
            
            if invalid_percentage < 5.0:
                result = QualityResult.WARNING.value
                severity = QualitySeverity.MEDIUM.value
            else:
                result = QualityResult.FAIL.value
                severity = QualitySeverity.HIGH.value
            
            # Mostrar ejemplos de valores inválidos
            examples = [f"'{value}' ({count}x)" for value, count in invalid_values[:5]]
            examples_str = ", ".join(examples)
            if len(invalid_values) > 5:
                examples_str += "..."
            
            detalles = f"Valores fuera de dominio: {invalid_count}/{total_values} ({invalid_percentage:.1f}%). Ejemplos: {examples_str}"
        
        log_quality_metric(
            execution_id=execution_id,
            nombre_indicador="DOMAIN_CONSTRAINTS",
            entidad_asociada=f"{table_name}.{column_name}",
            resultado=result,
            detalles=detalles,
            severidad=severity
        )
        
        return len(invalid_values) == 0

    except sqlite3.Error as e:
        logging.error(f"Error validando dominio en {table_name}.{column_name}: {e}")
        log_quality_metric(
            execution_id=execution_id,
            nombre_indicador="DOMAIN_CONSTRAINTS",
            entidad_asociada=f"{table_name}.{column_name}",
            resultado=QualityResult.ERROR.value,
            detalles=f"Error SQL: {str(e)}",
            severidad=QualitySeverity.HIGH.value
        )
        return False
