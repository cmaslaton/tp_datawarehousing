"""
Utilidades para control de calidad de datos en el pipeline DWA.
Funciones auxiliares para registrar métricas y validaciones en el DQM.
"""

import sqlite3
import logging
from datetime import datetime
from typing import Optional, Any

# --- Constantes ---
DB_PATH = "db/tp_dwa.db"


def get_process_execution_id(proceso_nombre: str) -> int:
    """
    Obtiene o crea un ID de ejecución para el proceso actual.

    Args:
        proceso_nombre: Nombre del proceso en ejecución

    Returns:
        ID de ejecución para usar en las métricas de calidad
    """
    try:
        conn = sqlite3.connect(DB_PATH)
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
        conn.close()

        return execution_id
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
    try:
        conn = sqlite3.connect(DB_PATH)
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
        conn.close()

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
    try:
        conn = sqlite3.connect(DB_PATH)
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
        conn.close()

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
    try:
        if conn is None:
            conn = sqlite3.connect(DB_PATH)
            should_close = True
        else:
            should_close = False

        cursor = conn.cursor()

        cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
        count = cursor.fetchone()[0]

        if should_close:
            conn.close()

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
    try:
        if conn is None:
            conn = sqlite3.connect(DB_PATH)
            should_close = True
        else:
            should_close = False

        cursor = conn.cursor()

        cursor.execute(f"SELECT COUNT(*) FROM {table_name} WHERE {column_name} IS NULL")
        null_count = cursor.fetchone()[0]

        if should_close:
            conn.close()

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
    try:
        if conn is None:
            conn = sqlite3.connect(DB_PATH)
            should_close = True
        else:
            should_close = False

        cursor = conn.cursor()

        query = f"""
        SELECT COUNT(*) 
        FROM {child_table} c
        LEFT JOIN {parent_table} p ON c.{fk_column} = p.{pk_column}
        WHERE c.{fk_column} IS NOT NULL AND p.{pk_column} IS NULL
        """

        cursor.execute(query)
        orphan_count = cursor.fetchone()[0]

        if should_close:
            conn.close()

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
    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()

        conditions = []
        if min_value is not None:
            conditions.append(f"{column_name} < {min_value}")
        if max_value is not None:
            conditions.append(f"{column_name} > {max_value}")

        if not conditions:
            return True  # No hay restricciones

        where_clause = " OR ".join(conditions)
        query = f"SELECT COUNT(*) FROM {table_name} WHERE {where_clause}"

        cursor.execute(query)
        out_of_range_count = cursor.fetchone()[0]
        conn.close()

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
