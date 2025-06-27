# Análisis Paso 6: Create DQM (step_06_create_dqm.py)

## 1. Qué hace el script

Este script implementa la **infraestructura de Data Quality Mart** para monitoreo y auditoría del DWH. Sus responsabilidades incluyen:

- **Crear tablas DQM especializadas** para tracking de calidad y procesos
- **Implementar relaciones entre tablas DQM** con foreign keys
- **Forzar recreación limpia** con DROP/CREATE para esquema consistente
- **Registrar automáticamente** todas las tablas DQM en metadata institucional
- **Establecer fundación** para auditoría de procesos ETL

### Tablas DQM creadas:
- **DQM_ejecucion_procesos**: Log central de todos los procesos ETL
- **DQM_descriptivos_entidad**: Métricas descriptivas por entidad/proceso
- **DQM_indicadores_calidad**: Resultados de controles de calidad específicos

## 2. Control de calidad de datos

✅ **IMPLEMENTA** infraestructura completa de calidad:

### Estructura de Calidad:
- **Tracking de procesos**: Inicio, fin, duración, estado (Exitoso/Fallido/En Progreso)
- **Métricas descriptivas**: Conteos, estadísticas, descriptivos por entidad
- **Indicadores específicos**: Resultados detallados de validaciones

### Controles Estructurales:
- **Foreign Keys**: Relaciones entre procesos y sus métricas/indicadores
- **Recreación forzada**: DROP IF EXISTS para evitar inconsistencias de schema
- **Rollback automático**: Transacciones para integridad

### Capacidades de Auditoría:
```sql
-- Estructura para tracking completo:
-- ¿Cuándo corrió el proceso?
-- ¿Cuánto tardó?
-- ¿Fue exitoso?
-- ¿Qué métricas se generaron?
-- ¿Qué controles pasaron/fallaron?
```

## 3. Transformación de datos

❌ **NO REALIZA** transformaciones - **SOLO CREA INFRAESTRUCTURA**

### Preparación para Futuras Transformaciones:
- **Schema flexible**: `valor_metrica` como TEXT para cualquier tipo de métrica
- **Estructura extensible**: Nuevos indicadores sin cambios de schema
- **Relaciones normalizadas**: Un proceso puede tener múltiples métricas/indicadores

**Nota**: Las transformaciones de calidad ocurren en step_07+ cuando se usan estas tablas.

## 4. Cumplimiento con consignas del TP

✅ **CUMPLE PERFECTAMENTE** con consigna específica:

### Punto 7 del TP: ✅
- ✅ Diseñar y crear DQM → **Diseño completo implementado**
- ✅ Persistir procesos ejecutados → **DQM_ejecucion_procesos**
- ✅ Descriptivos de entidad → **DQM_descriptivos_entidad**  
- ✅ Indicadores de calidad → **DQM_indicadores_calidad**
- ✅ Documentar en Metadata → **Registro automático en MET_entidades**

### Recomendación 12d del TP: ✅
- ✅ Prefijo DQM_ para Data Quality Mart → **Implementado consistentemente**

### Recomendación 15 del TP: ✅
- ✅ Persistir indicadores de calidad → **Estructura preparada para todos los tipos**
- ✅ Estadísticas cuantitativas → **DQM_descriptivos_entidad diseñado para esto**

## 5. Mejoras sugeridas

### Mejoras de Estructura DQM:
1. **Tabla de reglas de negocio**:
```sql
CREATE TABLE DQM_reglas_calidad (
    id_regla INTEGER PRIMARY KEY,
    nombre_regla TEXT NOT NULL,
    descripcion TEXT,
    query_validacion TEXT,
    umbral_aceptable REAL,
    criticidad TEXT -- 'CRITICA', 'ADVERTENCIA', 'INFO'
);
```

2. **Tabla de alertas**:
```sql
CREATE TABLE DQM_alertas (
    id_alerta INTEGER PRIMARY KEY,
    id_ejecucion INTEGER,
    tipo_alerta TEXT,
    descripcion TEXT,
    fecha_creacion DATETIME,
    estado TEXT -- 'PENDIENTE', 'REVISADO', 'RESUELTO'
);
```

3. **Historial de calidad**:
```sql
CREATE TABLE DQM_tendencias_calidad (
    id_tendencia INTEGER PRIMARY KEY,
    entidad TEXT,
    metrica TEXT,
    fecha DATE,
    valor REAL,
    tendencia TEXT -- 'MEJORA', 'EMPEORA', 'ESTABLE'
);
```

### Mejoras de Metadata DQM:
4. **Lineage de datos**:
```sql
CREATE TABLE DQM_lineage (
    id_lineage INTEGER PRIMARY KEY,
    tabla_origen TEXT,
    tabla_destino TEXT,
    transformacion TEXT,
    id_ejecucion INTEGER
);
```

5. **Documentación de procesos**:
```sql
CREATE TABLE DQM_documentacion_procesos (
    nombre_proceso TEXT PRIMARY KEY,
    descripcion TEXT,
    owner TEXT,
    frecuencia TEXT,
    dependencias TEXT
);
```

### Mejoras de Configuración:
6. **Umbrales configurables**:
```python
DQ_THRESHOLDS = {
    'null_percentage_warning': 5.0,
    'null_percentage_critical': 15.0,
    'row_count_variance_warning': 10.0,
    'duplicate_percentage_critical': 1.0
}
```

7. **Plantillas de validación**:
```python
VALIDATION_TEMPLATES = {
    'null_check': "SELECT COUNT(*) FROM {table} WHERE {column} IS NULL",
    'duplicate_check': "SELECT COUNT(*) - COUNT(DISTINCT {column}) FROM {table}",
    'range_check': "SELECT COUNT(*) FROM {table} WHERE {column} NOT BETWEEN {min} AND {max}"
}
```

### Mejoras de Automatización:
8. **Auto-discovery de métricas**:
```python
def auto_generate_metrics(table_name):
    # Generar automáticamente métricas estándar
    # Row count, null counts, distinct counts
    # Min/max para campos numéricos
    # Patrones para campos de texto
```

9. **Detección de anomalías**:
```python
def detect_anomalies(metric_history):
    # Algoritmos estadísticos simples
    # Detección de outliers
    # Comparación con períodos anteriores
```

### Mejoras de Reporting:
10. **Dashboard de calidad**:
```sql
-- Views agregadas para reporting
CREATE VIEW DQM_dashboard_calidad AS
SELECT 
    DATE(fecha_inicio) as fecha,
    COUNT(*) as procesos_ejecutados,
    SUM(CASE WHEN estado = 'Exitoso' THEN 1 ELSE 0 END) as exitosos,
    AVG(duracion_seg) as duracion_promedio
FROM DQM_ejecucion_procesos
GROUP BY DATE(fecha_inicio);
```

### Código sugerido para auto-métricas:
```python
def generate_standard_metrics(conn, table_name, process_id):
    cursor = conn.cursor()
    
    # Row count
    cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
    row_count = cursor.fetchone()[0]
    log_dq_metric(conn, process_id, table_name, "row_count", row_count)
    
    # Get all columns
    cursor.execute(f"PRAGMA table_info({table_name})")
    columns = cursor.fetchall()
    
    for col_info in columns:
        col_name = col_info[1]
        
        # Null count
        cursor.execute(f"SELECT COUNT(*) FROM {table_name} WHERE {col_name} IS NULL")
        null_count = cursor.fetchone()[0]
        log_dq_metric(conn, process_id, table_name, f"{col_name}_null_count", null_count)
        
        # Distinct count
        cursor.execute(f"SELECT COUNT(DISTINCT {col_name}) FROM {table_name}")
        distinct_count = cursor.fetchone()[0]
        log_dq_metric(conn, process_id, table_name, f"{col_name}_distinct_count", distinct_count)
```

## Veredicto Final

**INFRAESTRUCTURA DE CALIDAD SÓLIDA Y PROFESIONAL** que establece la fundación para auditoría y monitoreo comprehensivo. El diseño es extensible y permite tracking detallado de todos los aspectos del pipeline ETL.

**Fortalezas destacadas**:
- Diseño normalizado y extensible
- Cobertura completa: procesos, métricas, indicadores
- Integración automática con metadata
- Preparación para auditoría enterprise-level

Este paso es **fundamental para la gobernanza de datos** y permite implementar un DWH auditado y monitoreado según estándares profesionales.