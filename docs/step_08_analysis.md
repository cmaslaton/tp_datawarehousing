# Análisis Paso 8: Load Ingesta2 to Staging (step_08_load_ingesta2_to_staging.py)

## 1. Qué hace el script

Este script implementa la **carga de datos incrementales** (Ingesta2) preparando para actualizaciones del DWH. Sus responsabilidades incluyen:

- **Crear tablas TMP2_ específicas** para datos de actualización  
- **Cargar archivos CSV de Ingesta2** con normalización de columnas
- **Preparar staging separado** para no contaminar datos originales
- **Mantener compatibilidad** con esquemas de la capa de ingesta (ING_)
- **Logging detallado** del proceso de carga incremental

### Archivos procesados de Ingesta2:
- **customers - novedades.csv** → `TMP2_customers`
- **orders - novedades.csv** → `TMP2_orders`  
- **order_details - novedades.csv** → `TMP2_order_details`

### Estrategia de aislamiento:
**TMP2_** (nuevos datos) vs **TMP_/ING_** (datos históricos)

## 2. Control de calidad de datos

✅ **IMPLEMENTA** controles básicos de calidad:

### Controles de Carga:
- **Validación de archivos**: Verifica existencia antes de procesar
- **Limpieza preventiva**: `DROP TABLE IF EXISTS` para carga limpia
- **Manejo de errores**: Try/catch por archivo sin abortar el proceso
- **Logging de problemas**: Registra archivos faltantes y errores de carga

### Controles de Formato:
- **Normalización de columnas**: Reutiliza función `normalize_column_name()`
- **Compatibilidad de esquema**: Esquemas TMP2_ compatibles con ING_
- **Manejo de encoding**: (Implícito en pandas.read_csv)

### Limitaciones:
- **Sin validación de contenido**: No verifica PKs, rangos, o business rules
- **Sin comparación con datos base**: No detecta conflictos con datos existentes
- **Sin métricas de calidad**: No registra estadísticas en DQM

**Observación**: Controles detallados se ejecutan en step_09 durante la actualización.

## 3. Transformación de datos

✅ **REALIZA** transformación crítica de normalización:

### Transformación Principal - Normalización de Columnas:
```python
# Reutiliza la misma lógica que step_02
# Asegura consistencia entre Ingesta1 e Ingesta2
normalize_column_name("Contact Name") → "contact_name"
normalize_column_name("OrderID") → "order_id"
```

### Transformaciones Estructurales:
- **Mapeo de archivos**: CSV names → tabla names consistente
- **Schema alignment**: TMP2_ compatible con ING_ para futuras operaciones
- **Limpieza de datos**: DROP/CREATE evita contaminación de cargas anteriores

### Preparación para SCD:
- **Preservación de PKs**: Customer_id, order_id mantenidos para SCD matching
- **Campos de negocio**: Todos los campos necesarios para detectar cambios
- **Compatibilidad temporal**: Esquemas preparados para merge con DWH

## 4. Cumplimiento con consignas del TP

✅ **CUMPLE ESPECÍFICAMENTE** con consignas de actualización:

### Punto 9a del TP: ✅
- ✅ Persistir en área temporal → **Tablas TMP2_ creadas y cargadas**
- ✅ Tablas entregadas como Ingesta2 → **3 archivos CSV procesados**

### Punto 9b del TP: ✅  
- ✅ Repetir pasos adecuados para Ingesta2 → **Reutiliza lógica de step_02**
- ✅ Normalización y carga → **Misma función normalize_column_name()**

### Recomendación 12 del TP: ✅
- ✅ Prefijo para temporales → **TMP2_ distingue de TMP_ original**

### Recomendación 18 del TP: ✅
- ✅ SQL estándar → **Solo pandas y SQLite estándar**

## 5. Mejoras sugeridas

### Mejoras de Validación:
1. **Validación de contenido**:
```python
def validate_ingesta2_content(df, table_name):
    # Verificar PKs no nulas
    # Validar que customer_ids/order_ids existen en base
    # Detectar duplicados dentro de Ingesta2
    # Validar formatos de fecha
```

2. **Comparación con datos base**:
```python
def compare_with_existing_data(conn, df, table_name):
    # Detectar registros completamente nuevos
    # Identificar modificaciones a registros existentes  
    # Reportar estadísticas de cambios esperados
```

3. **Validación de business rules**:
```python
def validate_business_rules_ingesta2():
    # Orders deben tener customer_id válido
    # Order_details deben referenciar orders válidas
    # Fechas de orders dentro de rango razonable
```

### Mejoras de Calidad:
4. **Integración con DQM**:
```python
def log_ingesta2_metrics(conn, process_id):
    # Registrar proceso de carga Ingesta2 en DQM
    # Contar registros nuevos vs modificados
    # Estadísticas de calidad por tabla
```

5. **Detección de anomalías**:
```python
def detect_anomalies_ingesta2():
    # Volumen inusual de cambios
    # Patrones extraños en los datos
    # Comparación con tendencias históricas
```

### Mejoras de Performance:
6. **Carga optimizada**:
```python
def optimized_load():
    # Carga en chunks para archivos grandes
    # Índices temporales en PKs
    # Transacciones por archivo
```

7. **Validación de esquemas**:
```python
def validate_schema_compatibility():
    # Verificar que columnas de CSV match tabla destino
    # Validar tipos de datos
    # Reportar columnas faltantes/extra
```

### Mejoras de Monitoring:
8. **Métricas detalladas**:
```python
def generate_ingesta2_report():
    # Archivos procesados vs esperados
    # Registros por archivo
    # Tiempo de procesamiento
    # Errores encontrados
```

9. **Comparación automática**:
```python
def compare_ingesta_volumes():
    # Comparar volumen Ingesta1 vs Ingesta2
    # Detectar patrones inusuales
    # Alertar si diferencias significativas
```

### Mejoras de Configuración:
10. **Configuración externa**:
```python
# Mover a config.yaml
INGESTA2_CONFIG = {
    'path': '.data/ingesta2',
    'expected_files': ['customers - novedades.csv', ...],
    'validation_rules': {...},
    'table_mappings': {...}
}
```

11. **Validación de dependencias**:
```python
def validate_dependencies():
    # Verificar que steps 1-7 corrieron exitosamente
    # Validar que DWH base existe
    # Confirmar que DQM está disponible
```

### Código sugerido para validación de contenido:
```python
def validate_ingesta2_data_quality(conn, df, table_name):
    issues = []
    
    # Verificar PKs críticas
    if table_name == 'TMP2_customers':
        null_customers = df['customer_id'].isnull().sum()
        if null_customers > 0:
            issues.append(f"{null_customers} customer_ids nulos")
    
    # Verificar existencia en datos base
    if table_name == 'TMP2_orders':
        cursor = conn.cursor()
        cursor.execute("SELECT DISTINCT customer_id FROM ING_customers")
        valid_customers = {row[0] for row in cursor.fetchall()}
        
        invalid_customers = set(df['customer_id']) - valid_customers
        if invalid_customers:
            issues.append(f"Customer IDs inválidos: {invalid_customers}")
    
    return issues
```

### Mejora de configuración sugerida:
```yaml
# ingesta2_config.yaml
ingesta2:
  source_path: ".data/ingesta2"
  expected_files:
    - filename: "customers - novedades.csv"
      table: "TMP2_customers"
      required_columns: ["customer_id", "company_name"]
    - filename: "orders - novedades.csv"  
      table: "TMP2_orders"
      required_columns: ["order_id", "customer_id"]
```

## Veredicto Final

**IMPLEMENTACIÓN SÓLIDA** que establece la base para actualizaciones incrementales del DWH. La reutilización de lógica de normalización asegura consistencia, y el aislamiento en TMP2_ es una decisión arquitectural inteligente.

**Fortalezas**:
- Aislamiento inteligente de datos incrementales
- Reutilización de código probado (normalización)
- Compatibilidad de esquemas bien diseñada
- Manejo de errores apropiado

**Área de mejora principal**: Falta integración con DQM y validaciones de contenido, pero esto es apropiado ya que el control detallado ocurre en step_09.

Este paso cumple perfectamente su rol de **preparación de staging** para la actualización incremental del DWH.