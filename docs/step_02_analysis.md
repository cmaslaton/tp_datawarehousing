# Análisis Paso 2: Load Staging Data (step_02_load_staging_data.py)

## 1. Qué hace el script

Este script ejecuta la **primera carga masiva de datos** desde archivos CSV de Ingesta1 hacia el área de staging. Sus responsabilidades incluyen:

- **Mapeo automático** de archivos CSV a tablas TMP_ 
- **Normalización inteligente** de nombres de columnas con función `normalize_column_name()`
- **Carga robusta con manejo de errores** de encoding (UTF-8 → latin-1 fallback)
- **Limpieza preventiva** de tablas antes de cada carga (DELETE antes de INSERT)
- **Logging detallado** de todo el proceso de ETL

### Archivos procesados:
- **Northwind**: categories, products, suppliers, orders, order_details, customers, employees, shippers, territories, employee_territories, regions
- **External data**: world-data-2023.csv

## 2. Control de calidad de datos

✅ **IMPLEMENTA** controles básicos de calidad:

### Controles Automáticos:
- **Manejo de encoding**: Intenta UTF-8, fallback a latin-1 para archivos problemáticos
- **Validación de archivos**: Verifica existencia de archivos antes de procesarlos
- **Limpieza de datos**: DELETE previo evita duplicados en re-ejecuciones

- |||| **Logging de errores**: Captura y registra errores sin abortar el proceso completo

### Controles de Transformación:
- **Normalización de columnas**: Convierte nombres inconsistentes a snake_case estándar
  - `categoryID` → `category_id`
  - `Density (P/Km2)` → `density`
  - `Capital/Major City` → `capital_major_city`

**Limitación**: No valida contenido de datos (nulos, formatos, rangos).

## 3. Transformación de datos

✅ **REALIZA** transformaciones significativas:

### Transformación Principal - `normalize_column_name()`:
```python
# Ejemplos de transformaciones aplicadas:
"categoryID" → "category_id"
"CompanyName" → "company_name"  
"Density (P/Km2)" → "density"
"Capital/Major City" → "capital_major_city"
"world-data" → "world_data"
```

### Lógica de Normalización:
1. **Limpieza de sufijos**: Remueve `(P/Km2)` y similares
2. **Reemplazo de separadores**: `-`, `/`, `:`, espacios → `_`
3. **CamelCase splitting**: `productName` → `product_name`
4. **Casos especiales**: Diccionario para excepciones conocidas
5. **Cleanup final**: Elimina `__` duplicados

## 4. Cumplimiento con consignas del TP

✅ **CUMPLE COMPLETAMENTE** con las consignas:

### Punto 1 del TP: ✅
- ✅ Analizar tablas CSV → **Análisis implícito mediante normalización**
- ✅ Incluir Ingesta1 → **Todos los archivos CSV mapeados y cargados**

### Punto 4 del TP: ✅  
- ✅ Crear área temporal → **Carga completa en tablas TMP_**
- ✅ Persistir modelo relacional → **Datos de Northwind + World-Data cargados**

### Punto 3 del TP: ✅
- ✅ Considerar tabla de países → **world-data-2023.csv incluido en TMP_world_data_2023**

### Recomendaciones del TP: ✅
- ✅ Usar SQL estándar → **Solo pandas.to_sql() y SQLite estándar**
- ✅ Logging robusto → **Logging comprehensivo implementado**

## 5. Mejoras sugeridas

### Mejoras de Calidad de Datos:
1. **Validación de contenido**: 
```python
def validate_data_quality(df, table_name):
    # Verificar nulos en columnas críticas
    # Validar rangos numéricos
    # Verificar formatos de fecha
    # Detectar outliers
```

2. **Validación de esquemas**: Verificar que columnas del CSV coincidan con tabla destino

3. **Métricas de carga**: Registrar conteos, duplicados detectados, registros rechazados

### Mejoras de Robustez:
4. **Transacciones**: Envolver cada carga en transacción para rollback en caso de error
5. **Validación de dependencias**: Verificar que step_01 se ejecutó exitosamente
6. **Configuración externa**: Mover mapeos a archivo de configuración

### Mejoras de Monitoreo:
7. **Integración con DQM**: Registrar estadísticas de carga en tablas DQM
8. **Checksums**: Verificar integridad de archivos CSV antes de procesar
9. **Progress tracking**: Mostrar progreso para archivos grandes

### Código sugerido para validación:
```python
def validate_critical_columns(df, table_name):
    critical_cols = {
        'TMP_products': ['product_id', 'product_name'],
        'TMP_orders': ['order_id', 'customer_id'],
        'TMP_customers': ['customer_id', 'company_name']
    }
    
    if table_name in critical_cols:
        for col in critical_cols[table_name]:
            null_count = df[col].isnull().sum()
            if null_count > 0:
                logging.warning(f"Tabla {table_name}: {null_count} nulos en columna crítica {col}")
```

## Veredicto Final

**IMPLEMENTACIÓN SÓLIDA Y PROFESIONAL** que demuestra comprensión profunda de ETL. La función de normalización de columnas es especialmente valiosa y muestra atención al detalle. El manejo de errores y logging es ejemplar. Cumple perfectamente con todas las consignas del TP y establece un estándar alto para los pasos siguientes.