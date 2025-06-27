# Análisis Paso 1: Setup Staging Area (step_01_setup_staging_area.py)

## 1. Qué hace el script

Este script establece la **infraestructura inicial** del Data Warehouse. Su responsabilidad principal es:

- **Crear la base de datos SQLite** (`tp_dwa.db`) en el directorio `.data/`
- **Definir y crear todas las tablas del área de staging** (prefijo `TMP_`) basadas en el modelo Northwind
- **Crear la tabla de metadatos** (`MET_entidades`) para gestión de metadata institucional
- **Incluir tabla para datos externos** (`TMP_world_data_2023`) desde el dataset World-Data-2023

### Tablas creadas:
- **TMP_categories, TMP_products, TMP_suppliers** - Entidades maestras de productos
- **TMP_orders, TMP_order_details** - Transacciones de ventas
- **TMP_customers, TMP_employees** - Actores del negocio
- **TMP_shippers, TMP_territories, TMP_regions** - Logística y geografía
- **TMP_employee_territories** - Relación empleados-territorios
- **TMP_world_data_2023** - Datos enriquecidos de países
- **MET_entidades** - Metadata institucional

## 2. No hay control de calidad ni tranaformación de datos, ya que en este paso solo define la estructura.

## 3. Transformación de datos

## 4. Cumplimiento con consignas del TP

✅ **CUMPLE COMPLETAMENTE** con las consignas:

### Punto 4 del TP: ✅
- ✅ Crear área temporal → **Implementado con tablas TMP_**
- ✅ Persistir modelo relacional → **Esquema Northwind completo** <<<<<<< ver que es esto

### Punto 5 del TP: ✅
- ✅ Crear soporte para Metadata → **Tabla MET_entidades implementada**
- ✅ Describir entidades → **Estructura preparada para descripciones**

### Recomendación 12a del TP: ✅
- ✅ Prefijo TMP_ para temporales → **Implementado consistentemente**

