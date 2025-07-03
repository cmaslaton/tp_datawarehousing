# Data Warehousing Project

Un proyecto de ETL (Extracción, Transformación, Carga) desarrollado como trabajo práctico de Data Warehousing, implementado siguiendo principios de desarrollo profesional y arquitectura modular.

## 📋 Tabla de Contenidos

- [Características](#características)
- [Requisitos](#requisitos)
- [Instalación](#instalación)
- [Uso](#uso)
- [Estructura del Proyecto](#estructura-del-proyecto)
- [Principios de Desarrollo](#principios-de-desarrollo)

## ✨ Características

- **Arquitectura Modular**: Cada paso del proceso ETL está implementado como un módulo independiente
- **Orquestación Centralizada**: Control de flujo unificado a través del módulo principal
- **Gestión de Configuración**: Uso de archivos de configuración y variables de entorno
- **Logging Robusto**: Sistema de registro completo para monitoreo y depuración
- **Estructura Profesional**: Organización basada en estándares de la comunidad Python

## 🔧 Requisitos

- Python 3.8 o superior
- pip (gestor de paquetes de Python)

## 📦 Instalación

1. **Clonar el repositorio**:
   ```bash
   git clone https://github.com/usuario/tp_datawarehousing.git
   cd tp_datawarehousing
   ```

2. **Crear un entorno virtual** (recomendado):
   ```bash
   python -m venv venv
   source venv/bin/activate  # En Windows: venv\Scripts\activate
   ```

3. **Instalar el proyecto en modo desarrollo**:
   ```bash
   pip install -e .
   ```

## 🚀 Uso

### Ejecución del Pipeline Completo

```bash
python -m tp_datawarehousing.main
```

### Opciones de Configuración

El proyecto utiliza archivos de configuración para gestionar parámetros. Asegúrate de configurar las variables de entorno necesarias antes de ejecutar el proyecto.

## 📁 Estructura del Proyecto

```
tp_datawarehousing/
├── .data/                    # Datos del proyecto
├── .docs/                    # Documentación
├── src/
│   └── tp_datawarehousing/
│       ├── __init__.py
│       ├── main.py          # Orquestador principal
│       └── steps/           # Módulos ETL individuales
│           └── ...
├── pyproject.toml           # Configuración del proyecto
└── README.md               # Este archivo
```

## 🏗️ Principios de Desarrollo

### 1. Modularidad y Separación de Responsabilidades
- Cada paso del proceso ETL está implementado como un módulo independiente
- Facilita el mantenimiento, testing y reutilización de código

### 2. Orquestación Centralizada
- El módulo `main.py` coordina la ejecución de todos los pasos
- Control de flujo claro y predecible

### 3. Gestión de Configuración
- Sin valores hardcodeados en el código
- Uso de archivos de configuración y variables de entorno
- Mayor seguridad y portabilidad

### 4. Logging y Monitoreo
- Sistema de logging robusto para seguimiento de ejecución
- Registro de eventos, advertencias y errores
- Facilita la depuración y el mantenimiento

### 5. Estándares de Calidad
- Estructura de proyecto siguiendo PEP 621
- Gestión de dependencias a través de `pyproject.toml`
- Código limpio y documentado

---

**Desarrollado como trabajo práctico d Introducción a Data Warehousing**