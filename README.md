# tp_datawarehousing

Este proyecto contiene la resolución del Trabajo Práctico de Data Warehousing.

## Principios de Desarrollo

Este proyecto se adhiere a un conjunto de principios de desarrollo profesional para garantizar un código de alta calidad, mantenible y escalable.

### 1. Estructura de Proyecto Profesional

Se sigue una estructura de directorios basada en las mejores prácticas de la comunidad de Python, separando el código fuente (`src`), los datos (`.data`) y la documentación (`.docs`). Esto mejora la claridad y facilita la instalación del paquete.

```
tp_datawarehousing/
├── .data/
├── .docs/
├── src/
│   └── tp_datawarehousing/
│       ├── __init__.py
│       ├── main.py
│       └── steps/
│           └── ...
├── pyproject.toml
└── README.md
```

### 2. Desarrollo Modular y Orquestación

- **Modularidad:** Cada paso lógico y discreto del proceso de ETL (Extracción, Transformación, Carga) se implementará en su propio script de Python, ubicado en el directorio `src/tp_datawarehousing/steps/`.
- **Orquestación:** El script `src/tp_datawarehousing/main.py` actuará como el orquestador principal. Será responsable de llamar a cada paso modular en la secuencia correcta, controlando el flujo completo del proceso.

### 3. Gestión de Dependencias

Todas las dependencias del proyecto se gestionan a través del archivo `pyproject.toml`, siguiendo el estándar de PEP 621. Se recomienda encarecidamente el uso de un entorno virtual.

### 4. Gestión de Configuración

Se evitará el uso de valores "hardcodeados" (como credenciales o rutas de archivos) en el código. En su lugar, se favorecerá el uso de archivos de configuración o variables de entorno para gestionar estos parámetros, haciendo que el proyecto sea más seguro y portable.

### 5. Logging y Manejo de Errores

Se implementará un sistema de `logging` robusto para registrar los eventos clave, advertencias y errores durante la ejecución. Esto es crucial para la monitorización, la depuración y el mantenimiento del flujo de datos.

# Comandos

pip install -e .
python -m tp_datawarehousing.main