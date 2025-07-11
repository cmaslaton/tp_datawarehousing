El objetivo de este trabajo práctico es desarrollar todas las capas de datos y ejecutar los procesos correspondientes
del flujo end-to-end en un DWA (Data Warehouse Analítico), desde la adquisición hasta la publicación y la explotación.
El material básico para la elaboración

# Aclaraciones 

• Este trabajo debe elaborarse por equipos.
• La cantidad optima de integrantes por equipo será publicada en la plataforma.
• Los equipos con más integrantes que la cantidad optima serán penalizados.
• La entrega de este TP consiste en publicar un documento resumiendo lo realizado según se especifica más
abajo.
• Además se deben entregar todos los componentes desarrollados.
• Cada grupo deberá exponer en clase una síntesis del trabajo realizado con una duración máxima de 10'
Podría reemplazarse con un video excepcionalmente.
• Las fechas de publicación y presentación serán indicadas en la plataforma
• Incluyan en los archivos a entregar la lista de los integrantes. Se recomienda considerar una carátula en
donde se identifique el grado/posgrado, la cohorte, la materia, el título del informe, los integrantes del
equipo y la fecha.
• La evaluación se realizará según la rúbrica descrita más abajo.
• Los integrantes de cada equipo obtendrán la misma calificación.
• Los docentes evaluarán el trabajo realizado por lo que se manifiesta en la presentación y en los documentos
entregados, por lo tanto se recomienda una elaboración cuidada y comentada. El contenido debe transmitir
las tareas realizadas con la especificidad suficiente para comprenderlas pero sin entrar en detalles
irrelevantes. Es bueno comentar sintéticamente los problemas o contratiempos que hayan enfrentado.
• No copien textos externos, si fuera necesario, citen la fuente.

Contexto general
Se publicarán dos conjuntos de datasets provenientes de una base de datos transaccional y de otras fuentes
secundarias.
1. Ingesta1: corresponde a los datos de una ingesta inicial para alimentar un DWA vacío. Los datos fueron
obtenidos de un sistema transaccional persistidos en un modelo relacional tradicional y otro dataset externo.
2. Ingesta2: corresponde a un subconjunto de la misma entidad de datos que se utilizará para una actualización
posterior.
Se deberán desarrollar todas las capas y procesos necesarios para implementar el flujo de datos dentro del DWA para
proveer de información a la organización. El objetivo final es desarrollar un tablero de visualización a partir de los
datos persistidos en el DWA. Se deben incluir los controles de calidad necesarios, la memoria institucional, el
enriquecimiento de los datos, la gestión de la metadata, la publicación de productos de datos y de aplicaciones de
datos.
A modo ilustrativo pero no exhaustivo, en la siguiente imagen se muestra el DER de la base transaccional.

[DER]

# Descripción detallada

Desarrollar el flujo de datos de un DWA.
Toda la implementación se debe desarrollar en una base de datos relacional utilizando comandos SQL estándares.

Se pide:

# Adquisición

✅ 1) Analizar las tablas (.CSV) incluidas en Ingesta1. `Realizado implícitamente en el script step_02, donde se normalizaron columnas para la carga.`
✅ 2) Comparar la estructura de las tablas y el modelo de entidad relación. Adecuar si fuera necesario. Definir y
crear las FOREIGN-KEYS necesarias para verificar la integridad referencial. `Realizado en el script step_03_add_foreign_keys.py`
✅ 3) Considerar también la tabla de países (World-Data-2023) y vincularla con las tablas que correspondan. `Realizado en el script step_04, estandarizando nombres de países para permitir la vinculación.`
✅ 4) Crear un área temporal y persistir el modelo relacional obtenido con los datos de los .CSV. `Realizado en los scripts step_01 (creación de estructura) y step_02 (carga de datos).`
✅ 5) Crear el soporte para la Metadata y utilizarlo para describir las entidades. `Realizado en el script step_01_setup_staging_area.py`

# Ingeniería

✅ 6) Definir y crear el Modelo Dimensional del DWA y documentarlo en la Metadata. Debe incluir una capa de
Memoria y una de Enriquecimiento (datos derivados). `Realizado en el script step_05_create_dwh_model.py, donde se define el esquema en estrella y se registran las tablas en la metadata.`
✅ 7) Diseñar y crear el DQM para poder persistir los procesos ejecutados sobre el DWA, los descriptivos de cada
entidad procesada y los indicadores de calidad. Documentar el diseño en la Metadata. `Realizado en el script step_06_create_dqm.py, donde se definen las tablas para el monitoreo de calidad y se registran en la metadata.`
✅ 8) Realizar la carga inicial del DWA con los datos que se seleccionen de las tablas recibidas y procesadas.
`Realizado en el script step_07_initial_dwh_load.py. Este script orquesta la carga completa y los controles de calidad.`
    a) Definir los controles de calidad de ingesta para cada tabla, los datos que se persistirán en el DQM y
    los indicadores y límites para aceptar o rechazar los datasets. Realizar y ejecutar los scripts
    correspondientes. Tener en cuenta: outliers, datos faltantes, valores que no respetan los formatos,
    etc. `Realizado en la función perform_ingestion_quality_checks, que valida nulos en PKs y valores negativos. Si falla, aborta la carga. Los resultados se persisten en DQM_indicadores_calidad.`
    b) Definir los controles de calidad de integración para el conjunto de tablas, los datos que se persistirán
    en el DQM y los indicadores y límites para aceptar o rechazar los datasets. Realizar y ejecutar los scripts
    correspondientes. Tener en cuenta: la integridad referencial e indicadores de comparación. `Realizado en la función perform_integration_quality_checks, que verifica SKs nulas en la tabla de hechos y compara conteos de filas. Los resultados se persisten en el DQM.`
    c) Ingestar los datos de Ingesta1 en el DWA definido. Las datos se deben insertar desde las tablas
    temporales creadas. Actualizar todas las capas. Siempre y cuando se superen los umbrales de calidad. `Realizado en el cuerpo principal de step_07_initial_dwh_load.py, que invoca las funciones de carga solo si los controles de ingesta (8a) son exitosos.`
✅ 9) Actualización: `Realizada en los scripts step_08 (carga de Ingesta2 en un staging temporal TMP2_) y step_09 (orquestador de la actualización).`
    a) Persistir en área temporal las tablas entregadas como Ingesta2. `Realizado en step_08_load_ingesta2_to_staging.py.`
    b) Repetir los pasos definidos para Ingesta1 que sean adecuados para Ingesta2. `Realizado en step_08 y step_09, incluyendo normalización, carga y registro en DQM.`
    c) Considerar altas, bajas y modificaciones. Tener en cuenta el orden de prevalencia para las actualizaciones. `Realizado en step_09: Se implementa SCD Tipo 2 para Clientes (altas y modificaciones con historia) y se actualizan/insertan hechos en la tabla de ventas.`
    d) Si hubiera errores se debe decidir si se cancela toda la actualización, se procesa en parte o en su totalidad.
    Lo que suceda debe quedar registrado en el DQM. `Realizado: el script step_09 registra el estado final (Exitoso/Fallido) y las métricas en el DQM.`
    e) Se debe considerar además la capa de Memoria para persistir la historia de los campos que han sido
    modificados. `Realizado en step_09 con la implementación de SCD Tipo 2 para la dimensión de clientes.`
    f) Se debe considerar además actualizar la capa de Enriquecimiento para persistir los datos derivados que
    se vean afectados. `Realizado implícitamente al actualizar/insertar nuevos hechos con métricas derivadas como monto_total.`
    g) Desarrollar y ejecutar los scripts correspondientes para actualizar el DWA con los nuevos datos. `Realizado.`
    h) Actualizar el DQM si fuera necesario. `Realizado.`
    i) Actualizar la Metadata si fuera necesario. `No fue necesario para la actualización.`

# Publicación

✅ 10) Publicar un producto de datos resultante del DWA para un caso de negocio particular y un período dado si
corresponde. `Realizado en el script step_10_create_data_product.py.`
    a) Desarrollar y ejecutar los scripts necesarios. `Realizado. Se crea la tabla agregada DP_Ventas_Mensuales_Categoria_Pais.`
    b) Dejar huella en el DQM. `Realizado. La ejecución del proceso se registra en DQM_ejecucion_procesos.`
    c) Dejar huella en la Metadata de ser necesario. `Realizado. La nueva tabla se registra en MET_entidades.`
11) Explotación
a) Desarrollar y publicar un tablero para la visualización del producto de datos desarrollado. Dejar huella en
el DQM y en Metadata de ser necesario.
b) Desarrollar y publicar un tablero de visualización que permita navegar por los datos persistidos en el DQM.
Dejar huella en el DQM y en Metadata de ser necesario.

# Recomendaciones

12) Se puede utilizar un único esquema de base de datos para todas las capas. Se recomienda identificar las
distintas capas con un prefijo, por ejemplo:
a) TMP_ para temporales para la validación de ingesta.
b) ING_ para la capa temporal a ingestar.
c) DWA_ para el Datawarehouse.
d) DQM_ para el Data Quality Mart.
e) DWM_ para la memoria.
f) MET_ para la metadata.
g) DPxx_ para los productos de datos.
13) En https://en.wikiversity.org/wiki/Database_Examples/Northwind/SQLite tienen algunas ayudas para crear
las tablas.
14) En todo control de calidad se deben detectar los errores, faltantes o inconsistencias y describir el proceso que
se llevaría adelante para corregirlos. Los indicadores de calidad deberán permitir decidir si la entidad se
procesa o no, completa o parcialmente.
15) El DQM debe persistir los indicadores que sirvan para determinar la calidad de los datos procesados y una
estadística que permita describir cuantitativamente al conjunto.
16) No es necesario pasar al DWA todos los atributos de las entidades originales, decidan cuáles son importantes
y justifiquen.
17) Sean prolijos y explícitos al codificar los scripts y documenten en el mismo fuente.
18) Este es un TP para una materia de DW, por lo tanto el foco debe estar puesto en los conceptos fundamentales
de esta disciplina. El uso de la BD es solo una herramienta para gestionar el DWA. Existen múltiples
herramientas para realizar los procesos solicitados, pero en este caso se pide realizarlos utilizando solo SQL
estándar.
19) Se prefiere un trabajo simple, que cubra todos los aspectos pero no necesariamente exhaustivo en los detalles
y, por supuesto, bien hecho.
20) Lo que no esté especificado y sea necesario para el trabajo, decídanlo y justifíquenlo.
21) Se recomienda usar SQLite pero no es obligatorio, pueden usar cualquier base SQL. Si usan SQLite se
recomienda utilizar también SQLiteStudio.
22) Para construir tableros se puede utilizar Power-BI Desktop u otros que conozcan (particularmente si quieren
verlo en IOS).

# Resultado esperado

Informe y presentación exponiendo:
1. Entrega de un informe y/o presentación (.PDF/.PPTX) con un resumen de lo realizado. Esto permitirá evaluar el
resultado sin necesariamente abrir ningún entorno de base de datos.
2. Se deben incluir como anexos todos los scripts desarrollados, los DER y estructuras correspondientes.
3. Entregar como .ZIP la base resultante con todos los componentes (.db, .sql, etc. y los tableros) para verificación
de autoría si fuera necesario.
4. Entregar el tablero desarrollado (por ejemplo, Tablero.PBIX).
5. En la presentación en clase deberán ejecutar los tableros desarrollados.
6. Salvo el informe/presentación que debe ser publicado en el aula virtual, los demás objetos pueden ser publicados
en un drive con libre acceso.