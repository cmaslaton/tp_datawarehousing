from tp_datawarehousing.steps import (
    step_01_setup_staging_area,
    step_02_load_staging_data,
    step_03_create_ingestion_layer,
    step_04_link_world_data,
    step_05_create_dwh_model,
    step_06_create_dqm,
    step_07_initial_dwh_load,
    step_08_load_ingesta2_to_staging,
    step_09_update_dwh_with_ingesta2,
    step_10_create_data_product,
)
import logging


def main():
    """
    Orquesta la ejecución de todos los pasos del proceso de DWA.
    """
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
    )

    logging.info("Iniciando el proceso de Data Warehousing.")

    # --- Paso 1: Configurar el área de Staging ---
    logging.info(
        "--- Ejecutando Paso 1: Configuración del área de Staging y Metadatos ---"
    )
    step_01_setup_staging_area.create_database_and_tables()
    logging.info("--- Paso 1: Finalizado ---")

    # --- Paso 2: Cargar datos en Staging ---
    logging.info("--- Ejecutando Paso 2: Carga de datos de Ingesta1 a Staging ---")
    step_02_load_staging_data.load_all_staging_data()
    logging.info("--- Paso 2: Finalizado ---")

    # --- Paso 3: Crear Capa de Ingesta con Integridad ---
    logging.info(
        "--- Ejecutando Paso 3: Creando Capa de Ingesta (ING_) con Integridad ---"
    )
    step_03_create_ingestion_layer.main()
    logging.info("--- Paso 3: Finalizado ---")

    # --- Paso 4: Vincular y Estandarizar Datos de Países ---
    logging.info("--- Ejecutando Paso 4: Vinculando Datos de Países ---")
    step_04_link_world_data.main()
    logging.info("--- Paso 4: Finalizado ---")

    # --- Paso 5: Crear el Modelo Dimensional del DWH ---
    logging.info("--- Ejecutando Paso 5: Creando el Modelo Dimensional (DWH) ---")
    step_05_create_dwh_model.main()
    logging.info("--- Paso 5: Finalizado ---")

    # --- Paso 6: Crear el Modelo del DQM ---
    logging.info("--- Ejecutando Paso 6: Creando el Data Quality Mart (DQM) ---")
    step_06_create_dqm.main()
    logging.info("--- Paso 6: Finalizado ---")

    # --- Paso 7: Carga Inicial del DWH ---
    # Nota: El script del paso 7 corresponde al punto 8 del TP.
    logging.info("--- Ejecutando Paso 7: Carga Inicial del DWH (Punto 8 del TP) ---")
    step_07_initial_dwh_load.main()
    logging.info("--- Paso 7: Finalizado ---")

    # --- Paso 8: Cargar Ingesta2 en Staging ---
    logging.info("--- Ejecutando Paso 8: Cargando Ingesta2 a Staging (TMP2) ---")
    step_08_load_ingesta2_to_staging.main()
    logging.info("--- Paso 8: Finalizado ---")

    # --- Paso 9: Actualizar DWH con Ingesta2 ---
    logging.info("--- Ejecutando Paso 9: Actualizando DWH con Ingesta2 ---")
    step_09_update_dwh_with_ingesta2.main()
    logging.info("--- Paso 9: Finalizado ---")

    # --- Paso 10: Crear Producto de Datos ---
    logging.info("--- Ejecutando Paso 10: Creando Producto de Datos ---")
    step_10_create_data_product.main()
    logging.info("--- Paso 10: Finalizado ---")

    logging.info("Proceso de Data Warehousing finalizado con éxito.")


if __name__ == "__main__":
    main()
