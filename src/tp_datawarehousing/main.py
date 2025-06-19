from tp_datawarehousing.steps import (
    step_01_setup_staging_area,
    step_02_load_staging_data,
    step_03_add_foreign_keys,
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

    # --- Paso 3: Añadir Foreign Keys y Verificar Integridad ---
    logging.info(
        "--- Ejecutando Paso 3: Añadiendo Foreign Keys y Verificando Integridad ---"
    )
    step_03_add_foreign_keys.add_foreign_keys_and_verify()
    logging.info("--- Paso 3: Finalizado ---")

    logging.info("Proceso de Data Warehousing finalizado con éxito.")


if __name__ == "__main__":
    main()
