"""
Step 08b - Data Remediation: Orquestador de remediación automática de problemas de calidad.

Este paso se ejecuta después del step_08 (carga de Ingesta2) para detectar y remediar
automáticamente los problemas de calidad identificados en el DQM.

Problemas que resuelve:
1. SCD2 Temporal Logic: Fechas de inicio > fin
2. Missing Regions: Clientes sin región asignada  
3. Missing Shipping Data: Datos de envío incompletos

Autor: Claude Code (Data Remediation Rockstar 🎸)
"""

import logging
from datetime import datetime
from tp_datawarehousing.quality_utils import (
    get_process_execution_id,
    update_process_execution,
    log_quality_metric
)
from tp_datawarehousing.data_remediation_utils import (
    fix_scd2_temporal_logic,
    resolve_missing_regions,
    handle_missing_shipping_data,
    create_remediation_report,
    validate_remediation_results,
    RemediationStats,
    # 🚀 NUEVAS FUNCIONES DE REMEDIACIÓN AVANZADA
    advanced_geographic_remediation,
    advanced_contact_data_remediation,
    world_data_enrichment_remediation
)

def main():
    """
    🚀 FUNCIÓN PRINCIPAL DE REMEDIACIÓN AVANZADA DE DATOS
    
    Flujo de remediación EXPANDIDO:
    1. Inicia proceso de ejecución en DQM
    2. Ejecuta correcciones SCD2 (básicas)
    3. Resuelve regiones faltantes (básicas)
    4. Maneja datos de envío incompletos (básicas)
    5. 🌍 NUEVA: Remediación geográfica avanzada
    6. 📧 NUEVA: Remediación de datos de contacto
    7. 🌎 NUEVA: Enriquecimiento de world data
    8. Valida que las correcciones se aplicaron
    9. Genera reporte consolidado AVANZADO
    10. Actualiza estado final en DQM
    """
    
    # Configuración de logging específico para este step
    logging.info("=" * 70)
    logging.info("🔧 INICIANDO PROCESO DE REMEDIACIÓN DE DATOS")
    logging.info("=" * 70)
    
    # Inicializar tracking de ejecución
    execution_id = get_process_execution_id("STEP_08B_DATA_REMEDIATION")
    if execution_id is None:
        logging.error("❌ No se pudo obtener ID de ejecución para el proceso de remediación")
        return False
    
    try:
        # === FASE 1: CORRECCIÓN DE LÓGICA TEMPORAL SCD2 ===
        logging.info("🕐 Fase 1: Corrigiendo problemas de lógica temporal SCD2...")
        scd2_stats = fix_scd2_temporal_logic(execution_id)
        
        if scd2_stats.scd2_fixes > 0:
            logging.info(f"   ✅ Corregidos {scd2_stats.scd2_fixes} registros SCD2")
        else:
            logging.info("   ℹ️ No se encontraron problemas SCD2 que corregir")
        
        # === FASE 2: RESOLUCIÓN DE REGIONES FALTANTES ===
        logging.info("🌍 Fase 2: Resolviendo regiones faltantes...")
        region_stats = resolve_missing_regions(execution_id)
        
        if region_stats.region_fixes > 0:
            logging.info(f"   ✅ Asignadas regiones a {region_stats.region_fixes} clientes")
        else:
            logging.info("   ℹ️ No se encontraron clientes sin región")
        
        # === FASE 3: MANEJO DE DATOS DE ENVÍO FALTANTES ===
        logging.info("📦 Fase 3: Manejando datos de envío incompletos...")
        shipping_stats = handle_missing_shipping_data(execution_id)
        
        if shipping_stats.shipping_fixes > 0:
            logging.info(f"   ✅ Procesadas {shipping_stats.shipping_fixes} órdenes con problemas de envío")
        else:
            logging.info("   ℹ️ No se encontraron problemas de datos de envío")
        
        # === 🚀 FASE 4: REMEDIACIÓN GEOGRÁFICA AVANZADA ===
        logging.info("🌍 Fase 4: Ejecutando remediación geográfica avanzada...")
        geographic_stats = advanced_geographic_remediation(execution_id)
        
        if geographic_stats.get_total_fixes() > 0:
            logging.info(f"   ✅ Remediación geográfica: {geographic_stats.get_total_fixes()} fixes aplicados")
            logging.info(f"      • Regiones geográficas: {geographic_stats.null_geographic_fixes}")
            logging.info(f"      • Propagación ship_region: {geographic_stats.pattern_based_fixes}")
            logging.info(f"      • Fuzzy matching: {geographic_stats.fuzzy_match_fixes}")
        else:
            logging.info("   ℹ️ No se encontraron problemas geográficos que corregir")
        
        # === 📧 FASE 5: REMEDIACIÓN DE DATOS DE CONTACTO ===
        logging.info("📧 Fase 5: Ejecutando remediación de datos de contacto...")
        contact_stats = advanced_contact_data_remediation(execution_id)
        
        if contact_stats.get_total_fixes() > 0:
            logging.info(f"   ✅ Datos de contacto generados: {contact_stats.get_total_fixes()} entradas")
        else:
            logging.info("   ℹ️ No se encontraron datos de contacto faltantes")
        
        # === 🌎 FASE 6: ENRIQUECIMIENTO DE WORLD DATA ===
        logging.info("🌎 Fase 6: Ejecutando enriquecimiento de world data...")
        world_data_stats = world_data_enrichment_remediation(execution_id)
        
        if world_data_stats.get_total_fixes() > 0:
            logging.info(f"   ✅ World data enriquecido: {world_data_stats.get_total_fixes()} estimaciones")
        else:
            logging.info("   ℹ️ No se encontraron oportunidades de enriquecimiento")
        
        # === FASE 7: VALIDACIÓN DE REMEDIACIONES ===
        logging.info("🔍 Fase 7: Validando que las correcciones se aplicaron...")
        validation_passed = validate_remediation_results(execution_id)
        
        if validation_passed:
            logging.info("   ✅ Validación exitosa: Todas las correcciones se aplicaron correctamente")
        else:
            logging.warning("   ⚠️ Validación con advertencias: Revisar logs para detalles")
        
        # === FASE 8: GENERACIÓN DE REPORTE CONSOLIDADO AVANZADO ===
        logging.info("📊 Fase 8: Generando reporte consolidado avanzado...")
        report = create_remediation_report(execution_id, scd2_stats, region_stats, shipping_stats,
                                         geographic_stats, contact_stats, world_data_stats)
        
        # Determinar estado final del proceso EXPANDIDO
        basic_fixes = scd2_stats.scd2_fixes + region_stats.region_fixes + shipping_stats.shipping_fixes
        advanced_fixes = (geographic_stats.get_total_fixes() + contact_stats.get_total_fixes() + 
                         world_data_stats.get_total_fixes())
        total_fixes = basic_fixes + advanced_fixes
        
        if total_fixes > 0:
            final_status = "Exitoso"
            final_comments = f"Remediación AVANZADA completada: {report['summary']['total_fixes_applied']} fixes total. Categorías resueltas: {report['summary']['problem_categories_resolved']}/{report['summary']['problem_categories_total']}. Status: {report['summary']['status']}"
            logging.info(f"🎉 REMEDIACIÓN AVANZADA COMPLETADA EXITOSAMENTE!")
        else:
            final_status = "Exitoso"
            final_comments = "No se detectaron problemas de calidad que requerir remediación"
            logging.info("ℹ️ REMEDIACIÓN COMPLETADA: No se encontraron problemas que corregir")
        
        # Registrar métricas finales
        log_quality_metric(
            execution_id=execution_id,
            nombre_indicador="REMEDIATION_PROCESS_SUMMARY",
            entidad_asociada="DATA_REMEDIATION",
            resultado=report['summary']['status'],
            detalles=f"Total fixes: {total_fixes}. Resolution rate: {report['summary']['resolution_rate_percent']}%"
        )
        
        # === FINALIZACIÓN ===
        update_process_execution(execution_id, final_status, final_comments)
        
        # Reporte final CORREGIDO para el usuario
        logging.info("=" * 70)
        logging.info("📋 RESUMEN FINAL DE REMEDIACIÓN (MÉTRICAS CORREGIDAS):")
        logging.info(f"   📂 Categorías resueltas: {report['summary']['problem_categories_resolved']}/{report['summary']['problem_categories_total']}")
        logging.info(f"   📊 Tasa de resolución por categoría: {report['summary']['resolution_rate_percent']}%")
        logging.info(f"   🔧 Total de fixes aplicados: {report['summary']['total_fixes_applied']}")
        logging.info(f"   📈 Mejora de calidad vs baseline: {report['summary']['data_quality_improvement_percent']}%")
        logging.info(f"   📦 BREAKDOWN DE FIXES:")
        logging.info(f"      • Básicos: {report['breakdown']['basic_fixes']['basic_total']} fixes")
        logging.info(f"        - SCD2: {scd2_stats.scd2_fixes}, Regiones: {region_stats.region_fixes}, Shipping: {shipping_stats.shipping_fixes}")
        logging.info(f"      • Avanzados: {report['breakdown']['advanced_fixes']['advanced_total']} fixes")
        logging.info(f"        - Geográficos: {geographic_stats.get_total_fixes()}, Contacto: {contact_stats.get_total_fixes()}, World data: {world_data_stats.get_total_fixes()}")
        logging.info(f"   ✅ Estado final: {report['summary']['status']}")
        logging.info("=" * 70)
        
        return True
        
    except Exception as e:
        error_msg = f"Error crítico en proceso de remediación: {str(e)}"
        logging.error(f"❌ {error_msg}")
        
        # Registrar error en DQM
        log_quality_metric(
            execution_id=execution_id,
            nombre_indicador="REMEDIATION_PROCESS_ERROR",
            entidad_asociada="DATA_REMEDIATION",
            resultado="CRITICAL_ERROR",
            detalles=error_msg
        )
        
        # Actualizar estado de ejecución
        update_process_execution(execution_id, "Fallido", error_msg)
        return False

def run_remediation_diagnostics():
    """
    Función auxiliar para ejecutar solo diagnósticos sin aplicar correcciones.
    Útil para análisis previo de problemas.
    """
    from tp_datawarehousing.quality_utils import get_db_connection
    
    logging.info("🔍 EJECUTANDO DIAGNÓSTICOS DE REMEDIACIÓN...")
    
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Diagnóstico 1: Problemas SCD2
        cursor.execute("""
            SELECT COUNT(*) FROM DWA_DIM_Clientes 
            WHERE fecha_inicio_validez > fecha_fin_validez
        """)
        scd2_issues = cursor.fetchone()[0]
        logging.info(f"   📅 Problemas SCD2 temporal: {scd2_issues} registros")
        
        # Diagnóstico 2: Regiones faltantes
        cursor.execute("""
            SELECT COUNT(*) FROM TMP2_customers 
            WHERE region IS NULL OR region = ''
        """)
        region_issues = cursor.fetchone()[0]
        logging.info(f"   🌍 Clientes sin región: {region_issues} registros")
        
        # Diagnóstico 3: Datos de envío
        cursor.execute("""
            SELECT 
                SUM(CASE WHEN ship_region IS NULL THEN 1 ELSE 0 END) as sin_region,
                SUM(CASE WHEN ship_postal_code IS NULL THEN 1 ELSE 0 END) as sin_postal,
                SUM(CASE WHEN shipped_date IS NULL THEN 1 ELSE 0 END) as sin_fecha
            FROM TMP2_orders
        """)
        shipping_stats = cursor.fetchone()
        sin_region, sin_postal, sin_fecha = shipping_stats
        logging.info(f"   📦 Órdenes con problemas: región:{sin_region}, postal:{sin_postal}, fecha:{sin_fecha}")
        
        total_issues = scd2_issues + region_issues + sin_region + sin_postal
        logging.info(f"   📊 TOTAL ISSUES DETECTADOS: {total_issues}")
        
        conn.close()
        return total_issues
        
    except Exception as e:
        logging.error(f"Error en diagnósticos: {e}")
        return -1

if __name__ == "__main__":
    """
    Permite ejecutar el step de forma independiente para testing.
    
    Uso:
    python -m tp_datawarehousing.steps.step_08b_data_remediation
    """
    
    # Configurar logging para ejecución independiente
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s"
    )
    
    logging.info("🚀 Ejecutando Step 08b - Data Remediation de forma independiente")
    
    # Ejecutar diagnósticos primero
    issues_detected = run_remediation_diagnostics()
    
    if issues_detected > 0:
        logging.info(f"💊 Se detectaron {issues_detected} problemas. Iniciando remediación...")
        success = main()
        
        if success:
            logging.info("🎉 Remediación completada exitosamente!")
        else:
            logging.error("❌ La remediación falló. Revisar logs para detalles.")
    elif issues_detected == 0:
        logging.info("✅ No se detectaron problemas de calidad. Sistema en buen estado.")
    else:
        logging.error("❌ Error ejecutando diagnósticos.")