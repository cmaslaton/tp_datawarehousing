#!/usr/bin/env python3
"""
Script para revisar las métricas de calidad del DQM.
Muestra los indicadores de calidad registrados durante la ejecución del pipeline.
"""

import sqlite3
import pandas as pd
from datetime import datetime

DB_PATH = "db/tp_dwa.db"

def show_quality_metrics():
    """Muestra las métricas de calidad registradas en el DQM."""
    
    try:
        conn = sqlite3.connect(DB_PATH)
        
        print("=" * 80)
        print("📊 RESUMEN DE MÉTRICAS DE CALIDAD - DATA WAREHOUSE")
        print("=" * 80)
        
        # Procesos ejecutados
        print("\n🔄 PROCESOS EJECUTADOS:")
        query_procesos = """
        SELECT 
            id_ejecucion,
            nombre_proceso,
            fecha_inicio,
            fecha_fin,
            estado,
            duracion_seg,
            comentarios
        FROM DQM_ejecucion_procesos 
        ORDER BY fecha_inicio DESC
        """
        
        df_procesos = pd.read_sql_query(query_procesos, conn)
        if not df_procesos.empty:
            for _, row in df_procesos.iterrows():
                status_icon = "✅" if row['estado'] == "Exitoso" else "❌" if row['estado'] == "Fallido" else "⏳"
                duracion = f"{row['duracion_seg']:.1f}s" if row['duracion_seg'] else "N/A"
                print(f"{status_icon} {row['nombre_proceso']} - {row['estado']} ({duracion})")
                if row['comentarios']:
                    print(f"   💬 {row['comentarios']}")
        else:
            print("No hay procesos registrados")
            
        # Indicadores de calidad por categoría
        print("\n🎯 INDICADORES DE CALIDAD:")
        query_indicadores = """
        SELECT 
            i.nombre_indicador,
            i.entidad_asociada,
            i.resultado,
            i.detalles,
            p.nombre_proceso
        FROM DQM_indicadores_calidad i
        JOIN DQM_ejecucion_procesos p ON i.id_ejecucion = p.id_ejecucion
        ORDER BY p.fecha_inicio DESC, i.nombre_indicador
        """
        
        df_indicadores = pd.read_sql_query(query_indicadores, conn)
        if not df_indicadores.empty:
            # Agrupar por tipo de indicador
            grupos = df_indicadores.groupby('nombre_indicador')
            
            for indicador, grupo in grupos:
                print(f"\n📋 {indicador}:")
                for _, row in grupo.iterrows():
                    resultado_icon = "✅" if row['resultado'] == "PASS" else "❌" if row['resultado'] == "FAIL" else "⚠️" if row['resultado'] == "WARNING" else "ℹ️"
                    print(f"  {resultado_icon} {row['entidad_asociada']}: {row['resultado']}")
                    if row['detalles']:
                        print(f"     {row['detalles']}")
        else:
            print("No hay indicadores de calidad registrados")
            
        # Resumen por estado
        print("\n📈 RESUMEN POR ESTADO:")
        query_resumen = """
        SELECT 
            resultado,
            COUNT(*) as cantidad,
            ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM DQM_indicadores_calidad), 1) as porcentaje
        FROM DQM_indicadores_calidad 
        GROUP BY resultado
        ORDER BY cantidad DESC
        """
        
        df_resumen = pd.read_sql_query(query_resumen, conn)
        if not df_resumen.empty:
            for _, row in df_resumen.iterrows():
                icon = "✅" if row['resultado'] == "PASS" else "❌" if row['resultado'] == "FAIL" else "⚠️" if row['resultado'] == "WARNING" else "ℹ️"
                print(f"{icon} {row['resultado']}: {row['cantidad']} indicadores ({row['porcentaje']}%)")
        
        # Estadísticas por entidad
        print("\n🏗️ ESTADÍSTICAS POR ENTIDAD:")
        query_entidades = """
        SELECT 
            entidad_asociada,
            COUNT(*) as total_checks,
            SUM(CASE WHEN resultado = 'PASS' THEN 1 ELSE 0 END) as passed,
            SUM(CASE WHEN resultado = 'FAIL' THEN 1 ELSE 0 END) as failed,
            SUM(CASE WHEN resultado = 'WARNING' THEN 1 ELSE 0 END) as warnings
        FROM DQM_indicadores_calidad 
        GROUP BY entidad_asociada
        HAVING total_checks > 1
        ORDER BY total_checks DESC
        """
        
        df_entidades = pd.read_sql_query(query_entidades, conn)
        if not df_entidades.empty:
            for _, row in df_entidades.iterrows():
                pass_rate = (row['passed'] / row['total_checks']) * 100
                status = "🟢" if pass_rate == 100 else "🟡" if pass_rate >= 80 else "🔴"
                print(f"{status} {row['entidad_asociada']}: {row['passed']}/{row['total_checks']} checks pasaron ({pass_rate:.1f}%)")
        
        print("\n" + "=" * 80)
        print(f"📅 Reporte generado: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("=" * 80)
        
    except sqlite3.Error as e:
        print(f"❌ Error accediendo a la base de datos: {e}")
    except Exception as e:
        print(f"❌ Error: {e}")
    finally:
        if 'conn' in locals():
            conn.close()

if __name__ == "__main__":
    show_quality_metrics()