#!/usr/bin/env python3
"""
Script de optimización de base de datos para resolver bloqueos y mejorar rendimiento.
Usar cuando hay problemas de "database is locked" persistentes.

Uso:
    python optimize_database.py
"""

import sys
import os
import logging
from pathlib import Path

# Agregar src al path
sys.path.append('src')

from tp_datawarehousing.quality_utils import (
    optimize_database, 
    force_wal_checkpoint,
    get_db_connection,
    DB_PATH
)

# Configurar logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

def check_database_status():
    """Verifica el estado actual de la base de datos."""
    try:
        conn = get_db_connection(timeout=10.0)
        cursor = conn.cursor()
        
        # Información básica
        cursor.execute("PRAGMA journal_mode")
        journal_mode = cursor.fetchone()[0]
        
        cursor.execute("PRAGMA busy_timeout")
        busy_timeout = cursor.fetchone()[0]
        
        cursor.execute("PRAGMA cache_size")
        cache_size = cursor.fetchone()[0]
        
        cursor.execute("PRAGMA page_count")
        page_count = cursor.fetchone()[0]
        
        cursor.execute("PRAGMA freelist_count")
        free_pages = cursor.fetchone()[0]
        
        # Estado del WAL
        cursor.execute("PRAGMA wal_checkpoint")
        wal_info = cursor.fetchone()
        
        conn.close()
        
        print("=" * 60)
        print("📊 ESTADO ACTUAL DE LA BASE DE DATOS")
        print("=" * 60)
        print(f"📁 Archivo: {DB_PATH}")
        print(f"📋 Journal Mode: {journal_mode}")
        print(f"⏱️  Busy Timeout: {busy_timeout}ms")
        print(f"💾 Cache Size: {cache_size} páginas")
        print(f"📄 Páginas totales: {page_count}")
        print(f"🆓 Páginas libres: {free_pages}")
        if wal_info:
            print(f"📝 WAL Info: {wal_info}")
        
        # Calcular tamaño de archivo
        if os.path.exists(DB_PATH):
            size_mb = os.path.getsize(DB_PATH) / (1024 * 1024)
            print(f"📦 Tamaño archivo: {size_mb:.2f} MB")
        
        return True
        
    except Exception as e:
        print(f"❌ Error verificando estado: {e}")
        return False

def force_unlock_database():
    """Intenta forzar el desbloqueo de la base de datos."""
    print("\n🔓 Intentando desbloquear base de datos...")
    
    try:
        # 1. Checkpoint WAL forzado
        print("1️⃣ Haciendo checkpoint WAL...")
        if force_wal_checkpoint():
            print("   ✅ WAL checkpoint exitoso")
        else:
            print("   ⚠️ WAL checkpoint falló")
        
        # 2. Optimización completa
        print("2️⃣ Optimizando base de datos...")
        if optimize_database():
            print("   ✅ Optimización exitosa")
        else:
            print("   ⚠️ Optimización falló")
        
        # 3. Verificación post-optimización
        print("3️⃣ Verificando estado post-optimización...")
        if check_database_status():
            print("   ✅ Verificación exitosa")
        
        return True
        
    except Exception as e:
        print(f"   ❌ Error en desbloqueo: {e}")
        return False

def main():
    """Función principal del script."""
    print("🔧 OPTIMIZADOR DE BASE DE DATOS SQLite")
    print("=" * 60)
    
    # Verificar que existe la base de datos
    if not os.path.exists(DB_PATH):
        print(f"❌ Base de datos no encontrada: {DB_PATH}")
        return 1
    
    # 1. Estado inicial
    print("1️⃣ Verificando estado inicial...")
    check_database_status()
    
    # 2. Optimización
    print("\n2️⃣ Iniciando optimización...")
    if force_unlock_database():
        print("\n✅ OPTIMIZACIÓN COMPLETADA")
        print("\n💡 Recomendaciones:")
        print("   • Ejecuta este script si vuelves a tener bloqueos")
        print("   • Considera ejecutarlo antes de pipelines largos")
        print("   • Evita ejecutar múltiples procesos simultáneamente")
    else:
        print("\n❌ OPTIMIZACIÓN FALLÓ")
        print("\n🔧 Soluciones adicionales:")
        print("   • Asegúrate de que no hay otros procesos usando la BD")
        print("   • Reinicia el proceso que está causando bloqueos")
        print("   • Como último recurso, elimina archivos .wal y .shm")
    
    return 0

if __name__ == "__main__":
    sys.exit(main())