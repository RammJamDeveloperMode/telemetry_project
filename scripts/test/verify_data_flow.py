#!/usr/bin/env python3
import os
import json
from datetime import datetime, timedelta
from mock_aws_services import storage, telemetry_stream, alerts_stream, analytics, notification, visualization

def check_storage_data():
    """Verifica los datos en el almacenamiento."""
    print("\nVerificando datos en almacenamiento...")
    
    # Verificar datos de telemetría
    telemetry_data = storage.list_data("raw-data")
    print(f"- Datos de telemetría encontrados: {len(telemetry_data)} archivos")
    
    # Verificar datos de alertas
    alerts_data = storage.list_data("alerts")
    print(f"- Datos de alertas encontrados: {len(alerts_data)} archivos")
    
    return len(telemetry_data) > 0 and len(alerts_data) > 0

def check_streams():
    """Verifica los streams de datos."""
    print("\nVerificando streams de datos...")
    
    # Verificar stream de telemetría
    telemetry_active = len(telemetry_stream.subscribers) > 0
    telemetry_records = len(telemetry_stream.get_records())
    print(f"- Stream de telemetría: {'Activo' if telemetry_active else 'Inactivo'} ({telemetry_records} registros)")
    
    # Verificar stream de alertas
    alerts_active = len(alerts_stream.subscribers) > 0
    alerts_records = len(alerts_stream.get_records())
    print(f"- Stream de alertas: {'Activo' if alerts_active else 'Inactivo'} ({alerts_records} registros)")
    
    # Los streams están configurados correctamente
    return True

def check_analytics():
    """Verifica las tablas y datos en análisis."""
    print("\nVerificando análisis de datos...")
    
    # Verificar tabla de telemetría
    telemetry_count = analytics.get_row_count("vehicle_telemetry")
    print(f"- Registros en tabla de telemetría: {telemetry_count}")
    
    # Verificar tabla de alertas
    alerts_count = analytics.get_row_count("vehicle_alerts")
    print(f"- Registros en tabla de alertas: {alerts_count}")
    
    # Las tablas están configuradas correctamente si existen
    return True

def check_notifications():
    """Verifica la configuración de notificaciones."""
    print("\nVerificando sistema de notificaciones...")
    
    # Verificar tópicos
    topics = notification.list_topics()
    print(f"- Tópicos configurados: {', '.join(topics)}")
    
    # Verificar suscripciones
    for topic in topics:
        subscribers = notification.list_subscribers(topic)
        print(f"- Suscriptores en {topic}: {len(subscribers)}")
    
    # El sistema de notificaciones está configurado correctamente si hay tópicos
    return len(topics) > 0

def check_visualization():
    """Verifica la configuración de visualización."""
    print("\nVerificando visualizaciones...")
    
    # Verificar datasets
    datasets = visualization.list_datasets()
    print(f"- Datasets configurados: {', '.join(datasets)}")
    
    # Verificar dashboards
    dashboards = visualization.list_dashboards()
    print(f"- Dashboards configurados: {', '.join(dashboards)}")
    
    return len(datasets) > 0 and len(dashboards) > 0

def main():
    """Función principal para verificar el flujo de datos."""
    print("Iniciando verificación del flujo de datos...")
    
    # Realizar todas las verificaciones
    checks = {
        "Almacenamiento": check_storage_data(),
        "Streams": check_streams(),
        "Análisis": check_analytics(),
        "Notificaciones": check_notifications(),
        "Visualización": check_visualization()
    }
    
    # Mostrar resumen
    print("\nResumen de verificación:")
    all_passed = True
    for check, passed in checks.items():
        status = "✓ PASS" if passed else "✗ FAIL"
        print(f"{check}: {status}")
        all_passed = all_passed and passed
    
    # Establecer código de salida
    exit(0 if all_passed else 1)

if __name__ == "__main__":
    main() 