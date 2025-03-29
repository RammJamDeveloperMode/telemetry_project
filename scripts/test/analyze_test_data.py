#!/usr/bin/env python3
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from datetime import datetime
import os

def load_data(data_path):
    """Cargar datos de telemetría y alertas"""
    telemetry_df = pd.read_csv(os.path.join(data_path, 'telemetry_data.csv'))
    alerts_df = pd.read_csv(os.path.join(data_path, 'alerts_data.csv'))
    
    # Convertir timestamps a datetime
    telemetry_df['timestamp'] = pd.to_datetime(telemetry_df['timestamp'])
    alerts_df['timestamp'] = pd.to_datetime(alerts_df['timestamp'])
    
    return telemetry_df, alerts_df

def analyze_telemetry(telemetry_df):
    """Analizar datos de telemetría"""
    print("\n=== Análisis de Datos de Telemetría ===")
    
    # Estadísticas básicas por vehículo
    print("\nEstadísticas por vehículo:")
    stats = telemetry_df.groupby('vehicle_id').agg({
        'engine_temperature': ['mean', 'max', 'min'],
        'speed': ['mean', 'max', 'min'],
        'fuel_level': ['mean', 'max', 'min']
    }).round(2)
    print(stats)
    
    # Crear gráficos
    plt.figure(figsize=(15, 10))
    
    # Temperatura del motor por vehículo
    plt.subplot(2, 2, 1)
    sns.boxplot(data=telemetry_df, x='vehicle_id', y='engine_temperature')
    plt.title('Temperatura del Motor por Vehículo')
    plt.xticks(rotation=45)
    
    # Velocidad por vehículo
    plt.subplot(2, 2, 2)
    sns.boxplot(data=telemetry_df, x='vehicle_id', y='speed')
    plt.title('Velocidad por Vehículo')
    plt.xticks(rotation=45)
    
    # Nivel de combustible por vehículo
    plt.subplot(2, 2, 3)
    sns.boxplot(data=telemetry_df, x='vehicle_id', y='fuel_level')
    plt.title('Nivel de Combustible por Vehículo')
    plt.xticks(rotation=45)
    
    # Distribución de coordenadas GPS
    plt.subplot(2, 2, 4)
    plt.scatter(telemetry_df['longitude'], telemetry_df['latitude'], alpha=0.5)
    plt.title('Distribución de Posiciones GPS')
    plt.xlabel('Longitud')
    plt.ylabel('Latitud')
    
    plt.tight_layout()
    plt.savefig('/Volumes/Data/telemetry_project/test_data/telemetry_analysis.png')
    print("\nGráficos guardados en: /Volumes/Data/telemetry_project/test_data/telemetry_analysis.png")

def analyze_alerts(alerts_df):
    """Analizar datos de alertas"""
    print("\n=== Análisis de Alertas ===")
    
    # Resumen de alertas por tipo
    print("\nAlertas por tipo:")
    alert_counts = alerts_df['alert_type'].value_counts()
    print(alert_counts)
    
    # Alertas por nivel de severidad
    print("\nAlertas por nivel de severidad:")
    severity_counts = alerts_df['severity'].value_counts()
    print(severity_counts)
    
    # Alertas por vehículo
    print("\nAlertas por vehículo:")
    vehicle_counts = alerts_df['vehicle_id'].value_counts()
    print(vehicle_counts)
    
    # Crear gráfico de alertas
    plt.figure(figsize=(12, 6))
    
    # Gráfico de barras de alertas por tipo
    plt.subplot(1, 2, 1)
    alert_counts.plot(kind='bar')
    plt.title('Alertas por Tipo')
    plt.xticks(rotation=45)
    
    # Gráfico de barras de alertas por severidad
    plt.subplot(1, 2, 2)
    severity_counts.plot(kind='bar')
    plt.title('Alertas por Nivel de Severidad')
    plt.xticks(rotation=45)
    
    plt.tight_layout()
    plt.savefig('/Volumes/Data/telemetry_project/test_data/alerts_analysis.png')
    print("\nGráficos guardados en: /Volumes/Data/telemetry_project/test_data/alerts_analysis.png")

def main():
    """Función principal"""
    print("Iniciando análisis de datos de prueba...")
    
    # Cargar datos
    data_path = '/Volumes/Data/telemetry_project/test_data'
    telemetry_df, alerts_df = load_data(data_path)
    
    # Analizar datos
    analyze_telemetry(telemetry_df)
    analyze_alerts(alerts_df)
    
    print("\nAnálisis completado.")

if __name__ == "__main__":
    main() 