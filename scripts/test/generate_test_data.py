#!/usr/bin/env python3
import json
import random
import time
from datetime import datetime, timezone
from kafka import KafkaProducer
import logging
import argparse

# Configuración de logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class VehicleTelemetryGenerator:
    def __init__(self, bootstrap_servers, topic):
        """Inicializa el generador de datos de telemetría."""
        self.producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        self.topic = topic
        self.vehicle_ids = [f"VIN-{i:09d}" for i in range(1, 1001)]  # 1000 vehículos
        self.events_per_second = 1000  # Objetivo de eventos por segundo
        self.last_event_time = {}  # Para controlar la frecuencia de eventos por vehículo

    def generate_telemetry(self):
        """Genera un registro de telemetría para un vehículo."""
        vehicle_id = random.choice(self.vehicle_ids)
        current_time = datetime.now(timezone.utc)
        
        # Control de frecuencia de eventos por vehículo
        if vehicle_id in self.last_event_time:
            time_diff = (current_time - self.last_event_time[vehicle_id]).total_seconds()
            if time_diff < 0.1:  # Mínimo 100ms entre eventos del mismo vehículo
                return None
        
        self.last_event_time[vehicle_id] = current_time
        timestamp = current_time.isoformat()
        
        # Generar datos realistas con patrones más naturales
        base_temp = random.uniform(80, 95)  # Temperatura base del motor
        if random.random() < 0.05:  # 5% de probabilidad de temperatura alta
            base_temp = random.uniform(95, 110)

        # Simular aceleración y desaceleración
        base_speed = random.uniform(0, 130)
        if random.random() < 0.03:  # 3% de probabilidad de exceso de velocidad
            base_speed = random.uniform(130, 150)

        # Simular consumo de combustible más realista
        base_fuel = random.uniform(10, 100)
        if random.random() < 0.02:  # 2% de probabilidad de nivel bajo
            base_fuel = random.uniform(5, 20)

        # Coordenadas de ejemplo (Ciudad de México) con movimiento más realista
        base_lat = 19.4326
        base_lon = -99.1332
        movement = random.uniform(-0.001, 0.001)  # Movimiento más suave
        latitude = base_lat + movement
        longitude = base_lon + movement

        return {
            "vehicle_id": vehicle_id,
            "timestamp": timestamp,
            "engine_temperature": round(base_temp, 1),
            "vehicle_speed": round(base_speed, 1),
            "fuel_level": round(base_fuel, 1),
            "brake_status": random.choice([True, False]),
            "location": {
                "latitude": round(latitude, 4),
                "longitude": round(longitude, 4)
            }
        }

    def run(self, duration_seconds=None):
        """Ejecuta el generador de datos."""
        logger.info(f"Iniciando generación de datos de telemetría para {len(self.vehicle_ids)} vehículos")
        logger.info(f"Objetivo: {self.events_per_second} eventos por segundo")
        
        start_time = time.time()
        events_sent = 0
        last_report_time = start_time
        
        try:
            while True:
                # Generar y enviar datos
                telemetry = self.generate_telemetry()
                if telemetry:
                    self.producer.send(self.topic, telemetry)
                    events_sent += 1
                
                # Control de velocidad de generación
                current_time = time.time()
                elapsed = current_time - start_time
                target_events = int(self.events_per_second * elapsed)
                
                if events_sent >= target_events:
                    time.sleep(0.001)  # Pequeña pausa para controlar la velocidad
                
                # Reporte de métricas cada segundo
                if current_time - last_report_time >= 1.0:
                    current_rate = events_sent / elapsed
                    logger.info(f"Eventos enviados: {events_sent}, Tasa actual: {current_rate:.2f} eventos/segundo")
                    last_report_time = current_time
                
                # Verificar duración si se especificó
                if duration_seconds and elapsed >= duration_seconds:
                    break
        
        except KeyboardInterrupt:
            logger.info("Deteniendo generación de datos...")
        finally:
            self.producer.close()
            total_time = time.time() - start_time
            logger.info(f"Generación completada. Total de eventos: {events_sent}")
            logger.info(f"Tiempo total: {total_time:.2f} segundos")
            logger.info(f"Tasa promedio: {events_sent/total_time:.2f} eventos/segundo")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Generador de datos de telemetría vehicular')
    parser.add_argument('--bootstrap-servers', default='localhost:9092',
                      help='Servidores Kafka bootstrap (default: localhost:9092)')
    parser.add_argument('--topic', default='vehicle-telemetry',
                      help='Topic de Kafka (default: vehicle-telemetry)')
    parser.add_argument('--duration', type=int, default=3600,
                      help='Duración en segundos (default: 3600)')
    
    args = parser.parse_args()
    
    # Crear y ejecutar el generador
    generator = VehicleTelemetryGenerator(args.bootstrap_servers, args.topic)
    generator.run(args.duration) 