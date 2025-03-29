#!/usr/bin/env python3
import json
import os
from datetime import datetime
from pathlib import Path

class MockStorage:
    def __init__(self, base_path="mock_data"):
        self.base_path = base_path
        Path(base_path).mkdir(parents=True, exist_ok=True)
        
    def save_data(self, data, prefix):
        timestamp = datetime.utcnow()
        year = timestamp.strftime("%Y")
        month = timestamp.strftime("%m")
        day = timestamp.strftime("%d")
        
        path = Path(self.base_path) / prefix / f"year={year}" / f"month={month}" / f"day={day}"
        path.mkdir(parents=True, exist_ok=True)
        
        filename = f"{timestamp.strftime('%H%M%S')}.json"
        with open(path / filename, "w") as f:
            json.dump(data, f)
        print(f"Saved data to {path / filename}")
        return str(path / filename)

    def list_data(self, prefix):
        """Lista los archivos de datos en el almacenamiento simulado."""
        data_files = []
        prefix_path = Path(self.base_path) / prefix
        
        if prefix_path.exists():
            for root, _, files in os.walk(prefix_path):
                for file in files:
                    if file.endswith('.json'):
                        data_files.append(str(Path(root) / file))
        
        return data_files

class MockStream:
    def __init__(self, name):
        self.name = name
        self.subscribers = []
        self.records = []
    
    def put_record(self, data):
        """Envía un registro al stream."""
        self.records.append(data)
        # Notificar a todos los suscriptores
        for subscriber in self.subscribers:
            subscriber(data)
        print(f"Sent data to stream {self.name}")
    
    def get_records(self):
        """Obtiene todos los registros del stream."""
        return self.records

class MockAnalytics:
    def __init__(self):
        self.tables = {}
        self._setup_tables()
    
    def _setup_tables(self):
        """Configura las tablas iniciales."""
        self.tables = {
            "vehicle_telemetry": {
                "schema": {
                    "vehicle_id": "string",
                    "timestamp": "datetime",
                    "engine_temperature": "double",
                    "vehicle_speed": "double",
                    "fuel_level": "double",
                    "latitude": "double",
                    "longitude": "double"
                },
                "data": []
            },
            "vehicle_alerts": {
                "schema": {
                    "alert_id": "string",
                    "vehicle_id": "string",
                    "alert_type": "string",
                    "severity": "string",
                    "message": "string",
                    "timestamp": "datetime"
                },
                "data": []
            }
        }
    
    def insert_data(self, table_name, data):
        """Inserta datos en una tabla."""
        if table_name in self.tables:
            self.tables[table_name]["data"].append(data)
            print(f"Inserted data into table {table_name} (total: {len(self.tables[table_name]['data'])})")
    
    def get_row_count(self, table_name):
        """Obtiene el número de filas en una tabla."""
        if table_name in self.tables:
            count = len(self.tables[table_name]["data"])
            print(f"Table {table_name} has {count} rows")
            return count
        return 0

class MockNotification:
    def __init__(self):
        self.topics = {
            "vehicle-telemetry-alerts": [],
            "vehicle-critical-alerts": []
        }
    
    def publish(self, topic, message):
        """Publica un mensaje en un tópico."""
        if topic in self.topics:
            for subscriber in self.topics[topic]:
                subscriber(message)
            print(f"Published message to topic {topic}")
    
    def subscribe(self, topic, callback):
        """Suscribe una función callback a un tópico."""
        if topic in self.topics:
            self.topics[topic].append(callback)
            print(f"Subscribed to topic {topic}")
    
    def list_topics(self):
        """Lista los tópicos disponibles."""
        return list(self.topics.keys())
    
    def list_subscribers(self, topic):
        """Lista los suscriptores de un tópico."""
        return self.topics.get(topic, [])

class MockVisualization:
    def __init__(self):
        self.datasets = {
            "vehicle-telemetry-dataset": {"source": "vehicle_telemetry"},
            "vehicle-alerts-dataset": {"source": "vehicle_alerts"}
        }
        self.dashboards = {
            "vehicle-telemetry-dashboard": {"datasets": ["vehicle-telemetry-dataset"]},
            "vehicle-alerts-dashboard": {"datasets": ["vehicle-alerts-dataset"]}
        }
    
    def create_dataset(self, name, source):
        """Crea un nuevo dataset."""
        self.datasets[name] = {"source": source}
        print(f"Created dataset {name}")
    
    def update_dataset(self, name, data):
        """Actualiza los datos de un dataset."""
        if name in self.datasets:
            self.datasets[name]["data"] = data
            print(f"Updated dataset {name}")
    
    def create_dashboard(self, name, datasets):
        """Crea un nuevo dashboard."""
        self.dashboards[name] = {"datasets": datasets}
        print(f"Created dashboard {name}")
    
    def list_datasets(self):
        """Lista los datasets disponibles."""
        return list(self.datasets.keys())
    
    def list_dashboards(self):
        """Lista los dashboards disponibles."""
        return list(self.dashboards.keys())

# Instancias globales de los servicios simulados
storage = MockStorage()
telemetry_stream = MockStream("vehicle-telemetry-stream")
alerts_stream = MockStream("vehicle-alerts-stream")
analytics = MockAnalytics()
notification = MockNotification()
visualization = MockVisualization()

def setup_mock_environment():
    """Configura el entorno simulado."""
    # Crear directorios necesarios
    os.makedirs("mock_data/raw-data", exist_ok=True)
    os.makedirs("mock_data/alerts", exist_ok=True)
    
    # Configurar suscriptores para los streams
    def process_telemetry(data):
        storage.save_data(data, "raw-data")
        analytics.insert_data("vehicle_telemetry", data)
    
    def process_alert(data):
        storage.save_data(data, "alerts")
        analytics.insert_data("vehicle_alerts", data)
    
    telemetry_stream.subscribers.append(process_telemetry)
    alerts_stream.subscribers.append(process_alert)
    
    print("Mock environment setup completed") 