#!/bin/bash

# Verificar si Homebrew está instalado
if ! command -v brew &> /dev/null; then
    echo "Instalando Homebrew..."
    /bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"
fi

# Instalar Kafka y Zookeeper
echo "Instalando Kafka y Zookeeper..."
brew install kafka

# Crear directorio para datos de Kafka
mkdir -p /tmp/kafka-logs

# Iniciar Zookeeper
echo "Iniciando Zookeeper..."
brew services start zookeeper

# Esperar a que Zookeeper esté listo
sleep 5

# Iniciar Kafka
echo "Iniciando Kafka..."
brew services start kafka

# Esperar a que Kafka esté listo
sleep 5

# Crear topic para telemetría
echo "Creando topic vehicle-telemetry..."
/opt/homebrew/Cellar/kafka/3.9.0/libexec/bin/kafka-topics.sh --create --topic vehicle-telemetry --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1

# Verificar que el topic se creó correctamente
echo "Verificando topics existentes..."
/opt/homebrew/Cellar/kafka/3.9.0/libexec/bin/kafka-topics.sh --list --bootstrap-server localhost:9092

echo "Configuración local completada."
echo "Para detener los servicios:"
echo "brew services stop kafka"
echo "brew services stop zookeeper" 