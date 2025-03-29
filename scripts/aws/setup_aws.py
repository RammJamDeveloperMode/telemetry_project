#!/usr/bin/env python3
import json
import boto3
import time
from botocore.exceptions import ClientError

def load_config():
    """Carga la configuración de AWS desde el archivo JSON."""
    with open('config/aws_config.json', 'r') as f:
        return json.load(f)

def setup_msk(config):
    """Configura el cluster MSK y sus tópicos."""
    print("\nConfigurando Amazon MSK...")
    
    # Crear cliente MSK
    kafka = boto3.client('kafka')
    
    try:
        # Crear cluster MSK
        response = kafka.create_cluster(
            ClusterName=config['msk']['cluster_name'],
            BrokerNodeGroupInfo={
                'InstanceType': config['msk']['broker_node_group_info']['instance_type'],
                'NumberOfBrokerNodes': config['msk']['number_of_broker_nodes'],
                'StorageInfo': {
                    'EbsStorageInfo': {
                        'VolumeSize': config['msk']['broker_node_group_info']['volume_size'],
                        'VolumeType': config['msk']['broker_node_group_info']['volume_type']
                    }
                }
            },
            EncryptionInfo={
                'EncryptionInTransit': {
                    'InCluster': True,
                    'ClientBroker': 'TLS'
                }
            },
            EnhancedMonitoring='PER_BROKER',
            KafkaVersion=config['msk']['kafka_version']
        )
        
        cluster_arn = response['ClusterArn']
        print(f"Cluster MSK creado: {cluster_arn}")
        
        # Esperar a que el cluster esté activo
        print("Esperando a que el cluster esté activo...")
        waiter = kafka.get_waiter('cluster_active')
        waiter.wait(
            ClusterArn=cluster_arn,
            WaiterConfig={'Delay': 30, 'MaxAttempts': 40}
        )
        
        # Obtener información de los brokers
        response = kafka.get_bootstrap_brokers(ClusterArn=cluster_arn)
        bootstrap_servers = response['BootstrapBrokerStringTls']
        
        print(f"Bootstrap servers: {bootstrap_servers}")
        return bootstrap_servers
        
    except ClientError as e:
        print(f"Error al configurar MSK: {str(e)}")
        return None

def setup_s3(config):
    """Configura los buckets S3 y sus políticas."""
    print("\nConfigurando Amazon S3...")
    
    s3 = boto3.client('s3')
    buckets = {}
    
    try:
        # Crear buckets
        for bucket_type in ['raw_data_bucket', 'processed_data_bucket']:
            bucket_name = config['s3'][bucket_type]
            
            # Crear bucket
            s3.create_bucket(
                Bucket=bucket_name,
                CreateBucketConfiguration={
                    'LocationConstraint': boto3.session.Session().region_name
                }
            )
            
            # Configurar lifecycle rules
            lifecycle_rules = {
                'Rules': [
                    {
                        'ID': f'{bucket_type}-lifecycle',
                        'Status': 'Enabled',
                        'Transitions': [
                            {
                                'Days': config['s3']['retention_days'],
                                'StorageClass': 'STANDARD_IA'
                            }
                        ],
                        'Expiration': {
                            'Days': config['s3']['retention_days'] * 2
                        }
                    }
                ]
            }
            
            s3.put_bucket_lifecycle_configuration(
                Bucket=bucket_name,
                LifecycleConfiguration=lifecycle_rules
            )
            
            buckets[bucket_type] = bucket_name
            print(f"Bucket S3 configurado: {bucket_name}")
        
        return buckets
        
    except ClientError as e:
        print(f"Error al configurar S3: {str(e)}")
        return None

def setup_glue(config):
    """Configura la base de datos y tablas en AWS Glue."""
    print("\nConfigurando AWS Glue...")
    
    glue = boto3.client('glue')
    database_name = config['glue']['database_name']
    
    try:
        # Crear base de datos
        glue.create_database(
            DatabaseInput={
                'Name': database_name,
                'Description': 'Base de datos para telemetría vehicular'
            }
        )
        
        # Definir esquemas
        schemas = {
            'telemetry': {
                'vehicle_id': 'string',
                'timestamp': 'timestamp',
                'engine_temperature': 'double',
                'vehicle_speed': 'double',
                'fuel_level': 'double',
                'brake_status': 'boolean',
                'latitude': 'double',
                'longitude': 'double',
                'year': 'string',
                'month': 'string',
                'day': 'string'
            },
            'alerts': {
                'alert_id': 'string',
                'vehicle_id': 'string',
                'timestamp': 'timestamp',
                'alert_type': 'string',
                'alert_message': 'string',
                'severity': 'string',
                'year': 'string',
                'month': 'string',
                'day': 'string'
            }
        }
        
        # Crear tablas
        for table_name, schema in schemas.items():
            glue.create_table(
                DatabaseName=database_name,
                TableInput={
                    'Name': table_name,
                    'StorageDescriptor': {
                        'Columns': [
                            {'Name': col_name, 'Type': col_type}
                            for col_name, col_type in schema.items()
                        ],
                        'Location': f"s3://{config['s3']['processed_data_bucket']}/{table_name}",
                        'InputFormat': 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat',
                        'OutputFormat': 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat',
                        'SerdeInfo': {
                            'SerializationLibrary': 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
                        }
                    },
                    'TableType': 'EXTERNAL_TABLE',
                    'Parameters': {
                        'classification': 'parquet'
                    }
                }
            )
        
        print(f"Base de datos y tablas Glue configuradas: {database_name}")
        return database_name
        
    except ClientError as e:
        print(f"Error al configurar Glue: {str(e)}")
        return None

def setup_athena(config):
    """Configura el workgroup en Amazon Athena."""
    print("\nConfigurando Amazon Athena...")
    
    athena = boto3.client('athena')
    workgroup = config['athena']['workgroup']
    
    try:
        # Crear workgroup
        athena.create_work_group(
            Name=workgroup,
            Configuration={
                'ResultConfiguration': {
                    'OutputLocation': f"s3://{config['athena']['output_bucket']}/athena-results/"
                },
                'EnforceWorkGroupConfiguration': True,
                'PublishCloudWatchMetricsEnabled': True,
                'BytesScannedCutoffPerQuery': 1073741824  # 1GB
            }
        )
        
        print(f"Workgroup Athena configurado: {workgroup}")
        return workgroup
        
    except ClientError as e:
        print(f"Error al configurar Athena: {str(e)}")
        return None

def setup_quicksight(config):
    """Configura los datasets y dashboards en Amazon QuickSight."""
    print("\nConfigurando Amazon QuickSight...")
    
    quicksight = boto3.client('quicksight')
    namespace = config['quicksight']['namespace']
    
    try:
        # Crear datasets
        for dataset_name in config['quicksight']['datasets'].values():
            quicksight.create_data_set(
                AwsAccountId=boto3.client('sts').get_caller_identity()['Account'],
                DataSetId=dataset_name,
                Name=dataset_name,
                ImportMode='SPICE',
                PhysicalTableMap={
                    'telemetry': {
                        'RelationalTable': {
                            'DataSourceArn': f"arn:aws:quicksight:{boto3.session.Session().region_name}:{boto3.client('sts').get_caller_identity()['Account']}:datasource/vehicle-telemetry",
                            'Schema': 'vehicle_telemetry_db',
                            'Table': 'telemetry'
                        }
                    }
                }
            )
        
        print(f"Datasets QuickSight configurados: {', '.join(config['quicksight']['datasets'].values())}")
        return True
        
    except ClientError as e:
        print(f"Error al configurar QuickSight: {str(e)}")
        return False

def setup_sns(config):
    """Configura los tópicos SNS."""
    print("\nConfigurando Amazon SNS...")
    
    sns = boto3.client('sns')
    topics = {}
    
    try:
        # Crear tópicos
        for topic_key, topic_name in {
            'telemetry_topic': config['sns']['telemetry_topic'],
            'critical_topic': config['sns']['critical_topic']
        }.items():
            response = sns.create_topic(
                Name=topic_name,
                Tags=[
                    {
                        'Key': 'Project',
                        'Value': 'VehicleTelemetry'
                    }
                ]
            )
            topics[topic_key] = response['TopicArn']
        
        print(f"Tópicos SNS configurados: {', '.join(topics.keys())}")
        return topics
        
    except ClientError as e:
        print(f"Error al configurar SNS: {str(e)}")
        return None

def main():
    """Función principal para configurar todos los servicios AWS."""
    print("Iniciando configuración de servicios AWS...")
    
    # Cargar configuración
    config = load_config()
    
    # Configurar servicios
    bootstrap_servers = setup_msk(config)
    buckets = setup_s3(config)
    database_name = setup_glue(config)
    workgroup = setup_athena(config)
    quicksight_success = setup_quicksight(config)
    topics = setup_sns(config)
    
    # Verificar resultados
    success = all([
        bootstrap_servers,
        buckets,
        database_name,
        workgroup,
        quicksight_success,
        topics
    ])
    
    if success:
        print("\n¡Configuración completada exitosamente!")
        
        # Guardar información de conexión
        connection_info = {
            'bootstrap_servers': bootstrap_servers,
            'buckets': buckets,
            'database_name': database_name,
            'workgroup': workgroup,
            'topics': topics
        }
        
        with open('config/connection_info.json', 'w') as f:
            json.dump(connection_info, f, indent=4)
            
        print("Información de conexión guardada en config/connection_info.json")
    else:
        print("\n¡Error en la configuración! Revise los mensajes anteriores.")
        exit(1)

if __name__ == "__main__":
    main() 