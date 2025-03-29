package com.vehicle.telemetry

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.streaming.{StreamingQuery, Trigger}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.OutputMode
import java.util.Properties
import scala.io.Source

object TelemetryProcessor {
  def main(args: Array[String]): Unit = {
    // Inicializar Spark con configuración optimizada
    val spark = SparkSession.builder()
      .appName("VehicleTelemetryProcessor")
      .master("local[*]")  // Para pruebas locales
      .config("spark.sql.streaming.checkpointLocation", "/tmp/checkpoints")
      .config("spark.sql.streaming.schemaInference", "true")
      .config("spark.sql.streaming.minBatchesToRetain", "100")
      .config("spark.sql.streaming.maxBatchesToRetainInMemory", "100")
      .config("spark.sql.streaming.stopGracefullyOnShutdown", "true")
      .config("spark.sql.streaming.continuous.epochBacklogQueueSize", "10000")
      .config("spark.sql.streaming.continuous.executorQueueSize", "10000")
      .config("spark.sql.streaming.continuous.executorPollIntervalMs", "100")
      .config("spark.sql.streaming.continuous.shuffleParallelism", "10")
      .config("spark.sql.streaming.continuous.backpressure.enabled", "true")
      .config("spark.sql.streaming.continuous.backpressure.initialRate", "1000")
      .config("spark.sql.streaming.continuous.backpressure.pid.proportional", "1.0")
      .config("spark.sql.streaming.continuous.backpressure.pid.integral", "0.2")
      .config("spark.sql.streaming.continuous.backpressure.pid.derived", "0.05")
      .config("spark.sql.streaming.continuous.backpressure.pid.minRate", "100")
      .config("spark.sql.streaming.continuous.backpressure.pid.maxRate", "10000")
      .getOrCreate()

    // Cargar configuración desde archivo de propiedades
    val config = loadConfig("config/application.properties")

    // Definir esquema de telemetría
    val telemetrySchema = StructType(Seq(
      StructField("vehicle_id", StringType),
      StructField("timestamp", TimestampType),
      StructField("engine_temperature", DoubleType),
      StructField("vehicle_speed", DoubleType),
      StructField("fuel_level", DoubleType),
      StructField("brake_status", BooleanType),
      StructField("location", StructType(Seq(
        StructField("latitude", DoubleType),
        StructField("longitude", DoubleType)
      )))
    ))

    // Configurar Kafka
    val kafkaOptions = Map(
      "kafka.bootstrap.servers" -> config.getProperty("kafka.bootstrap.servers"),
      "subscribe" -> config.getProperty("kafka.topic"),
      "startingOffsets" -> "earliest",
      "failOnDataLoss" -> "false",
      "maxOffsetsPerTrigger" -> "10000",
      "maxRatePerPartition" -> "1000",
      "minPartitions" -> "10",
      "maxPartitions" -> "100"
    )

    // Leer stream de telemetría
    val telemetryStream = spark.readStream
      .format("kafka")
      .options(kafkaOptions)
      .load()
      .select(from_json(col("value").cast(StringType), telemetrySchema).as("data"))
      .select("data.*")

    // Procesar datos
    val processedStream = telemetryStream
      .withColumn("year", date_format(col("timestamp"), "yyyy"))
      .withColumn("month", date_format(col("timestamp"), "MM"))
      .withColumn("day", date_format(col("timestamp"), "dd"))
      .withColumn("hour", date_format(col("timestamp"), "HH"))
      .withColumn("minute", date_format(col("timestamp"), "mm"))
      .withColumn("date_partition", date_format(col("timestamp"), "yyyy-MM-dd"))
      .withColumn("partition_key", concat(
        col("year"), lit("/"),
        col("month"), lit("/"),
        col("day"), lit("/"),
        col("hour"), lit("/"),
        col("minute")
      ))

    // Detectar alertas
    val alertsStream = processedStream
      .filter(col("engine_temperature") > 90 || col("vehicle_speed") > 120 || col("fuel_level") < 20)
      .withColumn("alert_id", expr("uuid()"))
      .withColumn("alert_type", when(col("engine_temperature") > 90, "HIGH_TEMPERATURE")
        .when(col("vehicle_speed") > 120, "SPEED_LIMIT")
        .when(col("fuel_level") < 20, "LOW_FUEL"))
      .withColumn("alert_message", when(col("engine_temperature") > 90, 
          concat(lit("High engine temperature: "), col("engine_temperature"), lit("°C")))
        .when(col("vehicle_speed") > 120, 
          concat(lit("Speed limit exceeded: "), col("vehicle_speed"), lit(" km/h")))
        .when(col("fuel_level") < 20, 
          concat(lit("Low fuel level: "), col("fuel_level"), lit("%"))))
      .withColumn("severity", when(col("engine_temperature") > 90, "HIGH")
        .when(col("vehicle_speed") > 120, "MEDIUM")
        .when(col("fuel_level") < 20, "LOW"))

    // Guardar datos procesados localmente
    val processedQuery = processedStream.writeStream
      .format("parquet")
      .option("path", "/tmp/output/processed")
      .option("checkpointLocation", "/tmp/checkpoints/processed")
      .partitionBy("year", "month", "day", "hour", "minute")
      .outputMode(OutputMode.Append())
      .trigger(Trigger.ProcessingTime("5 seconds"))
      .start()

    // Guardar alertas localmente
    val alertsQuery = alertsStream.writeStream
      .format("parquet")
      .option("path", "/tmp/output/alerts")
      .option("checkpointLocation", "/tmp/checkpoints/alerts")
      .partitionBy("date_partition")
      .outputMode(OutputMode.Append())
      .trigger(Trigger.ProcessingTime("5 seconds"))
      .start()

    // Imprimir alertas en consola para pruebas
    val consoleQuery = alertsStream.writeStream
      .format("console")
      .option("truncate", "false")
      .outputMode(OutputMode.Append())
      .trigger(Trigger.ProcessingTime("5 seconds"))
      .start()

    // Esperar terminación
    processedQuery.awaitTermination()
    alertsQuery.awaitTermination()
    consoleQuery.awaitTermination()
  }

  private def loadConfig(path: String): Properties = {
    val props = new Properties()
    val source = Source.fromFile(path)
    props.load(source.bufferedReader())
    source.close()
    props
  }
} 