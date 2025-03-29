package com.vehicle.telemetry

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.streaming.StreamingQuery
import org.apache.spark.sql.functions._
import org.scalatest.BeforeAndAfter
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import java.time.LocalDateTime
import java.io.File

class IntegrationTest extends AnyFlatSpec with Matchers with BeforeAndAfter {
  var spark: SparkSession = _
  var checkpointDir: String = _
  var testOutputDir: String = _
  
  before {
    // Crear directorios temporales para checkpoints y output
    checkpointDir = "target/test-checkpoints"
    testOutputDir = "target/test-output"
    new File(checkpointDir).mkdirs()
    new File(testOutputDir).mkdirs()
    
    spark = SparkSession.builder()
      .appName("TelemetryIntegrationTest")
      .master("local[*]")
      .config("spark.sql.streaming.checkpointLocation", checkpointDir)
      .getOrCreate()
  }
  
  after {
    if (spark != null) {
      spark.stop()
    }
    // Limpiar directorios
    new File(checkpointDir).delete()
    new File(testOutputDir).delete()
  }
  
  "The complete pipeline" should "process telemetry data and generate alerts" in {
    // Crear datos de prueba
    val testData = Seq(
      ("VIN-123", LocalDateTime.now(), 105.5, 65.0, 45.3, false, 19.4326, -99.1332),
      ("VIN-456", LocalDateTime.now(), 85.0, 125.0, 15.3, true, 19.4327, -99.1333),
      ("VIN-789", LocalDateTime.now(), 85.0, 65.0, 15.3, false, 19.4328, -99.1334)
    )
    
    // Crear DataFrame de prueba
    val testDF = spark.createDataFrame(testData).toDF(
      "vehicle_id", "timestamp", "engine_temperature", "vehicle_speed",
      "fuel_level", "brake_status", "latitude", "longitude"
    )
    
    // Procesar datos
    val processedDF = testDF
      .withColumn("year", date_format(col("timestamp"), "yyyy"))
      .withColumn("month", date_format(col("timestamp"), "MM"))
      .withColumn("day", date_format(col("timestamp"), "dd"))
    
    // Verificar datos procesados
    processedDF.count() should be(3)
    processedDF.columns should contain allOf("year", "month", "day")
    
    // Guardar datos procesados en parquet
    val telemetryOutputPath = s"$testOutputDir/telemetry"
    processedDF.write.mode("overwrite").parquet(telemetryOutputPath)
    
    // Mostrar datos procesados
    println("\nDatos de telemetría procesados:")
    println("================================")
    spark.read.parquet(telemetryOutputPath).orderBy("vehicle_id").show(false)
    
    // Procesar alertas
    val alertsDF = processedDF
      .filter(col("engine_temperature") > 90 || col("vehicle_speed") > 120 || col("fuel_level") < 20)
      .withColumn("alert_id", expr("uuid()"))
      .withColumn("alert_type", when(col("engine_temperature") > 90, "HIGH_TEMPERATURE")
        .when(col("vehicle_speed") > 120, "SPEED_LIMIT")
        .when(col("fuel_level") < 20, "LOW_FUEL"))
      .withColumn("alert_message", when(col("engine_temperature") > 90, 
        concat(lit("High engine temperature: "), col("engine_temperature"), lit("°C")))
        .when(col("vehicle_speed") > 120, concat(lit("Speed limit exceeded: "), col("vehicle_speed"), lit(" km/h")))
        .when(col("fuel_level") < 20, concat(lit("Low fuel level: "), col("fuel_level"), lit("%"))))
      .withColumn("severity", when(col("engine_temperature") > 90, "HIGH")
        .when(col("vehicle_speed") > 120, "MEDIUM")
        .when(col("fuel_level") < 20, "LOW"))
    
    // Guardar alertas en parquet
    val alertsOutputPath = s"$testOutputDir/alerts"
    alertsDF.write.mode("overwrite").parquet(alertsOutputPath)
    
    // Mostrar alertas
    println("\nAlertas generadas:")
    println("==================")
    spark.read.parquet(alertsOutputPath).orderBy("vehicle_id").show(false)
    
    // Verificar alertas
    alertsDF.count() should be(3)
    
    // Verificar tipos de alertas
    val alertTypes = alertsDF.select("alert_type").distinct().collect().map(_.getString(0))
    alertTypes should contain allOf("HIGH_TEMPERATURE", "SPEED_LIMIT", "LOW_FUEL")
    
    // Verificar severidades
    val severities = alertsDF.select("severity").distinct().collect().map(_.getString(0))
    severities should contain allOf("HIGH", "MEDIUM", "LOW")
  }
  
  it should "handle invalid data gracefully" in {
    // Crear datos inválidos
    val invalidData = spark.createDataFrame(Seq(
      ("VIN-123", null, 105.5, 65.0, 45.3, false, 19.4326, -99.1332),
      ("VIN-456", LocalDateTime.now(), Double.NaN, 125.0, 15.3, true, 19.4327, -99.1333)
    )).toDF(
      "vehicle_id", "timestamp", "engine_temperature", "vehicle_speed",
      "fuel_level", "brake_status", "latitude", "longitude"
    )
    
    // Procesar datos (debería filtrar los nulos)
    val processedDF = invalidData
      .na.drop(Seq("timestamp", "engine_temperature"))
      .withColumn("year", date_format(col("timestamp"), "yyyy"))
      .withColumn("month", date_format(col("timestamp"), "MM"))
      .withColumn("day", date_format(col("timestamp"), "dd"))
    
    // Guardar datos procesados en parquet
    val invalidOutputPath = s"$testOutputDir/invalid"
    processedDF.write.mode("overwrite").parquet(invalidOutputPath)
    
    // Mostrar datos inválidos procesados
    println("\nDatos inválidos procesados:")
    println("==========================")
    spark.read.parquet(invalidOutputPath).show(false)
    
    // Verificar que los datos inválidos fueron filtrados
    processedDF.count() should be(0)
  }
} 