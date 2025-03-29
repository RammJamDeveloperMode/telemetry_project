package com.vehicle.telemetry

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.streaming.StreamingQuery
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import java.sql.Timestamp

class TelemetryProcessorTest extends AnyFunSuite with Matchers with BeforeAndAfterAll {
  var spark: SparkSession = _
  
  override def beforeAll(): Unit = {
    spark = SparkSession.builder()
      .appName("TelemetryProcessorTest")
      .master("local[*]")
      .config("spark.sql.streaming.checkpointLocation", "target/checkpoints")
      .getOrCreate()
  }
  
  override def afterAll(): Unit = {
    if (spark != null) {
      spark.stop()
    }
  }
  
  test("Procesamiento de datos de telemetría válidos") {
    // Crear datos de prueba
    val testData = Seq(
      ("VIN-123456789", Timestamp.valueOf("2025-03-25 13:45:30"), 92.5, 65.7, 45.3, false, 19.4326, -99.1332),
      ("VIN-987654321", Timestamp.valueOf("2025-03-25 13:45:31"), 88.2, 75.3, 60.1, true, 19.4327, -99.1333)
    )
    
    val schema = StructType(Seq(
      StructField("vehicle_id", StringType),
      StructField("timestamp", TimestampType),
      StructField("engine_temperature", DoubleType),
      StructField("vehicle_speed", DoubleType),
      StructField("fuel_level", DoubleType),
      StructField("brake_status", BooleanType),
      StructField("latitude", DoubleType),
      StructField("longitude", DoubleType)
    ))
    
    val inputDF = spark.createDataFrame(testData).toDF(schema.fieldNames: _*)
    
    // Procesar datos
    val processedDF = inputDF
      .withColumn("year", date_format(col("timestamp"), "yyyy"))
      .withColumn("month", date_format(col("timestamp"), "MM"))
      .withColumn("day", date_format(col("timestamp"), "dd"))
    
    // Verificar resultados
    processedDF.count() shouldBe 2
    processedDF.select("year").distinct().count() shouldBe 1
    processedDF.select("month").distinct().count() shouldBe 1
    processedDF.select("day").distinct().count() shouldBe 1
  }
  
  test("Detección de alertas de temperatura alta") {
    val testData = Seq(
      ("VIN-123456789", Timestamp.valueOf("2025-03-25 13:45:30"), 95.5, 65.7, 45.3, false, 19.4326, -99.1332),
      ("VIN-987654321", Timestamp.valueOf("2025-03-25 13:45:31"), 88.2, 75.3, 60.1, true, 19.4327, -99.1333)
    )
    
    val schema = StructType(Seq(
      StructField("vehicle_id", StringType),
      StructField("timestamp", TimestampType),
      StructField("engine_temperature", DoubleType),
      StructField("vehicle_speed", DoubleType),
      StructField("fuel_level", DoubleType),
      StructField("brake_status", BooleanType),
      StructField("latitude", DoubleType),
      StructField("longitude", DoubleType)
    ))
    
    val inputDF = spark.createDataFrame(testData).toDF(schema.fieldNames: _*)
    
    val alertsDF = inputDF
      .filter(col("engine_temperature") > 90)
      .withColumn("alert_id", expr("uuid()"))
      .withColumn("alert_type", when(col("engine_temperature") > 90, "HIGH_TEMPERATURE"))
      .withColumn("alert_message", when(col("engine_temperature") > 90, 
        concat(lit("High engine temperature: "), col("engine_temperature"), lit("°C"))))
      .withColumn("severity", when(col("engine_temperature") > 90, "HIGH"))
    
    alertsDF.count() shouldBe 1
    alertsDF.select("alert_type").first().getString(0) shouldBe "HIGH_TEMPERATURE"
    alertsDF.select("severity").first().getString(0) shouldBe "HIGH"
  }
  
  test("Detección de alertas de velocidad excesiva") {
    val testData = Seq(
      ("VIN-123456789", Timestamp.valueOf("2025-03-25 13:45:30"), 92.5, 125.7, 45.3, false, 19.4326, -99.1332),
      ("VIN-987654321", Timestamp.valueOf("2025-03-25 13:45:31"), 88.2, 75.3, 60.1, true, 19.4327, -99.1333)
    )
    
    val schema = StructType(Seq(
      StructField("vehicle_id", StringType),
      StructField("timestamp", TimestampType),
      StructField("engine_temperature", DoubleType),
      StructField("vehicle_speed", DoubleType),
      StructField("fuel_level", DoubleType),
      StructField("brake_status", BooleanType),
      StructField("latitude", DoubleType),
      StructField("longitude", DoubleType)
    ))
    
    val inputDF = spark.createDataFrame(testData).toDF(schema.fieldNames: _*)
    
    val alertsDF = inputDF
      .filter(col("vehicle_speed") > 120)
      .withColumn("alert_id", expr("uuid()"))
      .withColumn("alert_type", when(col("vehicle_speed") > 120, "SPEED_LIMIT"))
      .withColumn("alert_message", when(col("vehicle_speed") > 120, 
        concat(lit("Speed limit exceeded: "), col("vehicle_speed"), lit(" km/h"))))
      .withColumn("severity", when(col("vehicle_speed") > 120, "MEDIUM"))
    
    alertsDF.count() shouldBe 1
    alertsDF.select("alert_type").first().getString(0) shouldBe "SPEED_LIMIT"
    alertsDF.select("severity").first().getString(0) shouldBe "MEDIUM"
  }
  
  test("Detección de alertas de combustible bajo") {
    val testData = Seq(
      ("VIN-123456789", Timestamp.valueOf("2025-03-25 13:45:30"), 92.5, 65.7, 15.3, false, 19.4326, -99.1332),
      ("VIN-987654321", Timestamp.valueOf("2025-03-25 13:45:31"), 88.2, 75.3, 60.1, true, 19.4327, -99.1333)
    )
    
    val schema = StructType(Seq(
      StructField("vehicle_id", StringType),
      StructField("timestamp", TimestampType),
      StructField("engine_temperature", DoubleType),
      StructField("vehicle_speed", DoubleType),
      StructField("fuel_level", DoubleType),
      StructField("brake_status", BooleanType),
      StructField("latitude", DoubleType),
      StructField("longitude", DoubleType)
    ))
    
    val inputDF = spark.createDataFrame(testData).toDF(schema.fieldNames: _*)
    
    val alertsDF = inputDF
      .filter(col("fuel_level") < 20)
      .withColumn("alert_id", expr("uuid()"))
      .withColumn("alert_type", when(col("fuel_level") < 20, "LOW_FUEL"))
      .withColumn("alert_message", when(col("fuel_level") < 20, 
        concat(lit("Low fuel level: "), col("fuel_level"), lit("%"))))
      .withColumn("severity", when(col("fuel_level") < 20, "LOW"))
    
    alertsDF.count() shouldBe 1
    alertsDF.select("alert_type").first().getString(0) shouldBe "LOW_FUEL"
    alertsDF.select("severity").first().getString(0) shouldBe "LOW"
  }
} 