package com.vehicle.telemetry

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.BeforeAndAfterAll
import org.scalatest.matchers.should.Matchers
import java.io.File
import java.nio.file.Files

class KinesisAnalyticsTest extends AnyFunSuite with BeforeAndAfterAll with Matchers {
  var spark: SparkSession = _
  var tempDir: File = _

  override def beforeAll(): Unit = {
    spark = SparkSession.builder()
      .appName("KinesisAnalyticsTest")
      .master("local[*]")
      .config("spark.sql.streaming.checkpointLocation", "/tmp/checkpoints/test")
      .getOrCreate()
    
    // Crear directorio temporal para pruebas
    tempDir = Files.createTempDirectory("telemetry_test").toFile
  }

  override def afterAll(): Unit = {
    if (spark != null) {
      spark.stop()
    }
    // Limpiar directorio temporal
    if (tempDir != null && tempDir.exists()) {
      tempDir.listFiles().foreach(_.delete())
      tempDir.delete()
    }
  }

  test("processStreamingData debería analizar correctamente los patrones de conducción") {
    // Crear datos de prueba
    val schema = StructType(Seq(
      StructField("vehicle_id", StringType),
      StructField("timestamp", TimestampType),
      StructField("engine_temperature", DoubleType),
      StructField("vehicle_speed", DoubleType),
      StructField("fuel_level", DoubleType)
    ))

    val testData = Seq(
      ("VIN-001", java.sql.Timestamp.valueOf("2024-03-29 10:00:00"), 85.0, 80.0, 75.0),
      ("VIN-001", java.sql.Timestamp.valueOf("2024-03-29 10:01:00"), 87.0, 85.0, 73.0),
      ("VIN-001", java.sql.Timestamp.valueOf("2024-03-29 10:02:00"), 90.0, 95.0, 70.0)
    )

    val inputDF = spark.createDataFrame(testData).toDF(schema.fieldNames: _*)
    val resultDF = KinesisAnalytics.processStreamingData(spark, inputDF)

    // Verificar resultados
    val result = resultDF.collect()
    result.length should be(1)
    result(0).getAs[String]("driving_style") should be("moderado")
    result(0).getAs[String]("risk_level") should be("medio")
  }

  test("detectAnomalies debería identificar comportamientos anómalos") {
    // Crear datos de prueba
    val schema = StructType(Seq(
      StructField("vehicle_id", StringType),
      StructField("timestamp", TimestampType),
      StructField("engine_temperature", DoubleType),
      StructField("vehicle_speed", DoubleType)
    ))

    val testData = Seq(
      ("VIN-001", java.sql.Timestamp.valueOf("2024-03-29 10:00:00"), 85.0, 80.0),
      ("VIN-001", java.sql.Timestamp.valueOf("2024-03-29 10:01:00"), 87.0, 85.0),
      ("VIN-001", java.sql.Timestamp.valueOf("2024-03-29 10:02:00"), 110.0, 130.0)
    )

    val inputDF = spark.createDataFrame(testData).toDF(schema.fieldNames: _*)
    val resultDF = KinesisAnalytics.detectAnomalies(spark, inputDF)

    // Verificar resultados
    val result = resultDF.collect()
    result.length should be(1)
    result(0).getAs[Boolean]("temp_anomaly") should be(true)
    result(0).getAs[Boolean]("speed_anomaly") should be(true)
  }

  test("predictMaintenance debería priorizar correctamente el mantenimiento") {
    // Crear datos de prueba
    val schema = StructType(Seq(
      StructField("vehicle_id", StringType),
      StructField("timestamp", TimestampType),
      StructField("engine_temperature", DoubleType),
      StructField("vehicle_speed", DoubleType)
    ))

    val testData = Seq(
      ("VIN-001", java.sql.Timestamp.valueOf("2024-03-29 10:00:00"), 95.0, 110.0),
      ("VIN-001", java.sql.Timestamp.valueOf("2024-03-29 10:01:00"), 92.0, 105.0),
      ("VIN-001", java.sql.Timestamp.valueOf("2024-03-29 10:02:00"), 98.0, 115.0),
      ("VIN-001", java.sql.Timestamp.valueOf("2024-03-29 10:03:00"), 93.0, 108.0),
      ("VIN-001", java.sql.Timestamp.valueOf("2024-03-29 10:04:00"), 96.0, 112.0),
      ("VIN-001", java.sql.Timestamp.valueOf("2024-03-29 10:05:00"), 94.0, 107.0),
      ("VIN-001", java.sql.Timestamp.valueOf("2024-03-29 10:06:00"), 97.0, 113.0),
      ("VIN-001", java.sql.Timestamp.valueOf("2024-03-29 10:07:00"), 91.0, 104.0),
      ("VIN-001", java.sql.Timestamp.valueOf("2024-03-29 10:08:00"), 99.0, 116.0),
      ("VIN-001", java.sql.Timestamp.valueOf("2024-03-29 10:09:00"), 93.0, 109.0),
      ("VIN-001", java.sql.Timestamp.valueOf("2024-03-29 10:10:00"), 95.0, 111.0),
      ("VIN-001", java.sql.Timestamp.valueOf("2024-03-29 10:11:00"), 96.0, 114.0)
    )

    val inputDF = spark.createDataFrame(testData).toDF(schema.fieldNames: _*)
    val resultDF = KinesisAnalytics.predictMaintenance(spark, inputDF)

    // Verificar resultados
    val result = resultDF.collect()
    result.length should be(1)
    result(0).getAs[String]("maintenance_priority") should be("alta")
  }

  test("debería escribir y leer correctamente archivos Parquet") {
    // Crear datos de prueba
    val schema = StructType(Seq(
      StructField("vehicle_id", StringType),
      StructField("timestamp", TimestampType),
      StructField("engine_temperature", DoubleType),
      StructField("vehicle_speed", DoubleType),
      StructField("fuel_level", DoubleType)
    ))

    val testData = Seq(
      ("VIN-001", java.sql.Timestamp.valueOf("2024-03-29 10:00:00"), 85.0, 80.0, 75.0),
      ("VIN-001", java.sql.Timestamp.valueOf("2024-03-29 10:01:00"), 87.0, 85.0, 73.0),
      ("VIN-001", java.sql.Timestamp.valueOf("2024-03-29 10:02:00"), 90.0, 95.0, 70.0)
    )

    val inputDF = spark.createDataFrame(testData).toDF(schema.fieldNames: _*)
    
    // Escribir a Parquet
    val parquetPath = s"${tempDir.getAbsolutePath}/test_data.parquet"
    inputDF.write.mode("overwrite").parquet(parquetPath)
    
    // Verificar que el archivo existe
    new File(parquetPath).exists() should be(true)
    
    // Leer el archivo Parquet
    val readDF = spark.read.parquet(parquetPath)
    
    // Verificar el esquema
    readDF.schema should be(schema)
    
    // Verificar los datos
    val result = readDF.collect()
    result.length should be(3)
    
    // Verificar valores específicos de manera más robusta
    val vehicleIds = result.map(_.getAs[String]("vehicle_id")).toSet
    val temperatures = result.map(_.getAs[Double]("engine_temperature")).toSet
    val speeds = result.map(_.getAs[Double]("vehicle_speed")).toSet
    val fuelLevels = result.map(_.getAs[Double]("fuel_level")).toSet
    
    vehicleIds should contain("VIN-001")
    temperatures should contain allOf(85.0, 87.0, 90.0)
    speeds should contain allOf(80.0, 85.0, 95.0)
    fuelLevels should contain allOf(75.0, 73.0, 70.0)
  }
} 