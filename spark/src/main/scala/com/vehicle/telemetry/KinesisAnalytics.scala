package com.vehicle.telemetry

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.StreamingQuery
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types._

object KinesisAnalytics {
  /**
   * Analiza los patrones de conducción de cada vehículo
   * Utiliza ventanas de 5 minutos para identificar el estilo de conducción
   * y evaluar el nivel de riesgo basado en múltiples métricas
   */
  def processStreamingData(spark: SparkSession, inputStream: DataFrame): DataFrame = {
    // Análisis de patrones de conducción en ventanas de 5 minutos
    val drivingPatterns = inputStream
      .withWatermark("timestamp", "5 minutes")
      .groupBy(
        window(col("timestamp"), "5 minutes"),
        col("vehicle_id")
      )
      .agg(
        avg("vehicle_speed").as("avg_speed"),
        max("vehicle_speed").as("max_speed"),
        avg("engine_temperature").as("avg_temp"),
        max("engine_temperature").as("max_temp"),
        avg("fuel_level").as("avg_fuel")
      )
      .withColumn("driving_style", 
        when(col("avg_speed") > 100, "agresivo")
        .when(col("avg_speed") > 70, "moderado")
        .otherwise("conservador")
      )
      .withColumn("risk_level",
        when(col("max_speed") > 120 || col("max_temp") > 95, "alto")
        .when(col("max_speed") > 90 || col("max_temp") > 85, "medio")
        .otherwise("bajo")
      )

    drivingPatterns
  }

  /**
   * Detecta comportamientos anómalos en tiempo real
   * Utiliza análisis estadístico para identificar desviaciones significativas
   * de los patrones normales de operación
   */
  def detectAnomalies(spark: SparkSession, inputStream: DataFrame): DataFrame = {
    // Detección de anomalías en ventanas de 10 minutos
    val anomalies = inputStream
      .withWatermark("timestamp", "10 minutes")
      .groupBy(
        window(col("timestamp"), "10 minutes"),
        col("vehicle_id")
      )
      .agg(
        avg("engine_temperature").as("avg_temp"),
        stddev("engine_temperature").as("std_temp"),
        avg("vehicle_speed").as("avg_speed"),
        stddev("vehicle_speed").as("std_speed")
      )
      .withColumn("temp_anomaly",
        when(col("avg_temp").plus(lit(2).multiply(col("std_temp"))) > 105, true)
        .otherwise(false)
      )
      .withColumn("speed_anomaly",
        when(col("avg_speed").plus(lit(2).multiply(col("std_speed"))) > 120, true)
        .otherwise(false)
      )

    anomalies
  }

  /**
   * Predice necesidades de mantenimiento basándose en patrones históricos
   * Analiza 24 horas de datos para identificar vehículos que requieren atención
   */
  def predictMaintenance(spark: SparkSession, inputStream: DataFrame): DataFrame = {
    // Predicción de mantenimiento basada en patrones de 24 horas
    val maintenance = inputStream
      .withWatermark("timestamp", "24 hours")
      .groupBy(
        window(col("timestamp"), "24 hours"),
        col("vehicle_id")
      )
      .agg(
        count(when(col("engine_temperature") > 90, true)).as("high_temp_count"),
        count(when(col("vehicle_speed") > 100, true)).as("high_speed_count")
      )
      .withColumn("maintenance_priority",
        when(col("high_temp_count") > 10 || col("high_speed_count") > 20, "alta")
        .when(col("high_temp_count") > 5 || col("high_speed_count") > 10, "media")
        .otherwise("baja")
      )

    maintenance
  }
} 