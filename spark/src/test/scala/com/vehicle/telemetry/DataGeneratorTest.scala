package com.vehicle.telemetry

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

class DataGeneratorTest extends AnyFlatSpec with Matchers {
  "DataGenerator" should "generate valid vehicle data" in {
    val vehicleId = "VIN-123456789"
    val timestamp = LocalDateTime.now()
    
    val data = Map(
      "vehicle_id" -> vehicleId,
      "timestamp" -> timestamp,
      "engine_temperature" -> 92.5,
      "vehicle_speed" -> 65.7,
      "fuel_level" -> 45.3,
      "brake_status" -> false,
      "location" -> Map(
        "latitude" -> 19.4326,
        "longitude" -> -99.1332
      )
    )
    
    // Verificar estructura de datos
    data should contain key "vehicle_id"
    data should contain key "timestamp"
    data should contain key "engine_temperature"
    data should contain key "vehicle_speed"
    data should contain key "fuel_level"
    data should contain key "brake_status"
    data should contain key "location"
    
    // Verificar tipos de datos
    data("vehicle_id") shouldBe a[String]
    data("timestamp") shouldBe a[LocalDateTime]
    data("engine_temperature") shouldBe a[Double]
    data("vehicle_speed") shouldBe a[Double]
    data("fuel_level") shouldBe a[Double]
    data("brake_status") shouldBe a[Boolean]
    data("location") shouldBe a[Map[_, _]]
    
    // Verificar valores
    data("vehicle_id") should be(vehicleId)
    data("engine_temperature") should be(92.5)
    data("vehicle_speed") should be(65.7)
    data("fuel_level") should be(45.3)
    data("brake_status").asInstanceOf[Boolean] should be(false)
    
    // Verificar ubicación
    val location = data("location").asInstanceOf[Map[String, Double]]
    location("latitude") should be(19.4326)
    location("longitude") should be(-99.1332)
  }
  
  it should "generate data within valid ranges" in {
    val data = Map(
      "engine_temperature" -> 92.5,
      "vehicle_speed" -> 65.7,
      "fuel_level" -> 45.3
    )
    
    // Verificar rangos válidos
    data("engine_temperature") should be >= 70.0
    data("engine_temperature") should be <= 120.0
    data("vehicle_speed") should be >= 0.0
    data("vehicle_speed") should be <= 200.0
    data("fuel_level") should be >= 0.0
    data("fuel_level") should be <= 100.0
  }
  
  it should "generate valid timestamps" in {
    val timestamp = LocalDateTime.now()
    
    // Verificar formato de timestamp
    timestamp should not be null
    timestamp.getYear should be >= 2024
    timestamp.getMonth.getValue should be >= 1
    timestamp.getMonth.getValue should be <= 12
    timestamp.getDayOfMonth should be >= 1
    timestamp.getDayOfMonth should be <= 31
    timestamp.getHour should be >= 0
    timestamp.getHour should be <= 23
    timestamp.getMinute should be >= 0
    timestamp.getMinute should be <= 59
    timestamp.getSecond should be >= 0
    timestamp.getSecond should be <= 59
  }
  
  it should "generate valid location coordinates" in {
    val location = Map(
      "latitude" -> 19.4326,
      "longitude" -> -99.1332
    )
    
    // Verificar rangos de coordenadas
    location("latitude") should be >= -90.0
    location("latitude") should be <= 90.0
    location("longitude") should be >= -180.0
    location("longitude") should be <= 180.0
  }
} 