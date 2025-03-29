-- 1. Detección de Anomalías de Temperatura
SELECT
    vehicle_id,
    AVG(engine_temperature) as avg_temp,
    MAX(engine_temperature) as max_temp,
    MIN(engine_temperature) as min_temp,
    STDDEV(engine_temperature) as std_temp
FROM
    vehicle_telemetry
WHERE
    date_partition = '2025-03-25'
GROUP BY
    vehicle_id
HAVING
    max_temp > 105 OR std_temp > 10
ORDER BY
    max_temp DESC
LIMIT 20;

-- 2. Análisis de Patrones de Conducción
SELECT
    FLOOR(vehicle_speed/10)*10 as speed_range,
    COUNT(*) as frequency,
    AVG(fuel_level) as avg_fuel_level,
    AVG(engine_temperature) as avg_engine_temp
FROM
    vehicle_telemetry
WHERE
    date_partition BETWEEN '2025-03-20' AND '2025-03-25'
    AND vehicle_id IN ('VIN-123456789', 'VIN-987654321')
GROUP BY
    FLOOR(vehicle_speed/10)*10
ORDER BY
    speed_range;

-- 3. Identificación de Vehículos para Mantenimiento Preventivo
WITH
vehicle_stats AS (
    SELECT
        vehicle_id,
        COUNT(*) as data_points,
        AVG(engine_temperature) as avg_temp,
        MAX(engine_temperature) as max_temp,
        COUNT(CASE WHEN engine_temperature > 100 THEN 1 END) as high_temp_events,
        AVG(vehicle_speed) as avg_speed,
        MAX(vehicle_speed) as max_speed,
        MIN(fuel_level) as min_fuel
    FROM
        vehicle_telemetry
    WHERE
        date_partition BETWEEN '2025-03-01' AND '2025-03-25'
    GROUP BY
        vehicle_id
)
SELECT
    vehicle_id,
    avg_temp,
    max_temp,
    high_temp_events,
    avg_speed,
    max_speed,
    min_fuel,
    (CASE
        WHEN high_temp_events > 5 AND max_temp > 110 THEN 'URGENT'
        WHEN high_temp_events > 2 OR max_temp > 105 THEN 'HIGH'
        WHEN avg_temp > 95 THEN 'MEDIUM'
        ELSE 'LOW'
    END) as maintenance_priority
FROM
    vehicle_stats
WHERE
    high_temp_events > 0 OR max_temp > 100 OR min_fuel < 10
ORDER BY
    high_temp_events DESC, max_temp DESC;

-- 4. Análisis de Eficiencia de Combustible
SELECT
    vehicle_id,
    date_partition,
    AVG(vehicle_speed) as avg_speed,
    AVG(fuel_level) as avg_fuel,
    COUNT(*) as readings_count,
    (MAX(fuel_level) - MIN(fuel_level)) as fuel_consumption
FROM
    vehicle_telemetry
WHERE
    date_partition BETWEEN '2025-03-20' AND '2025-03-25'
GROUP BY
    vehicle_id, date_partition
HAVING
    readings_count > 100
ORDER BY
    date_partition, vehicle_id;

-- 5. Detección de Comportamientos Anómalos
SELECT
    vehicle_id,
    date_partition,
    COUNT(*) as total_readings,
    COUNT(CASE WHEN engine_temperature > 95 THEN 1 END) as high_temp_count,
    COUNT(CASE WHEN vehicle_speed > 120 THEN 1 END) as speeding_count,
    COUNT(CASE WHEN fuel_level < 20 THEN 1 END) as low_fuel_count,
    (COUNT(CASE WHEN engine_temperature > 95 THEN 1 END) +
     COUNT(CASE WHEN vehicle_speed > 120 THEN 1 END) +
     COUNT(CASE WHEN fuel_level < 20 THEN 1 END)) as total_anomalies
FROM
    vehicle_telemetry
WHERE
    date_partition BETWEEN '2025-03-20' AND '2025-03-25'
GROUP BY
    vehicle_id, date_partition
HAVING
    total_anomalies > 5
ORDER BY
    total_anomalies DESC; 