-- =============================================================================
-- PiUPS Breakout Garden IoT Sensor Data - Snowflake Setup
-- Target: IOT_LAB.SENSORS
-- Ingest: Snowpipe Streaming v2 (High Speed REST API)
-- =============================================================================
-- Sensors:
--   BH1745   - RGB Color (red, green, blue, clear)
--   LTR559   - Light (lux) and Proximity
--   VL53L1X  - Time-of-Flight Distance (mm)
--   BME680   - Temperature (C), Humidity (%), Pressure (hPa), Gas resistance (Ohm)
--   LSM303D  - Accelerometer (g) XYZ, Magnetometer XYZ
-- =============================================================================

USE DATABASE IOT_LAB;
USE SCHEMA SENSORS;
USE WAREHOUSE INGEST;

-- Add PiUPS-specific columns to existing BREAKOUT_GARDEN_DATA table
ALTER TABLE IOT_LAB.SENSORS.BREAKOUT_GARDEN_DATA ADD COLUMN IF NOT EXISTS
    bh1745_red          NUMBER(10,1)    COMMENT 'BH1745 red channel raw intensity';
ALTER TABLE IOT_LAB.SENSORS.BREAKOUT_GARDEN_DATA ADD COLUMN IF NOT EXISTS
    bh1745_green        NUMBER(10,1)    COMMENT 'BH1745 green channel raw intensity';
ALTER TABLE IOT_LAB.SENSORS.BREAKOUT_GARDEN_DATA ADD COLUMN IF NOT EXISTS
    bh1745_blue         NUMBER(10,1)    COMMENT 'BH1745 blue channel raw intensity';
ALTER TABLE IOT_LAB.SENSORS.BREAKOUT_GARDEN_DATA ADD COLUMN IF NOT EXISTS
    bh1745_clear        NUMBER(10,1)    COMMENT 'BH1745 clear channel raw intensity';
ALTER TABLE IOT_LAB.SENSORS.BREAKOUT_GARDEN_DATA ADD COLUMN IF NOT EXISTS
    ltr559_lux          NUMBER(8,2)     COMMENT 'LTR559 ambient light in lux';
ALTER TABLE IOT_LAB.SENSORS.BREAKOUT_GARDEN_DATA ADD COLUMN IF NOT EXISTS
    ltr559_proximity    NUMBER(6,0)     COMMENT 'LTR559 proximity sensor value (0-65535)';
ALTER TABLE IOT_LAB.SENSORS.BREAKOUT_GARDEN_DATA ADD COLUMN IF NOT EXISTS
    vl53l1x_distance_mm NUMBER(6,0)     COMMENT 'VL53L1X time-of-flight distance in millimeters';
ALTER TABLE IOT_LAB.SENSORS.BREAKOUT_GARDEN_DATA ADD COLUMN IF NOT EXISTS
    lsm303d_accel_x     NUMBER(8,4)     COMMENT 'LSM303D accelerometer X axis in g';
ALTER TABLE IOT_LAB.SENSORS.BREAKOUT_GARDEN_DATA ADD COLUMN IF NOT EXISTS
    lsm303d_accel_y     NUMBER(8,4)     COMMENT 'LSM303D accelerometer Y axis in g';
ALTER TABLE IOT_LAB.SENSORS.BREAKOUT_GARDEN_DATA ADD COLUMN IF NOT EXISTS
    lsm303d_accel_z     NUMBER(8,4)     COMMENT 'LSM303D accelerometer Z axis in g';
ALTER TABLE IOT_LAB.SENSORS.BREAKOUT_GARDEN_DATA ADD COLUMN IF NOT EXISTS
    lsm303d_mag_x       NUMBER(8,2)     COMMENT 'LSM303D magnetometer X axis';
ALTER TABLE IOT_LAB.SENSORS.BREAKOUT_GARDEN_DATA ADD COLUMN IF NOT EXISTS
    lsm303d_mag_y       NUMBER(8,2)     COMMENT 'LSM303D magnetometer Y axis';
ALTER TABLE IOT_LAB.SENSORS.BREAKOUT_GARDEN_DATA ADD COLUMN IF NOT EXISTS
    lsm303d_mag_z       NUMBER(8,2)     COMMENT 'LSM303D magnetometer Z axis';

-- Recreate the pipe to include new columns
CREATE OR REPLACE PIPE IOT_LAB.SENSORS.BREAKOUT_GARDEN_PIPE AS
COPY INTO IOT_LAB.SENSORS.BREAKOUT_GARDEN_DATA (
    raw_data, uuid, device_id, hostname, ip_address, mac_address, reading_ts,
    latitude, longitude, altitude, gps_satellites, gps_fix_quality, gps_speed_knots,
    heart_rate_bpm, spo2_percent, smoke_particle_level,
    eco2_ppm, tvoc_ppb,
    temperature_c, humidity_percent, pressure_hpa, gas_resistance_ohms,
    gas_oxidising_ohms, gas_reducing_ohms, gas_nh3_ohms,
    cpu_temp_c, cpu_percent, memory_percent, disk_usage_percent,
    bh1745_red, bh1745_green, bh1745_blue, bh1745_clear,
    ltr559_lux, ltr559_proximity,
    vl53l1x_distance_mm,
    lsm303d_accel_x, lsm303d_accel_y, lsm303d_accel_z,
    lsm303d_mag_x, lsm303d_mag_y, lsm303d_mag_z
)
FROM (
    SELECT
        $1                                          AS raw_data,
        $1:uuid::VARCHAR(64)                        AS uuid,
        $1:device_id::VARCHAR(50)                   AS device_id,
        $1:hostname::VARCHAR(100)                   AS hostname,
        $1:ip_address::VARCHAR(45)                  AS ip_address,
        $1:mac_address::VARCHAR(17)                 AS mac_address,
        TO_TIMESTAMP_NTZ($1:reading_ts::VARCHAR)    AS reading_ts,
        $1:latitude::NUMBER(10,7)                   AS latitude,
        $1:longitude::NUMBER(11,7)                  AS longitude,
        $1:altitude::NUMBER(8,2)                    AS altitude,
        $1:gps_satellites::NUMBER(3,0)              AS gps_satellites,
        $1:gps_fix_quality::NUMBER(1,0)             AS gps_fix_quality,
        $1:gps_speed_knots::NUMBER(6,2)             AS gps_speed_knots,
        $1:heart_rate_bpm::NUMBER(5,1)              AS heart_rate_bpm,
        $1:spo2_percent::NUMBER(5,1)                AS spo2_percent,
        $1:smoke_particle_level::NUMBER(10,1)       AS smoke_particle_level,
        $1:eco2_ppm::NUMBER(6,1)                    AS eco2_ppm,
        $1:tvoc_ppb::NUMBER(6,1)                    AS tvoc_ppb,
        $1:temperature_c::NUMBER(6,2)               AS temperature_c,
        $1:humidity_percent::NUMBER(5,2)            AS humidity_percent,
        $1:pressure_hpa::NUMBER(7,2)                AS pressure_hpa,
        $1:gas_resistance_ohms::NUMBER(10,0)        AS gas_resistance_ohms,
        $1:gas_oxidising_ohms::NUMBER(10,1)         AS gas_oxidising_ohms,
        $1:gas_reducing_ohms::NUMBER(10,1)          AS gas_reducing_ohms,
        $1:gas_nh3_ohms::NUMBER(10,1)               AS gas_nh3_ohms,
        $1:cpu_temp_c::NUMBER(5,1)                  AS cpu_temp_c,
        $1:cpu_percent::NUMBER(5,1)                 AS cpu_percent,
        $1:memory_percent::NUMBER(5,1)              AS memory_percent,
        $1:disk_usage_percent::NUMBER(5,1)          AS disk_usage_percent,
        $1:bh1745_red::NUMBER(10,1)                 AS bh1745_red,
        $1:bh1745_green::NUMBER(10,1)               AS bh1745_green,
        $1:bh1745_blue::NUMBER(10,1)                AS bh1745_blue,
        $1:bh1745_clear::NUMBER(10,1)               AS bh1745_clear,
        $1:ltr559_lux::NUMBER(8,2)                  AS ltr559_lux,
        $1:ltr559_proximity::NUMBER(6,0)            AS ltr559_proximity,
        $1:vl53l1x_distance_mm::NUMBER(6,0)         AS vl53l1x_distance_mm,
        $1:lsm303d_accel_x::NUMBER(8,4)             AS lsm303d_accel_x,
        $1:lsm303d_accel_y::NUMBER(8,4)             AS lsm303d_accel_y,
        $1:lsm303d_accel_z::NUMBER(8,4)             AS lsm303d_accel_z,
        $1:lsm303d_mag_x::NUMBER(8,2)              AS lsm303d_mag_x,
        $1:lsm303d_mag_y::NUMBER(8,2)              AS lsm303d_mag_y,
        $1:lsm303d_mag_z::NUMBER(8,2)              AS lsm303d_mag_z
    FROM TABLE(DATA_SOURCE(TYPE => 'STREAMING'))
);

-- View: Latest readings per device (includes all sensor columns)
CREATE OR REPLACE VIEW IOT_LAB.SENSORS.V_PIUPS_LATEST AS
SELECT *
FROM IOT_LAB.SENSORS.BREAKOUT_GARDEN_DATA
WHERE bh1745_red IS NOT NULL OR ltr559_lux IS NOT NULL OR vl53l1x_distance_mm IS NOT NULL
QUALIFY ROW_NUMBER() OVER (PARTITION BY device_id ORDER BY reading_ts DESC) = 1;

-- View: Hourly aggregates for PiUPS sensors
CREATE OR REPLACE VIEW IOT_LAB.SENSORS.V_PIUPS_HOURLY AS
SELECT
    device_id,
    DATE_TRUNC('HOUR', reading_ts)  AS hour_ts,
    COUNT(*)                        AS reading_count,
    -- Weather
    AVG(temperature_c)              AS avg_temperature_c,
    MIN(temperature_c)              AS min_temperature_c,
    MAX(temperature_c)              AS max_temperature_c,
    AVG(humidity_percent)           AS avg_humidity_percent,
    AVG(pressure_hpa)               AS avg_pressure_hpa,
    AVG(gas_resistance_ohms)        AS avg_gas_resistance_ohms,
    -- Light
    AVG(ltr559_lux)                 AS avg_lux,
    MAX(ltr559_lux)                 AS max_lux,
    AVG(ltr559_proximity)           AS avg_proximity,
    -- Distance
    AVG(vl53l1x_distance_mm)        AS avg_distance_mm,
    MIN(vl53l1x_distance_mm)        AS min_distance_mm,
    MAX(vl53l1x_distance_mm)        AS max_distance_mm,
    -- Color
    AVG(bh1745_red)                 AS avg_red,
    AVG(bh1745_green)               AS avg_green,
    AVG(bh1745_blue)                AS avg_blue,
    AVG(bh1745_clear)               AS avg_clear,
    -- Motion (accelerometer magnitude)
    AVG(SQRT(POWER(lsm303d_accel_x, 2) + POWER(lsm303d_accel_y, 2) + POWER(lsm303d_accel_z, 2)))
                                    AS avg_accel_magnitude,
    MAX(SQRT(POWER(lsm303d_accel_x, 2) + POWER(lsm303d_accel_y, 2) + POWER(lsm303d_accel_z, 2)))
                                    AS max_accel_magnitude,
    -- System
    AVG(cpu_temp_c)                 AS avg_cpu_temp_c,
    MAX(cpu_percent)                AS max_cpu_percent,
    MAX(memory_percent)             AS max_memory_percent
FROM IOT_LAB.SENSORS.BREAKOUT_GARDEN_DATA
WHERE bh1745_red IS NOT NULL OR ltr559_lux IS NOT NULL OR vl53l1x_distance_mm IS NOT NULL
GROUP BY device_id, DATE_TRUNC('HOUR', reading_ts);

-- View: Anomaly detection (rule-based)
CREATE OR REPLACE VIEW IOT_LAB.SENSORS.V_PIUPS_ANOMALIES AS
SELECT
    reading_ts,
    device_id,
    temperature_c,
    humidity_percent,
    ltr559_lux,
    vl53l1x_distance_mm,
    cpu_temp_c,
    SQRT(POWER(lsm303d_accel_x, 2) + POWER(lsm303d_accel_y, 2) + POWER(lsm303d_accel_z, 2))
        AS accel_magnitude,
    CASE
        WHEN temperature_c > 40 OR temperature_c < 5 THEN 'TEMPERATURE_EXTREME'
        WHEN humidity_percent > 90 THEN 'HIGH_HUMIDITY'
        WHEN cpu_temp_c > 80 THEN 'CPU_OVERHEAT'
        WHEN ltr559_lux > 10000 THEN 'EXTREME_LIGHT'
        WHEN vl53l1x_distance_mm < 30 THEN 'PROXIMITY_ALERT'
        WHEN SQRT(POWER(lsm303d_accel_x, 2) + POWER(lsm303d_accel_y, 2) + POWER(lsm303d_accel_z, 2)) > 2.0
            THEN 'MOTION_DETECTED'
        ELSE NULL
    END AS anomaly_type
FROM IOT_LAB.SENSORS.BREAKOUT_GARDEN_DATA
WHERE (bh1745_red IS NOT NULL OR ltr559_lux IS NOT NULL)
  AND (
    temperature_c > 40 OR temperature_c < 5
    OR humidity_percent > 90
    OR cpu_temp_c > 80
    OR ltr559_lux > 10000
    OR vl53l1x_distance_mm < 30
    OR SQRT(POWER(lsm303d_accel_x, 2) + POWER(lsm303d_accel_y, 2) + POWER(lsm303d_accel_z, 2)) > 2.0
  );

-- Alert: Data gap detection for PiUPS devices
CREATE OR REPLACE ALERT IOT_LAB.SENSORS.PIUPS_DATA_GAP_ALERT
    WAREHOUSE = INGEST
    SCHEDULE = '5 MINUTE'
IF (EXISTS (
    SELECT 1
    FROM IOT_LAB.SENSORS.BREAKOUT_GARDEN_DATA
    WHERE reading_ts > DATEADD('MINUTE', -10, CURRENT_TIMESTAMP())
      AND (bh1745_red IS NOT NULL OR ltr559_lux IS NOT NULL)
    QUALIFY DATEDIFF('SECOND', reading_ts,
        LEAD(reading_ts) OVER (PARTITION BY device_id ORDER BY reading_ts)) > 60
))
THEN
    CALL SYSTEM$SEND_EMAIL(
        'piups_alerts',
        'tspann@gmail.com',
        'PiUPS: Data Gap Detected',
        'A gap > 60 seconds was detected in PiUPS sensor data. Check device connectivity.'
    );

-- Cost monitoring: Resource budget for streaming warehouse
-- CREATE OR REPLACE RESOURCE MONITOR IOT_INGEST_MONITOR
--     WITH CREDIT_QUOTA = 10
--     FREQUENCY = DAILY
--     START_TIMESTAMP = CURRENT_TIMESTAMP()
--     TRIGGERS
--         ON 75 PERCENT DO NOTIFY
--         ON 90 PERCENT DO NOTIFY
--         ON 100 PERCENT DO SUSPEND;
-- ALTER WAREHOUSE INGEST SET RESOURCE_MONITOR = IOT_INGEST_MONITOR;
