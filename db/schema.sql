-- Criação dos schemas (se ainda não existirem)
CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;

-- Criação do tipo ENUM para log_level
CREATE TYPE IF NOT EXISTS log_level AS ENUM ('ERROR','WARNING');

-- Tabelas do schema bronze
CREATE TABLE IF NOT EXISTS bronze.equipment_failure_sensors (
  id            VARCHAR        PRIMARY KEY,
  type_msg      log_level      NOT NULL,  
  sensor_id     BIGINT         NOT NULL,
  temperature   REAL           NOT NULL, 
  vibration     REAL           NOT NULL, 
  ts_utc        TIMESTAMP      NOT NULL
);

CREATE TABLE IF NOT EXISTS bronze.equipment (
  equipment_id  BIGINT         PRIMARY KEY,
  name          VARCHAR        NOT NULL,  
  group_name    VARCHAR        NOT NULL
);

CREATE TABLE IF NOT EXISTS bronze.equipment_sensors (
  sensor_id     BIGINT         PRIMARY KEY,
  equipment_id  BIGINT         NOT NULL
);

-- Tabela do schema silver
CREATE TABLE IF NOT EXISTS silver.equipment_failure_sensors (
  id            VARCHAR        PRIMARY KEY,
  equipment_id  BIGINT         NOT NULL,
  sensor_id     BIGINT         NOT NULL,
  name          VARCHAR        NOT NULL,  
  group_name    VARCHAR        NOT NULL,
  type_msg      log_level      NOT NULL,  
  temperature   REAL           NOT NULL, 
  vibration     REAL           NOT NULL, 
  ts_utc        TIMESTAMP      NOT NULL
);

-- Tabelas do schema gold
CREATE TABLE IF NOT EXISTS gold.equipment_failures_summary (
  equipment_id                  BIGINT             NOT NULL,
  name                          VARCHAR            NOT NULL,
  group_name                    VARCHAR            NOT NULL,
  total_failures                BIGINT,
  avg_failures_per_asset        DOUBLE PRECISION,
  sensor_id                     BIGINT,
  sensor_failures               BIGINT,
  sensor_rank_by_equipment      BIGINT,
  sensor_rank_by_equipment_group BIGINT
);
