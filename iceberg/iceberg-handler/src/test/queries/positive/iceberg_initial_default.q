CREATE TABLE ice_parq (
  id INT)
STORED BY ICEBERG  stored as parquet
TBLPROPERTIES ('format-version'='3');

INSERT INTO ice_parq (id) VALUES (1);

ALTER TABLE ice_parq ADD COLUMNS (point STRUCT<x:INT, y:INT> DEFAULT 'x:100,y:99',
  name STRING DEFAULT 'unknown',
  age INT DEFAULT 25,
  salary DOUBLE DEFAULT 50000.0,
  is_active BOOLEAN DEFAULT TRUE,
  created_date DATE DEFAULT '2024-01-01',
  created_ts TIMESTAMP DEFAULT '2024-01-01T10:00:00',
  score DECIMAL(5,2) DEFAULT 100.00,
  category STRING DEFAULT 'general');

INSERT INTO ice_parq (id) VALUES (2);

SELECT * FROM ice_parq ORDER BY id;

-- change default of a field of Struct column
ALTER TABLE ice_parq CHANGE COLUMN point point STRUCT<x:INT, y:INT> DEFAULT 'x:100,y:88';

-- rename and change default value of age column
ALTER TABLE ice_parq CHANGE COLUMN age age_new int DEFAULT 21;

INSERT INTO ice_parq (id) VALUES (3);

SELECT * FROM ice_parq ORDER BY id;

-- AVRO table
CREATE TABLE ice_avro (
  id INT)
STORED BY ICEBERG  stored as avro
TBLPROPERTIES ('format-version'='3');

INSERT INTO ice_avro (id) VALUES (1);

ALTER TABLE ice_avro ADD COLUMNS (point STRUCT<x:INT, y:INT> DEFAULT 'x:100,y:99',
  name STRING DEFAULT 'unknown',
  age INT DEFAULT 25,
  salary DOUBLE DEFAULT 50000.0,
  is_active BOOLEAN DEFAULT TRUE,
  created_date DATE DEFAULT '2024-01-01',
  created_ts TIMESTAMP DEFAULT '2024-01-01T10:00:00',
  score DECIMAL(5,2) DEFAULT 100.00,
  category STRING DEFAULT 'general');

INSERT INTO ice_avro (id) VALUES (2);

SELECT * FROM ice_avro ORDER BY id;

-- change default of a field of Struct column
ALTER TABLE ice_avro CHANGE COLUMN point point STRUCT<x:INT, y:INT> DEFAULT 'x:100,y:88';

-- rename and change default value of age column
ALTER TABLE ice_avro CHANGE COLUMN age age_new int DEFAULT 21;

INSERT INTO ice_avro (id) VALUES (3);

SELECT * FROM ice_avro ORDER BY id;

-- ORC table
CREATE TABLE ice_orc (
  id INT)
STORED BY ICEBERG stored as orc
TBLPROPERTIES ('format-version'='3');

INSERT INTO ice_orc (id) VALUES (1);

ALTER TABLE ice_orc ADD COLUMNS (point STRUCT<x:INT, y:INT> DEFAULT 'x:100,y:99',
  name STRING DEFAULT 'unknown',
  age INT DEFAULT 25,
  salary DOUBLE DEFAULT 50000.0,
  is_active BOOLEAN DEFAULT TRUE,
  created_date DATE DEFAULT '2024-01-01',
  created_ts TIMESTAMP DEFAULT '2024-01-01T10:00:00',
  score DECIMAL(5,2) DEFAULT 100.00,
  category STRING DEFAULT 'general');

INSERT INTO ice_orc (id) VALUES (2);

SELECT * FROM ice_orc ORDER BY id;

-- change default of a field of Struct column
ALTER TABLE ice_orc CHANGE COLUMN point point STRUCT<x:INT, y:INT> DEFAULT 'x:100,y:88';

-- rename and change default value of age column
ALTER TABLE ice_orc CHANGE COLUMN age age_new int DEFAULT 21;

INSERT INTO ice_orc (id) VALUES (3);

SELECT * FROM ice_orc ORDER BY id;

-- disable vectorization
set hive.vectorized.execution.enabled=false;
SELECT * FROM ice_parq ORDER BY id;
SELECT * FROM ice_avro ORDER BY id;
SELECT * FROM ice_orc ORDER BY id;