CREATE TABLE ice_t (
  id INT)
STORED BY ICEBERG
TBLPROPERTIES ('format-version'='3');

INSERT INTO ice_t (id) VALUES (1);

ALTER TABLE ice_t ADD COLUMNS (point STRUCT<x:INT, y:INT> DEFAULT 'x:100,y:99',
  name STRING DEFAULT 'unknown',
  age INT DEFAULT 25,
  salary DOUBLE DEFAULT 50000.0,
  is_active BOOLEAN DEFAULT TRUE,
  created_date DATE DEFAULT '2024-01-01',
  created_ts TIMESTAMP DEFAULT '2024-01-01T10:00:00',
  score DECIMAL(5,2) DEFAULT 100.00,
  category STRING DEFAULT 'general');

INSERT INTO ice_t (id) VALUES (2);

SELECT * FROM ice_t ORDER BY id;

ALTER TABLE ice_t REPLACE COLUMNS (id INT,
  point STRUCT<x:INT, y:INT> DEFAULT 'x:100,y:99',
  name STRING DEFAULT 'unknown',
  age INT DEFAULT 25,
  salary DOUBLE DEFAULT 50000.0,
  is_active BOOLEAN DEFAULT TRUE,
  created_date DATE DEFAULT '2024-01-01',
  created_ts TIMESTAMP DEFAULT '2024-01-01T10:00:00',
  category STRING DEFAULT 'general');

SELECT * FROM ice_t ORDER BY id;

-- change default of a field of Struct column
ALTER TABLE ice_t CHANGE COLUMN point point STRUCT<x:INT, y:INT> DEFAULT 'x:100,y:88';

-- rename and change default value of age column
ALTER TABLE ice_t CHANGE COLUMN age age_new int DEFAULT 21;

INSERT INTO ice_t (id) VALUES (3);

SELECT * FROM ice_t ORDER BY id;

-- Rename the struct column with default changes
ALTER TABLE ice_t CHANGE COLUMN point point_new STRUCT<x:INT, y:INT> DEFAULT 'x:55,y:88';

INSERT INTO ice_t (id) VALUES (4);

SELECT * FROM ice_t ORDER BY id;