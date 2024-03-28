--! qt:replace:/((rawData)Size\s+)[0-9]{2,}/$1__SOME_NUMBER__/

set hive.vectorized.execution.enabled=false;

CREATE TABLE parquet_create_people_staging (
  id int,
  first_name string,
  last_name string,
  address string,
  salary decimal,
  start_date timestamp,
  state string);

LOAD DATA LOCAL INPATH '../../data/files/parquet_create_people.txt' OVERWRITE INTO TABLE parquet_create_people_staging;

CREATE TABLE parquet_create_people (
  id int,
  first_name string,
  last_name string,
  address string,
  salary decimal,
  start_date timestamp,
  state string)
STORED AS parquet;

INSERT OVERWRITE TABLE parquet_create_people SELECT * FROM parquet_create_people_staging ORDER BY id;

-- describe the table first. This should contain un-updated stats.
DESC FORMATTED parquet_create_people;

-- now run noscan and re-check the stats, and they should be updated.
ANALYZE TABLE parquet_create_people COMPUTE STATISTICS noscan;
DESC FORMATTED parquet_create_people;

-- clean up
DROP TABLE parquet_create_people_staging;
DROP TABLE parquet_create_people;
