 create table employee (eid INT, birth timestamp) stored as parquet;
 
-- The parquet file was written with Hive 3.1.3 using the new Date/Time APIs (legacy=false) to
-- convert from US/Pacific to UTC. The presence of writer.time.zone in the metadata of the file
-- allow us to infer that new Date/Time APIS should be used for the conversion. The
-- hive.parquet.timestamp.legacy.conversion.enabled property shouldn't be taken into account in this
-- case.
LOAD DATA LOCAL INPATH '../../data/files/employee_hive_3_1_3_us_pacific.parquet' into table employee;    

-- Read timestamps using the non-vectorized reader
set hive.vectorized.execution.enabled=false;
set hive.parquet.timestamp.legacy.conversion.enabled=true;
select * from employee;
set hive.parquet.timestamp.legacy.conversion.enabled=false;
select * from employee;

-- Read timestamps using the vectorized reader
set hive.vectorized.execution.enabled=true;
-- Disable task conversion to allow vectorization to kick in
set hive.fetch.task.conversion=none;
set hive.parquet.timestamp.legacy.conversion.enabled=true;
select * from employee;
set hive.parquet.timestamp.legacy.conversion.enabled=false;
select * from employee;
