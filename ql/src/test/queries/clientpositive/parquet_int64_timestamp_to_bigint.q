-- The file hive_26612.parquet has the following schema:
-- {
--   "type" : "record",
--   "name" : "spark_schema",
--   "fields" : [ {
--     "name" : "typeid",
--     "type" : "int"
--   }, {
--     "name" : "eventtime",
--     "type" : [ "null", {
--       "type" : "long",
--       "logicalType" : "timestamp-millis"
--     } ],
--     "default" : null
--   } ]
-- }
-- It was created from Spark with the steps documented in HIVE-26612
CREATE TABLE ts_as_bigint_pq (typeid int, eventtime BIGINT) STORED AS PARQUET;

LOAD DATA LOCAL INPATH '../../data/files/hive_26612.parquet' into table ts_as_bigint_pq;

SELECT * FROM ts_as_bigint_pq;
