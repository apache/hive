CREATE DATABASE IF NOT EXISTS tpch_0_001;

CREATE TABLE tpch_0_001.part(
    p_partkey INT NOT NULL,
    p_name STRING,
    p_mfgr STRING,
    p_brand STRING,
    p_type STRING,
    p_size INT,
    p_container STRING,
    p_retailprice DOUBLE,
    p_comment STRING
)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '|';

LOAD DATA LOCAL INPATH '${hiveconf:test.data.dir}/tpch/sf0_001/part.tbl.bz2' overwrite into table tpch_0_001.part;

analyze table tpch_0_001.part compute statistics;
analyze table tpch_0_001.part compute statistics for columns;
