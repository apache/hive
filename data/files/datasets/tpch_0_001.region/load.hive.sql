CREATE DATABASE IF NOT EXISTS tpch_0_001;

CREATE TABLE tpch_0_001.REGION
(
    R_REGIONKEY INT NOT NULL,
    R_NAME      CHAR(25),
    R_COMMENT   VARCHAR(152)
)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '|';

LOAD DATA LOCAL INPATH '${hiveconf:test.data.dir}/tpch/sf0_001/region.tbl.bz2' overwrite into table tpch_0_001.region;

analyze table tpch_0_001.region compute statistics;
analyze table tpch_0_001.region compute statistics for columns;
