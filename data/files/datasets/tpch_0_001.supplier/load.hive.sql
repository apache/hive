CREATE DATABASE IF NOT EXISTS tpch_0_001;

CREATE TABLE tpch_0_001.SUPPLIER
(
    S_SUPPKEY   INT NOT NULL,
    S_NAME      CHAR(25),
    S_ADDRESS   VARCHAR(40),
    S_NATIONKEY INT NOT NULL, -- references N_NATIONKEY
    S_PHONE     CHAR(15),
    S_ACCTBAL   DECIMAL,
    S_COMMENT   VARCHAR(101)
)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '|';

LOAD DATA LOCAL INPATH '${hiveconf:test.data.dir}/tpch/sf0_001/supplier.tbl.bz2' overwrite into table tpch_0_001.supplier;

analyze table tpch_0_001.supplier compute statistics;
analyze table tpch_0_001.supplier compute statistics for columns;
