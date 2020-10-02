CREATE DATABASE IF NOT EXISTS tpch_0_001;

CREATE TABLE tpch_0_001.NATION
(
    N_NATIONKEY INT NOT NULL,
    N_NAME      CHAR(25),
    N_REGIONKEY INT NOT NULL, -- references R_REGIONKEY
    N_COMMENT   VARCHAR(152)
)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '|';

LOAD DATA LOCAL INPATH '${hiveconf:test.data.dir}/tpch/sf0_001/nation.tbl.bz2' overwrite into table tpch_0_001.nation;

analyze table tpch_0_001.nation compute statistics;
analyze table tpch_0_001.nation compute statistics for columns;
