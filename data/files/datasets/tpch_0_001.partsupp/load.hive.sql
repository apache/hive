CREATE DATABASE IF NOT EXISTS tpch_0_001;

CREATE TABLE tpch_0_001.partsupp
(
    PS_PARTKEY    INT NOT NULL, -- references P_PARTKEY
    PS_SUPPKEY    INT NOT NULL, -- references S_SUPPKEY
    PS_AVAILQTY   INT,
    PS_SUPPLYCOST DECIMAL,
    PS_COMMENT    VARCHAR(199)
)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '|';

LOAD DATA LOCAL INPATH '${hiveconf:test.data.dir}/tpch/sf0_001/partsupp.tbl.bz2' overwrite into table tpch_0_001.partsupp;

analyze table tpch_0_001.partsupp compute statistics;
analyze table tpch_0_001.partsupp compute statistics for columns;
