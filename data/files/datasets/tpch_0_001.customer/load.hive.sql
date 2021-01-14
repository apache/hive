CREATE DATABASE IF NOT EXISTS tpch_0_001;

CREATE TABLE tpch_0_001.CUSTOMER
(
    C_CUSTKEY    INT NOT NULL,
    C_NAME       VARCHAR(25),
    C_ADDRESS    VARCHAR(40),
    C_NATIONKEY  INT NOT NULL, -- references N_NATIONKEY
    C_PHONE      CHAR(15),
    C_ACCTBAL    DECIMAL,
    C_MKTSEGMENT CHAR(10),
    C_COMMENT    VARCHAR(117)
)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '|';

LOAD DATA LOCAL INPATH '${hiveconf:test.data.dir}/tpch/sf0_001/customer.tbl.bz2' overwrite into table tpch_0_001.customer;

analyze table tpch_0_001.customer compute statistics;
analyze table tpch_0_001.customer compute statistics for columns;
