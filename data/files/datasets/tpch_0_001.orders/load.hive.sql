CREATE DATABASE IF NOT EXISTS tpch_0_001;

CREATE TABLE tpch_0_001.ORDERS
(
    O_ORDERKEY      INT NOT NULL,
    O_CUSTKEY       INT NOT NULL, -- references C_CUSTKEY
    O_ORDERSTATUS   CHAR(1),
    O_TOTALPRICE    DECIMAL,
    O_ORDERDATE     DATE,
    O_ORDERPRIORITY CHAR(15),
    O_CLERK         CHAR(15),
    O_SHIPPRIORITY  INT,
    O_COMMENT       VARCHAR(79)
)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '|';

LOAD DATA LOCAL INPATH '${hiveconf:test.data.dir}/tpch/sf0_001/orders.tbl.bz2' overwrite into table tpch_0_001.orders;

analyze table tpch_0_001.orders compute statistics;
analyze table tpch_0_001.orders compute statistics for columns;
