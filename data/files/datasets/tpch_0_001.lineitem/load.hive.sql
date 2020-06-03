CREATE DATABASE IF NOT EXISTS tpch_0_001;

CREATE TABLE tpch_0_001.lineitem (L_ORDERKEY      INT,
                                L_PARTKEY       INT,
                                L_SUPPKEY       INT,
                                L_LINENUMBER    INT,
                                L_QUANTITY      DOUBLE,
                                L_EXTENDEDPRICE DOUBLE,
                                L_DISCOUNT      DOUBLE,
                                L_TAX           DOUBLE,
                                L_RETURNFLAG    STRING,
                                L_LINESTATUS    STRING,
                                l_shipdate      STRING,
                                L_COMMITDATE    STRING,
                                L_RECEIPTDATE   STRING,
                                L_SHIPINSTRUCT  STRING,
                                L_SHIPMODE      STRING,
                                L_COMMENT       STRING)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|';

LOAD DATA LOCAL INPATH '${hiveconf:test.data.dir}/tpch/sf0_001/lineitem.tbl.bz2' OVERWRITE INTO TABLE tpch_0_001.lineitem;

analyze table tpch_0_001.lineitem compute statistics;
analyze table tpch_0_001.lineitem compute statistics for columns;
