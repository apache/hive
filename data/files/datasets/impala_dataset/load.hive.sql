DROP TABLE IF EXISTS impala_tpch_customer;
CREATE TABLE impala_tpch_customer (
  c_custkey BIGINT,
  c_name STRING,
  c_address STRING,
  c_nationkey SMALLINT,
  c_phone STRING,
  c_acctbal DECIMAL(12,2),
  c_mktsegment STRING,
  c_comment STRING
) STORED AS PARQUET
TBLPROPERTIES ("transactional"="false");

DROP TABLE IF EXISTS impala_tpch_lineitem;
CREATE TABLE impala_tpch_lineitem (
   l_orderkey BIGINT,
   l_partkey BIGINT,
   l_suppkey BIGINT,
   l_linenumber INT,
   l_quantity DECIMAL(12,2),
   l_extendedprice DECIMAL(12,2),
   l_discount DECIMAL(12,2),
   l_tax DECIMAL(12,2),
   l_returnflag STRING,
   l_linestatus STRING,
   l_shipdate STRING,
   l_commitdate STRING,
   l_receiptdate STRING,
   l_shipinstruct STRING,
   l_shipmode STRING,
   l_comment STRING
) STORED AS PARQUET
TBLPROPERTIES ("transactional"="false");

DROP TABLE IF EXISTS impala_tpch_nation;
CREATE TABLE impala_tpch_nation (
  n_nationkey SMALLINT,
  n_name STRING,
  n_regionkey SMALLINT,
  n_comment STRING
) STORED AS PARQUET
TBLPROPERTIES ("transactional"="false");

DROP TABLE IF EXISTS impala_tpch_orders;
CREATE TABLE impala_tpch_orders (
  o_orderkey BIGINT,
  o_custkey BIGINT,
  o_orderstatus STRING,
  o_totalprice DECIMAL(12,2),
  o_orderdate STRING,
  o_orderpriority STRING,
  o_clerk STRING,
  o_shippriority INT,
  o_comment STRING
) STORED AS PARQUET
TBLPROPERTIES ("transactional"="false");

DROP TABLE IF EXISTS impala_tpch_part;
CREATE TABLE impala_tpch_part (
  p_partkey BIGINT,
  p_name STRING,
  p_mfgr STRING,
  p_brand STRING,
  p_type STRING,
  p_size INT,
  p_container STRING,
  p_retailprice DECIMAL(12,2),
  p_comment STRING
) STORED AS PARQUET
TBLPROPERTIES ("transactional"="false");

DROP TABLE IF EXISTS impala_tpch_partsupp;
CREATE TABLE impala_tpch_partsupp (
  ps_partkey BIGINT,
  ps_suppkey BIGINT,
  ps_availqty INT,
  ps_supplycost DECIMAL(12,2),
  ps_comment STRING
) STORED AS PARQUET
TBLPROPERTIES ("transactional"="false");

DROP TABLE IF EXISTS impala_tpch_region;
CREATE TABLE impala_tpch_region (
  r_regionkey SMALLINT,
  r_name STRING,
  r_comment STRING
) STORED AS PARQUET
TBLPROPERTIES ("transactional"="false");

DROP TABLE IF EXISTS impala_tpch_supplier;
CREATE TABLE impala_tpch_supplier (
  s_suppkey BIGINT,
  s_name STRING,
  s_address STRING,
  s_nationkey SMALLINT,
  s_phone STRING,
  s_acctbal DECIMAL(12,2),
  s_comment STRING
) STORED AS PARQUET
TBLPROPERTIES ("transactional"="false");
