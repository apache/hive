set hive.stats.column.autogather=true;
set hive.mapred.mode=nonstrict;
set hive.explain.user=false;
SET hive.auto.convert.join=true;
SET hive.auto.convert.join.noconditionaltask=true;
SET hive.auto.convert.join.noconditionaltask.size=10000000000;
SET hive.vectorized.execution.enabled=true;
set hive.fetch.task.conversion=none;

SET mapred.min.split.size=10000;
SET mapred.max.split.size=50000;

SET hive.optimize.scan.probedecode=true;

CREATE TABLE lineitem_trs (
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
) STORED AS ORC;


CREATE TABLE unique_lineitem_stage (
  l_orderkey STRING,
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
) STORED AS ORC;



-- l_orderkey Key types missmatch thus will be converted (from String and Bigint to Double) using a UDF Expr
-- Probe compiler optimization will SKIP such MJs with keys defined as Exprs (each expr will need to be evaluated before probe)
EXPLAIN VECTORIZATION DETAIL
SELECT * FROM
unique_lineitem_stage, lineitem_trs
where unique_lineitem_stage.L_ORDERKEY = lineitem_trs.L_ORDERKEY;
