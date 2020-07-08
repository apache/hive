set hive.stats.column.autogather=false;
set hive.mapred.mode=nonstrict;
set hive.explain.user=false;
SET hive.auto.convert.join=true;
SET hive.auto.convert.join.noconditionaltask=true;
SET hive.auto.convert.join.noconditionaltask.size=1000000000;
SET hive.vectorized.execution.enabled=true;
set hive.fetch.task.conversion=none;

SET mapred.min.split.size=10000;
SET mapred.max.split.size=50000;
set hive.support.concurrency=true;
set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;

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
) STORED AS ORC
TBLPROPERTIES('transactional'='true');


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
-- Probe complier optimization will SKIP such MJs with keys defined as Exprs (each expr will need to be evaluated before probe)
EXPLAIN VECTORIZATION DETAIL
merge into lineitem_trs using (
  select * from lineitem_trs
  where L_ORDERKEY in(
    select L_ORDERKEY
    from unique_lineitem_stage
  )
) sub on sub.L_ORDERKEY = lineitem_trs.L_ORDERKEY
  when matched then update set l_linenumber=8
  when not matched then insert values (
    sub.L_ORDERKEY, sub.L_PARTKEY, sub.L_SUPPKEY, sub.L_LINENUMBER, sub.L_QUANTITY, sub.L_EXTENDEDPRICE,
    sub.L_DISCOUNT, sub.L_TAX, sub.L_RETURNFLAG, sub.L_LINESTATUS, sub.L_SHIPDATE, sub.L_COMMITDATE,
    sub.L_RECEIPTDATE, sub.L_SHIPINSTRUCT,sub.L_SHIPMODE, sub.L_COMMENT
  );
