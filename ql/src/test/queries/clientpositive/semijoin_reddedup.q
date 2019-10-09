--! qt:dataset:lineitem
--! qt:dataset:part
--! qt:dataset:src

set hive.support.concurrency=true;
set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;
--set hive.compute.query.using.stats=false;
set hive.mapred.mode=nonstrict;
set hive.explain.user=false;
set hive.optimize.ppd=true;
set hive.ppd.remove.duplicatefilters=true;
set hive.tez.dynamic.partition.pruning=true;
set hive.tez.dynamic.semijoin.reduction=true;
set hive.optimize.metadataonly=false;
set hive.optimize.index.filter=true;
set hive.stats.autogather=true;
set hive.tez.bigtable.minsize.semijoin.reduction=1;
set hive.tez.min.bloom.filter.entries=1;
set hive.stats.fetch.column.stats=true;
set hive.tez.bloom.filter.factor=1.0f;
set hive.auto.convert.join=false;
set hive.optimize.shared.work=false;
set hive.stats.filter.range.uniform=false;


create database tpch_test;
use tpch_test;

CREATE TABLE `customer`(
  `c_custkey` bigint, 
  `c_name` string, 
  `c_address` string, 
  `c_nationkey` bigint, 
  `c_phone` string, 
  `c_acctbal` double, 
  `c_mktsegment` string, 
  `c_comment` string)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.orc.OrcSerde' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'
TBLPROPERTIES (
  'bucketing_version'='2', 
  'transactional'='true', 
  'transactional_properties'='default', 
  'transient_lastDdlTime'='1543026723');

CREATE TABLE `lineitem`(
  `l_orderkey` bigint, 
  `l_partkey` bigint, 
  `l_suppkey` bigint, 
  `l_linenumber` int, 
  `l_quantity` double, 
  `l_extendedprice` double, 
  `l_discount` double, 
  `l_tax` double, 
  `l_returnflag` string, 
  `l_linestatus` string, 
  `l_shipdate` string, 
  `l_commitdate` string, 
  `l_receiptdate` string, 
  `l_shipinstruct` string, 
  `l_shipmode` string, 
  `l_comment` string)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.orc.OrcSerde' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'
TBLPROPERTIES (
  'bucketing_version'='2', 
  'transactional'='true', 
  'transactional_properties'='default', 
  'transient_lastDdlTime'='1543027179');

CREATE TABLE `orders`(
  `o_orderkey` bigint, 
  `o_custkey` bigint, 
  `o_orderstatus` string, 
  `o_totalprice` double, 
  `o_orderdate` string, 
  `o_orderpriority` string, 
  `o_clerk` string, 
  `o_shippriority` int, 
  `o_comment` string)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.orc.OrcSerde' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'
TBLPROPERTIES (
  'bucketing_version'='2', 
  'transactional'='true', 
  'transactional_properties'='default', 
  'transient_lastDdlTime'='1543026824');

alter table customer update statistics set('numRows'='150000000','rawDataSize'='8633707142');
alter table lineitem update statistics set('numRows'='5999989709','rawDataSize'='184245066955');
alter table orders update statistics set('numRows'='1500000000','rawDataSize'='46741318253');


create view q18_tmp_cached as
select l_orderkey, sum(l_quantity) as t_sum_quantity
from lineitem
where l_orderkey is not null
group by l_orderkey;

-- Set bloom filter size to huge number so we get any possible semijoin reductions

set hive.tez.min.bloom.filter.entries=0;
set hive.tez.max.bloom.filter.entries=1;

explain
create table q18_large_volume_customer_cached stored as orc tblproperties ('transactional'='true', 'transactional_properties'='default') as
select c_name, c_custkey, o_orderkey, o_orderdate, o_totalprice, sum(l_quantity)
from customer, orders, q18_tmp_cached t, lineitem l
where
  c_custkey = o_custkey and o_orderkey = t.l_orderkey
  and o_orderkey is not null and t.t_sum_quantity > 300
  and o_orderkey = l.l_orderkey and l.l_orderkey is not null
group by c_name, c_custkey, o_orderkey, o_orderdate, o_totalprice
order by o_totalprice desc, o_orderdate
limit 100;

create table q18_large_volume_customer_cached stored as orc tblproperties ('transactional'='true', 'transactional_properties'='default') as
select c_name, c_custkey, o_orderkey, o_orderdate, o_totalprice, sum(l_quantity)
from customer, orders, q18_tmp_cached t, lineitem l
where
  c_custkey = o_custkey and o_orderkey = t.l_orderkey
  and o_orderkey is not null and t.t_sum_quantity > 300
  and o_orderkey = l.l_orderkey and l.l_orderkey is not null
group by c_name, c_custkey, o_orderkey, o_orderdate, o_totalprice
order by o_totalprice desc, o_orderdate
limit 100;

drop database tpch_test cascade;
