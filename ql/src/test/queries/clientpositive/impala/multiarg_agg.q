--! qt:dataset:impala_dataset

explain
select l_orderkey, group_concat(cast(l_suppkey as string), ':')
  from impala_tpch_lineitem
  group by l_orderkey
  limit 10;
