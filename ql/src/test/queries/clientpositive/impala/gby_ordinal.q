--! qt:dataset:impala_dataset

explain
select l_orderkey, count(*)
from impala_tpch_lineitem
group by 1
order by 1;
