--! qt:dataset:src
set hive.mapred.mode=nonstrict;
set hive.optimize.cte.materialize.threshold=-1;
set hive.explain.user=true;

explain
with q1(srcKey, srcValue) as (select * from src where key= '5')
select a.srcKey
from q1 a join q1 b
on a.srcKey=b.srcKey;
