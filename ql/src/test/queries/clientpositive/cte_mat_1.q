--! qt:dataset:src
set hive.mapred.mode=nonstrict;
set hive.optimize.cte.materialize.threshold=-1;

explain
with q1(srcKey, srcValue) as (select * from src where key= '5')
select a.srcKey, b.srcValue
from q1 a join q1 b
on a.srcKey=b.srcKey;

set hive.optimize.cte.materialize.threshold=2;
set hive.optimize.cte.materialize.full.aggregate.only=false;
-- Use a format that retains column names
set hive.default.fileformat=parquet;

explain
with q1(srcKey, srcValue) as (select * from src where key= '5')
select a.srcKey, b.srcValue
from q1 a join q1 b
on a.srcKey=b.srcKey;

with q1(srcKey, srcValue) as (select * from src where key= '5')
select a.srcKey, b.srcValue
from q1 a join q1 b
on a.srcKey=b.srcKey;

-- Hive allows <with column list> to have a smaller number of columns than the query expression
explain
with q1(`srcKey`) as (select * from src where key= '5')
select a.srcKey
from q1 a join q1 b
on a.srcKey=b.srcKey;

with q1(`srcKey`) as (select * from src where key= '5')
select a.srcKey
from q1 a join q1 b
on a.srcKey=b.srcKey;
