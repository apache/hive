-- HIVE-29516: Verify that query compilation succeeds when column statistics
-- are missing during semijoin optimization in removeSemijoinOptimizationByBenefit.

set hive.tez.dynamic.partition.pruning=true;
set hive.tez.dynamic.semijoin.reduction=true;
set hive.tez.bigtable.minsize.semijoin.reduction=1;
set hive.tez.min.bloom.filter.entries=1;
set hive.tez.bloom.filter.factor=1.0f;
set hive.auto.convert.join=false;

create table t1_nocolstats (id int, val string);
create table t2_nocolstats (id int, val string);

alter table t1_nocolstats update statistics set('numRows'='100000000', 'rawDataSize'='2000000000');
alter table t2_nocolstats update statistics set('numRows'='1000', 'rawDataSize'='20000');

explain
select t1.id, t1.val, t2.val
from t1_nocolstats t1 join t2_nocolstats t2 on t1.id = t2.id;
