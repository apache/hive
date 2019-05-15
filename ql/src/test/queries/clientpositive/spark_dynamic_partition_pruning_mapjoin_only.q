--! qt:dataset:srcpart
SET hive.vectorized.execution.enabled=false;
set hive.optimize.ppd=true;
set hive.ppd.remove.duplicatefilters=true;
set hive.optimize.metadataonly=false;
set hive.optimize.index.filter=true;
set hive.strict.checks.cartesian.product=false;

-- srcpart_date_n3 is the small table that will use map join.  srcpart2 is the big table.
-- both srcpart_date_n3 and srcpart2 will be joined with srcpart
create table srcpart_date_n3 as select ds as ds, ds as ds2 from srcpart group by ds;
create table srcpart2 as select * from srcpart;

-- enable map join and set the size to be small so that only join with srcpart_date_n3 gets to be a
-- map join
set hive.auto.convert.join=true;
set hive.auto.convert.join.noconditionaltask.size=100;

-- checking with dpp disabled
-- expectation: 2 spark jobs
EXPLAIN select *
 from srcpart
 join srcpart_date_n3 on (srcpart.ds = srcpart_date_n3.ds)
 join srcpart2 on (srcpart.hr = srcpart2.hr)
 where srcpart_date_n3.ds2 = '2008-04-08'
 and srcpart2.hr = 11;

-- checking with dpp enabled for all joins
-- both join parts of srcpart_date_n3 and srcpart2 scans will result in partition pruning sink
-- scan with srcpart2 will get split resulting in additional spark jobs
-- expectation: 3 spark jobs
set hive.spark.dynamic.partition.pruning=true;
EXPLAIN select *
 from srcpart
 join srcpart_date_n3 on (srcpart.ds = srcpart_date_n3.ds)
 join srcpart2 on (srcpart.hr = srcpart2.hr)
 where srcpart_date_n3.ds2 = '2008-04-08'
 and srcpart2.hr = 11;

-- Restrict dpp to be enabled only for map joins
-- expectation: 2 spark jobs
set hive.spark.dynamic.partition.pruning.map.join.only=true;
set hive.spark.dynamic.partition.pruning=false;
EXPLAIN select *
 from srcpart
 join srcpart_date_n3 on (srcpart.ds = srcpart_date_n3.ds)
 join srcpart2 on (srcpart.hr = srcpart2.hr)
 where srcpart_date_n3.ds2 = '2008-04-08'
 and srcpart2.hr = 11;

drop table srcpart_date_n3;
drop table srcpart2;
