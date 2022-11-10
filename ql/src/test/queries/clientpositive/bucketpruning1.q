set hive.mapred.mode=nonstrict;
set hive.optimize.ppd=true;
set hive.optimize.index.filter=true;
set hive.tez.bucket.pruning=true;
set hive.explain.user=false;
set hive.fetch.task.conversion=none;

CREATE TABLE srcbucket_pruned(key int, value string) partitioned by (ds string) CLUSTERED BY (key) INTO 16 BUCKETS STORED AS TEXTFILE;

-- cannot prune 2-key scenarios without a smarter optimizer
CREATE TABLE srcbucket_unpruned(key int, value string) partitioned by (ds string) CLUSTERED BY (key,value) INTO 16 BUCKETS STORED AS TEXTFILE;

-- valid AND cases: when an AND condition is a bucket column, then pruning should work

explain extended
select * from srcbucket_pruned where key = 1 and value is not null;

-- good cases

explain extended
select * from srcbucket_pruned where key = 1;

explain extended
select * from srcbucket_pruned where key = 16;

explain extended
select * from srcbucket_pruned where key = 17;

explain extended
select * from srcbucket_pruned where key = 16+1;

explain extended
select * from srcbucket_pruned where key = '11';

explain extended
select * from srcbucket_pruned where key = 1 and ds='2008-04-08';

explain extended
select * from srcbucket_pruned where key = 1 and ds='2008-04-08' and value='One';

explain extended
select * from srcbucket_pruned where value='One' and key = 1 and ds='2008-04-08';

explain extended
select * from srcbucket_pruned where key in (2,3);

explain extended
select * from srcbucket_pruned where key in (2,3) and ds='2008-04-08';

explain extended
select * from srcbucket_pruned where key in (2,3) and ds='2008-04-08' and value='One';

explain extended
select * from srcbucket_pruned where value='One' and key in (2,3) and ds='2008-04-08';

explain extended
select * from srcbucket_pruned where (key=1 or key=2) and ds='2008-04-08';

explain extended
select * from srcbucket_pruned where (key=1 or key=2) and value = 'One' and ds='2008-04-08';

-- compat case (-15 = 1 & 15)

explain extended
select * from srcbucket_pruned where key = -15;

-- valid but irrelevant case (all buckets selected)

explain extended
select * from srcbucket_pruned where key in (1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17);

explain extended
select * from srcbucket_pruned where key in (1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17) and ds='2008-04-08';

explain extended
select * from srcbucket_pruned where key in (1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17) and ds='2008-04-08' and value='One';

explain extended
select * from srcbucket_pruned where value='One' and key in (1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17) and ds='2008-04-08';

-- valid, but unimplemented cases

explain extended
select * from srcbucket_pruned where key = 1 and ds='2008-04-08' or key = 2;

explain extended
select * from srcbucket_pruned where key = 1 and ds='2008-04-08' and (value='One' or value = 'Two');

explain extended
select * from srcbucket_pruned where key = 1 or value = "One" or key = 2 and value = "Two";

-- Invalid cases

explain extended
select * from srcbucket_pruned where key = 'x11';

explain extended
select * from srcbucket_pruned where key = 1 or value = "One";

explain extended
select * from srcbucket_pruned where key = 1 or value = "One" or key = 2;

explain extended
select * from srcbucket_unpruned where key in (3, 5);

explain extended
select * from srcbucket_unpruned where key = 1;
