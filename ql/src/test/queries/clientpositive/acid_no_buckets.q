--! qt:dataset:srcpart
--! qt:replace:/(totalSize\s+)(\S+|\s+|.+)/$1#Masked#/
--this has 4 groups of tests
--Acid tables w/o bucketing
--the tests with bucketing (make sure we get the same results)
--same tests with and w/o vectorization

set hive.mapred.mode=nonstrict;
set hive.support.concurrency=true;
set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;
set hive.vectorized.execution.enabled=false;
set hive.explain.user=false;
set hive.merge.cardinality.check=true;

drop table if exists srcpart_acid;
CREATE TABLE srcpart_acid (key STRING, value STRING) PARTITIONED BY (ds STRING, hr STRING) stored as ORC TBLPROPERTIES ('transactional'='true', 'transactional_properties'='default');
insert into srcpart_acid PARTITION (ds, hr) select * from srcpart;

--2 rows for 413, 1 row for 43, 2 for 213, 1 for 44 in kv1.txt (in each partition)
select ds, hr, key, value from srcpart_acid where cast(key as integer) in(413,43) and hr='11' order by ds, hr, cast(key as integer);

analyze table srcpart_acid PARTITION(ds, hr) compute statistics;
analyze table srcpart_acid PARTITION(ds, hr) compute statistics for columns;
explain update srcpart_acid set value = concat(value, 'updated') where cast(key as integer) in(413,43) and hr='11';
update srcpart_acid set value = concat(value, 'updated') where cast(key as integer) in(413,43) and hr='11';
select ds, hr, key, value from srcpart_acid where value like '%updated' order by ds, hr, cast(key as integer);

insert into srcpart_acid PARTITION (ds='2008-04-08', hr=='11') values ('1001','val1001'),('1002','val1002'),('1003','val1003');
select ds, hr, key, value from srcpart_acid where cast(key as integer) > 1000 order by ds, hr, cast(key as integer);

describe formatted srcpart_acid;
describe formatted srcpart_acid key;

analyze table srcpart_acid PARTITION(ds, hr) compute statistics;
analyze table srcpart_acid PARTITION(ds, hr) compute statistics for columns;

-- make sure the stats stay the same after analyze (insert and update above also update stats)
describe formatted srcpart_acid;
describe formatted srcpart_acid key;

explain delete from srcpart_acid where key in( '1001', '213', '43');
--delete some rows from initial load, some that were updated and some that were inserted
delete from srcpart_acid where key in( '1001', '213', '43');

--make sure we deleted everything that should've been deleted
select count(*) from srcpart_acid where key in( '1001', '213', '43');
--make sure nothing extra was deleted  (2000 + 3 (insert) - 4 - 1 - 8 = 1990)
select count(*) from srcpart_acid;

--todo: should really have a way to run compactor here....

--update should match 1 rows in 1 partition
--delete should drop everything from 1 partition
--insert should do nothing
merge into srcpart_acid t using (select distinct ds, hr, key, value from srcpart_acid) s
on s.ds=t.ds and s.hr=t.hr and s.key=t.key and s.value=t.value
when matched and s.ds='2008-04-08' and s.hr=='11' and s.key='44' then update set value=concat(s.value,'updated by merge')
when matched and s.ds='2008-04-08' and s.hr=='12' then delete
when not matched then insert values('this','should','not','be there');

--check results
--should be 0
select count(*) from srcpart_acid where ds='2008-04-08' and hr=='12';
--should be 1 rows
select ds, hr, key, value from srcpart_acid where value like '%updated by merge';
--should be 0
select count(*) from srcpart_acid where ds = 'this' and hr = 'should' and key = 'not' and value = 'be there';
drop table if exists srcpart_acid;


drop table if exists srcpart_acidb;
CREATE TABLE srcpart_acidb (key STRING, value STRING) PARTITIONED BY (ds STRING, hr STRING) CLUSTERED BY(key) INTO 2 BUCKETS stored as ORC TBLPROPERTIES ('transactional'='true', 'transactional_properties'='default');
insert into srcpart_acidb PARTITION (ds, hr) select * from srcpart;

--2 rows for 413, 1 row for 43, 2 for 213, 2 for 12 in kv1.txt (in each partition)
select ds, hr, key, value from srcpart_acidb where cast(key as integer) in(413,43) and hr='11' order by ds, hr, cast(key as integer);

analyze table srcpart_acidb PARTITION(ds, hr) compute statistics;
analyze table srcpart_acidb PARTITION(ds, hr) compute statistics for columns;
explain update srcpart_acidb set value = concat(value, 'updated') where cast(key as integer) in(413,43) and hr='11';
update srcpart_acidb set value = concat(value, 'updated') where cast(key as integer) in(413,43) and hr='11';
select ds, hr, key, value from srcpart_acidb where value like '%updated' order by ds, hr, cast(key as integer);

insert into srcpart_acidb PARTITION (ds='2008-04-08', hr=='11') values ('1001','val1001'),('1002','val1002'),('1003','val1003');
select ds, hr, key, value from srcpart_acidb where cast(key as integer) > 1000 order by ds, hr, cast(key as integer);

analyze table srcpart_acidb PARTITION(ds, hr) compute statistics;
analyze table srcpart_acidb PARTITION(ds, hr) compute statistics for columns;
explain delete from srcpart_acidb where key in( '1001', '213', '43');
--delete some rows from initial load, some that were updated and some that were inserted
delete from srcpart_acidb where key in( '1001', '213', '43');

--make sure we deleted everything that should've been deleted
select count(*) from srcpart_acidb where key in( '1001', '213', '43');
--make sure nothing extra was deleted  (2000 + 3 (insert) - 4 - 1 - 8 = 1990)
select count(*) from srcpart_acidb;


--todo: should really have a way to run compactor here....

--update should match 1 rows in 1 partition
--delete should drop everything from 1 partition
--insert should do nothing
merge into srcpart_acidb t using (select distinct ds, hr, key, value from srcpart_acidb) s
on s.ds=t.ds and s.hr=t.hr and s.key=t.key and s.value=t.value
when matched and s.ds='2008-04-08' and s.hr=='11' and s.key='44' then update set value=concat(s.value,'updated by merge')
when matched and s.ds='2008-04-08' and s.hr=='12' then delete
when not matched then insert values('this','should','not','be there');

--check results
--should be 0
select count(*) from srcpart_acidb where ds='2008-04-08' and hr=='12';
--should be 1 rows
select ds, hr, key, value from srcpart_acidb where value like '%updated by merge';
--should be 0
select count(*) from srcpart_acidb where ds = 'this' and hr = 'should' and key = 'not' and value = 'be there';
drop table if exists srcpart_acidb;



--now same thing but vectorized
set hive.vectorized.execution.enabled=true;

drop table if exists srcpart_acidv;
CREATE TABLE srcpart_acidv (key STRING, value STRING) PARTITIONED BY (ds STRING, hr STRING) stored as ORC TBLPROPERTIES ('transactional'='true', 'transactional_properties'='default');
insert into srcpart_acidv PARTITION (ds, hr) select * from srcpart;

--2 rows for 413, 21 row for 43, 2 for 213, 2 for 12 in kv1.txt (in each partition)
select ds, hr, key, value from srcpart_acidv where cast(key as integer) in(413,43) and hr='11' order by ds, hr, cast(key as integer);

analyze table srcpart_acidv PARTITION(ds, hr) compute statistics;
analyze table srcpart_acidv PARTITION(ds, hr) compute statistics for columns;
explain vectorization only detail
update srcpart_acidv set value = concat(value, 'updated') where cast(key as integer) in(413,43) and hr='11';
update srcpart_acidv set value = concat(value, 'updated') where cast(key as integer) in(413,43) and hr='11';
select ds, hr, key, value from srcpart_acidv where value like '%updated' order by ds, hr, cast(key as integer);

insert into srcpart_acidv PARTITION (ds='2008-04-08', hr=='11') values ('1001','val1001'),('1002','val1002'),('1003','val1003');
select ds, hr, key, value from srcpart_acidv where cast(key as integer) > 1000 order by ds, hr, cast(key as integer);

analyze table srcpart_acidv PARTITION(ds, hr) compute statistics;
analyze table srcpart_acidv PARTITION(ds, hr) compute statistics for columns;
explain vectorization only detail
delete from srcpart_acidv where key in( '1001', '213', '43');
--delete some rows from initial load, some that were updated and some that were inserted
delete from srcpart_acidv where key in( '1001', '213', '43');

--make sure we deleted everything that should've been deleted
select count(*) from srcpart_acidv where key in( '1001', '213', '43');
--make sure nothing extra was deleted  (2000 + 3 - 4 - 1 - 8 = 1990)
select count(*) from srcpart_acidv;

--todo: should really have a way to run compactor here....

--update should match 1 rows in 1 partition
--delete should drop everything from 1 partition
--insert should do nothing
explain vectorization only detail
merge into srcpart_acidv t using (select distinct ds, hr, key, value from srcpart_acidv) s
on s.ds=t.ds and s.hr=t.hr and s.key=t.key and s.value=t.value
when matched and s.ds='2008-04-08' and s.hr=='11' and s.key='44' then update set value=concat(s.value,'updated by merge')
when matched and s.ds='2008-04-08' and s.hr=='12' then delete
when not matched then insert values('this','should','not','be there');
merge into srcpart_acidv t using (select distinct ds, hr, key, value from srcpart_acidv) s
on s.ds=t.ds and s.hr=t.hr and s.key=t.key and s.value=t.value
when matched and s.ds='2008-04-08' and s.hr=='11' and s.key='44' then update set value=concat(s.value,'updated by merge')
when matched and s.ds='2008-04-08' and s.hr=='12' then delete
when not matched then insert values('this','should','not','be there');

--check results
--should be 0
select count(*) from srcpart_acidv where ds='2008-04-08' and hr=='12';
--should be 1 rows
select ds, hr, key, value from srcpart_acidv where value like '%updated by merge';
--should be 0
select count(*) from srcpart_acidv where ds = 'this' and hr = 'should' and key = 'not' and value = 'be there';
drop table if exists srcpart_acidv;



drop table if exists srcpart_acidvb;
CREATE TABLE srcpart_acidvb (key STRING, value STRING) PARTITIONED BY (ds STRING, hr STRING) CLUSTERED BY(key) INTO 2 BUCKETS stored as ORC TBLPROPERTIES ('transactional'='true', 'transactional_properties'='default');
insert into srcpart_acidvb PARTITION (ds, hr) select * from srcpart;

--2 rows for 413, 1 row for 43, 2 for 213, 2 for 12 in kv1.txt (in each partition)
select ds, hr, key, value from srcpart_acidvb where cast(key as integer) in(413,43) and hr='11' order by ds, hr, cast(key as integer);

analyze table srcpart_acidvb PARTITION(ds, hr) compute statistics;
analyze table srcpart_acidvb PARTITION(ds, hr) compute statistics for columns;
explain vectorization only detail
update srcpart_acidvb set value = concat(value, 'updated') where cast(key as integer) in(413,43) and hr='11';
update srcpart_acidvb set value = concat(value, 'updated') where cast(key as integer) in(413,43) and hr='11';
select ds, hr, key, value from srcpart_acidvb where value like '%updated' order by ds, hr, cast(key as integer);

insert into srcpart_acidvb PARTITION (ds='2008-04-08', hr=='11') values ('1001','val1001'),('1002','val1002'),('1003','val1003');
select ds, hr, key, value from srcpart_acidvb where cast(key as integer) > 1000 order by ds, hr, cast(key as integer);

analyze table srcpart_acidvb PARTITION(ds, hr) compute statistics;
analyze table srcpart_acidvb PARTITION(ds, hr) compute statistics for columns;
explain vectorization only detail
delete from srcpart_acidvb where key in( '1001', '213', '43');
--delete some rows from initial load, some that were updated and some that were inserted
delete from srcpart_acidvb where key in( '1001', '213', '43');

--make sure we deleted everything that should've been deleted
select count(*) from srcpart_acidvb where key in( '1001', '213', '43');
--make sure nothing extra was deleted  (2000 + 3 (insert) - 4 - 1 - 8 = 1990)
select count(*) from srcpart_acidvb;


--todo: should really have a way to run compactor here....

--update should match 1 rows in 1 partition
--delete should drop everything from 1 partition
--insert should do nothing
explain vectorization only detail
merge into srcpart_acidvb t using (select distinct ds, hr, key, value from srcpart_acidvb) s
on s.ds=t.ds and s.hr=t.hr and s.key=t.key and s.value=t.value
when matched and s.ds='2008-04-08' and s.hr=='11' and s.key='44' then update set value=concat(s.value,'updated by merge')
when matched and s.ds='2008-04-08' and s.hr=='12' then delete
when not matched then insert values('this','should','not','be there');
merge into srcpart_acidvb t using (select distinct ds, hr, key, value from srcpart_acidvb) s
on s.ds=t.ds and s.hr=t.hr and s.key=t.key and s.value=t.value
when matched and s.ds='2008-04-08' and s.hr=='11' and s.key='44' then update set value=concat(s.value,'updated by merge')
when matched and s.ds='2008-04-08' and s.hr=='12' then delete
when not matched then insert values('this','should','not','be there');

--check results
--should be 0
select count(*) from srcpart_acidvb where ds='2008-04-08' and hr=='12';
--should be 1 rows
select ds, hr, key, value from srcpart_acidvb where value like '%updated by merge';
--should be 0
select count(*) from srcpart_acidvb where ds = 'this' and hr = 'should' and key = 'not' and value = 'be there';
drop table if exists srcpart_acidvb;
