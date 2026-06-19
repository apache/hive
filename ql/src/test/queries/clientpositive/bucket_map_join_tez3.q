-- Test for HIVE-27267

set hive.support.concurrency=true;
set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;

create table target_table(date_col date, string_col string, decimal_col decimal(38,0)) clustered by (decimal_col) into 7 buckets stored as orc tblproperties ('bucketing_version'='2', 'transactional'='true', 'transactional_properties'='default');
insert into table target_table values
('2017-05-17', 'pipeline', '50000000000000000000441610525'),
('2018-12-20', 'pipeline', '50000000000000000001048981030'),
('2020-06-30', 'pipeline', '50000000000000000002332575516'),
('2021-08-16', 'pipeline', '50000000000000000003897973989'),
('2017-06-06', 'pipeline', '50000000000000000000449148729'),
('2017-09-08', 'pipeline', '50000000000000000000525378314'),
('2022-08-30', 'pipeline', '50000000000000000005905545593'),
('2022-08-16', 'pipeline', '50000000000000000005905545593'),
('2018-05-03', 'pipeline', '50000000000000000000750826355'),
('2020-01-10', 'pipeline', '50000000000000000001816579677'),
('2021-11-01', 'pipeline', '50000000000000000004269423714'),
('2017-11-07', 'pipeline', '50000000000000000000585901787'),
('2019-10-15', 'pipeline', '50000000000000000001598843430'),
('2020-04-01', 'pipeline', '50000000000000000002035795461'),
('2020-02-24', 'pipeline', '50000000000000000001932600185'),
('2020-04-27', 'pipeline', '50000000000000000002108160849'),
('2016-07-05', 'pipeline', '50000000000000000000054405114'),
('2020-06-02', 'pipeline', '50000000000000000002234387967'),
('2020-08-21', 'pipeline', '50000000000000000002529168758'),
('2021-02-17', 'pipeline', '50000000000000000003158511687');

create table source_table(date_col date, string_col string, decimal_col decimal(38,0)) clustered by (decimal_col) into 7 buckets stored as orc tblproperties ('bucketing_version'='2', 'transactional'='true', 'transactional_properties'='default');
insert into table source_table values
('2022-08-30', 'pipeline', '50000000000000000005905545593'),
('2022-08-16', 'pipeline', '50000000000000000005905545593'),
('2022-09-01', 'pipeline', '50000000000000000006008686831'),
('2022-08-30', 'pipeline', '50000000000000000005992620837'),
('2022-09-01', 'pipeline', '50000000000000000005992620837'),
('2022-09-01', 'pipeline', '50000000000000000005992621067'),
('2022-08-30', 'pipeline', '50000000000000000005992621067');


-- Test 2 queries in 4 configs.

-- Each query has 1 join that can be converted to bucket join.
-- One of the query receives the small table from Map vertex while the other recives it from Reducer vertex.

-- 4 configs enfoce MapJoin to be converted to one of the following joins:
-- 1. BucketMapJoin, 2. MapJoin, 3. VectorBucketMapJoin, 4. VectorMapJoin

set hive.auto.convert.join=true;
set hive.optimize.dynamic.partition.hashjoin=false;

-- 1. BucketMapJoin
set hive.convert.join.bucket.mapjoin.tez=true;
set hive.vectorized.execution.enabled=false;

explain extended
select * from target_table inner join
(select date_col, 'pipeline' string_col, decimal_col from source_table where coalesce(decimal_col,'') = '50000000000000000005905545593') s
on s.date_col = target_table.date_col AND s.string_col = target_table.string_col AND s.decimal_col = target_table.decimal_col;
select * from target_table inner join
(select date_col, 'pipeline' string_col, decimal_col from source_table where coalesce(decimal_col,'') = '50000000000000000005905545593') s
on s.date_col = target_table.date_col AND s.string_col = target_table.string_col AND s.decimal_col = target_table.decimal_col;

explain extended
select * from target_table inner join
(select distinct date_col, 'pipeline' string_col, decimal_col from source_table where coalesce(decimal_col,'') = '50000000000000000005905545593') s
on s.date_col = target_table.date_col AND s.string_col = target_table.string_col AND s.decimal_col = target_table.decimal_col;
select * from target_table inner join
(select distinct date_col, 'pipeline' string_col, decimal_col from source_table where coalesce(decimal_col,'') = '50000000000000000005905545593') s
on s.date_col = target_table.date_col AND s.string_col = target_table.string_col AND s.decimal_col = target_table.decimal_col;

-- 2. MapJoin
set hive.convert.join.bucket.mapjoin.tez=false;
set hive.vectorized.execution.enabled=false;

explain extended
select * from target_table inner join
(select date_col, 'pipeline' string_col, decimal_col from source_table where coalesce(decimal_col,'') = '50000000000000000005905545593') s
on s.date_col = target_table.date_col AND s.string_col = target_table.string_col AND s.decimal_col = target_table.decimal_col;
select * from target_table inner join
(select date_col, 'pipeline' string_col, decimal_col from source_table where coalesce(decimal_col,'') = '50000000000000000005905545593') s
on s.date_col = target_table.date_col AND s.string_col = target_table.string_col AND s.decimal_col = target_table.decimal_col;

explain extended
select * from target_table inner join
(select distinct date_col, 'pipeline' string_col, decimal_col from source_table where coalesce(decimal_col,'') = '50000000000000000005905545593') s
on s.date_col = target_table.date_col AND s.string_col = target_table.string_col AND s.decimal_col = target_table.decimal_col;
select * from target_table inner join
(select distinct date_col, 'pipeline' string_col, decimal_col from source_table where coalesce(decimal_col,'') = '50000000000000000005905545593') s
on s.date_col = target_table.date_col AND s.string_col = target_table.string_col AND s.decimal_col = target_table.decimal_col;

-- 3. VectorBucketMapJoin
set hive.convert.join.bucket.mapjoin.tez=true;
set hive.vectorized.execution.enabled=true;

explain extended
select * from target_table inner join
(select date_col, 'pipeline' string_col, decimal_col from source_table where coalesce(decimal_col,'') = '50000000000000000005905545593') s
on s.date_col = target_table.date_col AND s.string_col = target_table.string_col AND s.decimal_col = target_table.decimal_col;
select * from target_table inner join
(select date_col, 'pipeline' string_col, decimal_col from source_table where coalesce(decimal_col,'') = '50000000000000000005905545593') s
on s.date_col = target_table.date_col AND s.string_col = target_table.string_col AND s.decimal_col = target_table.decimal_col;

explain extended
select * from target_table inner join
(select distinct date_col, 'pipeline' string_col, decimal_col from source_table where coalesce(decimal_col,'') = '50000000000000000005905545593') s
on s.date_col = target_table.date_col AND s.string_col = target_table.string_col AND s.decimal_col = target_table.decimal_col;
select * from target_table inner join
(select distinct date_col, 'pipeline' string_col, decimal_col from source_table where coalesce(decimal_col,'') = '50000000000000000005905545593') s
on s.date_col = target_table.date_col AND s.string_col = target_table.string_col AND s.decimal_col = target_table.decimal_col;

-- 4. VectorMapJoin
set hive.convert.join.bucket.mapjoin.tez=false;
set hive.vectorized.execution.enabled=true;

explain extended
select * from target_table inner join
(select date_col, 'pipeline' string_col, decimal_col from source_table where coalesce(decimal_col,'') = '50000000000000000005905545593') s
on s.date_col = target_table.date_col AND s.string_col = target_table.string_col AND s.decimal_col = target_table.decimal_col;
select * from target_table inner join
(select date_col, 'pipeline' string_col, decimal_col from source_table where coalesce(decimal_col,'') = '50000000000000000005905545593') s
on s.date_col = target_table.date_col AND s.string_col = target_table.string_col AND s.decimal_col = target_table.decimal_col;

explain extended
select * from target_table inner join
(select distinct date_col, 'pipeline' string_col, decimal_col from source_table where coalesce(decimal_col,'') = '50000000000000000005905545593') s
on s.date_col = target_table.date_col AND s.string_col = target_table.string_col AND s.decimal_col = target_table.decimal_col;
select * from target_table inner join
(select distinct date_col, 'pipeline' string_col, decimal_col from source_table where coalesce(decimal_col,'') = '50000000000000000005905545593') s
on s.date_col = target_table.date_col AND s.string_col = target_table.string_col AND s.decimal_col = target_table.decimal_col;
