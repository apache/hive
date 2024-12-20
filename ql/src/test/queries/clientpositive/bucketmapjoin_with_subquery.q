-- HIVE-27069
set hive.query.results.cache.enabled=false;
set hive.compute.query.using.stats=false;
set hive.support.concurrency=true;
set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;
set hive.auto.convert.join=true;
set hive.auto.convert.sortmerge.join=true;
set hive.auto.convert.sortmerge.join.to.mapjoin=true;
set hive.convert.join.bucket.mapjoin.tez=true;
set hive.fetch.task.conversion=none;
set hive.merge.nway.joins=false;
set hive.optimize.dynamic.partition.hashjoin=true;
set hive.optimize.index.filter=true;
set hive.optimize.remove.sq_count_check=true;
set hive.prewarm.enabled=false;
set hive.join.inner.residual=false;
set hive.limit.optimize.enable=true;
set hive.mapjoin.bucket.cache.size=10000;
set hive.strict.managed.tables=true;
set hive.tez.auto.reducer.parallelism=true;
set hive.tez.bucket.pruning=true;

CREATE TABLE DUP_TEST (id int , in_date timestamp , sample varchar(100)) stored as orc tblproperties('transactional'='true', 'transactional_properties'='default');

CREATE TABLE DUP_TEST_TARGET (id int , in_date timestamp , sample varchar(100)) CLUSTERED by (ID) INTO 5 BUCKETS STORED AS ORC tblproperties('transactional'='true', 'transactional_properties'='default');

INSERT INTO DUP_TEST
(id , in_date , sample)
values
(1  , '2023-04-14 10:11:12.111' , 'test1'),
(2  , '2023-04-14 10:11:12.111' , 'test2'),
(3  , '2023-04-14 10:11:12.111' , 'test3'),
(4  , '2023-04-14 10:11:12.111' , 'test4'),
(5  , '2023-04-14 10:11:12.111' , 'test5'),
(6  , '2023-04-14 10:11:12.111' , 'test6'),
(7  , '2023-04-14 10:11:12.111' , 'test7'),
(8  , '2023-04-14 10:11:12.111' , 'test8'),
(9  , '2023-04-14 10:11:12.111' , 'test9');

-- Run merge into the target table for the first time
MERGE INTO DUP_TEST_TARGET T USING (SELECT id , in_date , sample FROM (SELECT id , in_date , sample ,ROW_NUMBER()
OVER(PARTITION BY id ORDER BY in_date DESC ) AS ROW_NUMB  FROM DUP_TEST) OUTQUERY WHERE ROW_NUMB =1) as S ON T.id = S.id
    WHEN MATCHED THEN UPDATE SET  in_date = S.in_date , sample = S.sample
    WHEN NOT MATCHED THEN INSERT VALUES (S.id, S.in_date , S.sample);

explain vectorization detail select * from DUP_TEST_TARGET T join (SELECT id , in_date , sample FROM (SELECT id , in_date , sample ,ROW_NUMBER()
OVER(PARTITION BY id ORDER BY in_date DESC ) AS ROW_NUMB  FROM DUP_TEST) OUTQUERY WHERE ROW_NUMB =1) as S ON T.id = S.id;

select * from DUP_TEST_TARGET T join (SELECT id , in_date , sample FROM (SELECT id , in_date , sample ,ROW_NUMBER()
OVER(PARTITION BY id ORDER BY in_date DESC ) AS ROW_NUMB  FROM DUP_TEST) OUTQUERY WHERE ROW_NUMB =1) as S ON T.id = S.id;

select * from DUP_TEST_TARGET T join DUP_TEST S ON T.id = S.id;