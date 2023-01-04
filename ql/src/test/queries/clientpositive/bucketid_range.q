set hive.compute.query.using.stats=false;
set hive.enforce.bucketing=true;
set hive.support.concurrency=true;
set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;

dfs ${system:test.dfs.mkdir} ${system:test.tmp.dir}/integer_sequence;
dfs -copyFromLocal ../../data/files/integer_sequence.txt  ${system:test.tmp.dir}/integer_sequence/;

CREATE EXTERNAL TABLE integer_sequence_ext(int_field int)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
STORED AS TEXTFILE
LOCATION '${system:test.tmp.dir}/integer_sequence';

CREATE TABLE integer_sequence (int_field int)
CLUSTERED BY (int_field) INTO 4100 BUCKETS
STORED AS ORC TBLPROPERTIES ('transactional' = 'true');

-- this statement should not fail after HIVE-24715 (bucketId > 4095)
-- don't need to insert all the records, we know that the bucket hash will be >4095 for integers >4095
INSERT INTO integer_sequence SELECT int_field FROM integer_sequence_ext where int_field > 4000;

select count(*) from integer_sequence;
