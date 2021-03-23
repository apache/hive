--! qt:disabled:HIVE-24894
--! qt:dataset:src
--! qt:dataset:alltypesorc
set hive.entity.capture.transform=true;
set hive.support.concurrency=true;
set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;


create table transform_acid(a int, b varchar(128)) clustered by (a) into 2 buckets stored as orc TBLPROPERTIES ('transactional'='true');
insert into table transform_acid select cint, cast(cstring1 as varchar(128)) from alltypesorc where cint < 0 order by cint limit 1;


ADD FILE ../../ql/src/test/scripts/transform_acid_grep.sh;

SELECT transform(*) USING 'transform_acid_grep.sh' AS (col string) FROM transform_acid;
