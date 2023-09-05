set hive.support.concurrency=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;
set hive.acid.direct.insert.enabled=true;
set hive.auto.convert.join=true;

create table lbodor_test1 (val string) partitioned by (dt string) stored as avro TBLPROPERTIES ('transactional'='true', 'transactional_properties'='insert_only');
insert into lbodor_test1 partition (dt='20230817') values ("val1"), ("val2");
create table lbodor_test2 (val string) partitioned by (dt string) stored as avro TBLPROPERTIES ('transactional'='true', 'transactional_properties'='insert_only');
select ful.* from (select val from lbodor_test2 where dt='20230816') ful left join (select val from lbodor_test1 where dt='20230817') inc on ful.val=inc.val;

explain insert overwrite table lbodor_test2 partition (dt='20230817') select ful.* from (select val from lbodor_test2 where dt='20230816') ful left join (select val from lbodor_test1 where dt='20230817') inc on ful.val=inc.val union all select lbodor_test1.val from lbodor_test1 where dt='20230817';

insert overwrite table lbodor_test2 partition (dt='20230817') select ful.* from (select val from lbodor_test2 where dt='20230816') ful left join (select val from lbodor_test1 where dt='20230817') inc on ful.val=inc.val union all select lbodor_test1.val from lbodor_test1 where dt='20230817';
