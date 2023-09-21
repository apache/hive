set hive.tez.union.flatten.subdirectories=true;
set hive.support.concurrency=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;
set hive.acid.direct.insert.enabled=true;
set hive.auto.convert.join=true;

create table test1 (val string) partitioned by (dt string) stored as avro TBLPROPERTIES ('transactional'='true', 'transactional_properties'='insert_only');
insert into test1 partition (dt='20230817') values ("val1"), ("val2");
create table test2 (val string) partitioned by (dt string) stored as avro TBLPROPERTIES ('transactional'='true', 'transactional_properties'='insert_only');
select ful.* from (select val from test2 where dt='20230816') ful left join (select val from test1 where dt='20230817') inc on ful.val=inc.val;

explain insert overwrite table test2 partition (dt='20230817') select ful.* from (select val from test2 where dt='20230816') ful left join (select val from test1 where dt='20230817') inc on ful.val=inc.val union all select test1.val from test1 where dt='20230817';

insert overwrite table test2 partition (dt='20230817') select ful.* from (select val from test2 where dt='20230816') ful left join (select val from test1 where dt='20230817') inc on ful.val=inc.val union all select test1.val from test1 where dt='20230817';

dfs -ls -R ${hiveconf:hive.metastore.warehouse.dir}/test2;