set hive.tez.union.flatten.subdirectories=true;
set hive.support.concurrency=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;
set hive.acid.direct.insert.enabled=true;
set hive.auto.convert.join=true;

create table test1 (val string) partitioned by (dt string) stored as avro TBLPROPERTIES ('transactional'='true', 'transactional_properties'='insert_only');
insert into test1 partition (dt='20230817') values ("val1"), ("val2");


-- TEST FOR EXTERNAL TABLE

create table union_target_nonacid_directinsert_flattened (val string) partitioned by (dt string) stored as avro;

explain insert overwrite table union_target_nonacid_directinsert_flattened partition (dt='20230817') select ful.* from (select val from union_target_nonacid_directinsert_flattened where dt='20230816') ful left join (select val from test1 where dt='20230817') inc on ful.val=inc.val union all select test1.val from test1 where dt='20230817';

insert overwrite table union_target_nonacid_directinsert_flattened partition (dt='20230817') select ful.* from (select val from union_target_nonacid_directinsert_flattened where dt='20230816') ful left join (select val from test1 where dt='20230817') inc on ful.val=inc.val union all select test1.val from test1 where dt='20230817';

dfs -ls -R ${hiveconf:hive.metastore.warehouse.dir}/union_target_nonacid_directinsert_flattened;

select * from union_target_nonacid_directinsert_flattened;

-- TESTS FOR DIRECT & FLATTENED

create table union_target_mm_directinsert_flattened (val string) partitioned by (dt string) stored as avro TBLPROPERTIES ('transactional'='true', 'transactional_properties'='insert_only');

explain insert overwrite table union_target_mm_directinsert_flattened partition (dt='20230817') select ful.* from (select val from union_target_mm_directinsert_flattened where dt='20230816') ful left join (select val from test1 where dt='20230817') inc on ful.val=inc.val union all select test1.val from test1 where dt='20230817';

insert overwrite table union_target_mm_directinsert_flattened partition (dt='20230817') select ful.* from (select val from union_target_mm_directinsert_flattened where dt='20230816') ful left join (select val from test1 where dt='20230817') inc on ful.val=inc.val union all select test1.val from test1 where dt='20230817';

dfs -ls -R ${hiveconf:hive.metastore.warehouse.dir}/union_target_mm_directinsert_flattened;

select * from union_target_mm_directinsert_flattened;

create table union_target_acid_directinsert_flattened (val string) partitioned by (dt string) stored as ORC TBLPROPERTIES ('transactional'='true');

explain insert into table union_target_acid_directinsert_flattened partition (dt='20230817') select ful.* from (select val from union_target_acid_directinsert_flattened where dt='20230816') ful left join (select val from test1 where dt='20230817') inc on ful.val=inc.val union all select test1.val from test1 where dt='20230817';

insert into table union_target_acid_directinsert_flattened partition (dt='20230817') select ful.* from (select val from union_target_acid_directinsert_flattened where dt='20230816') ful left join (select val from test1 where dt='20230817') inc on ful.val=inc.val union all select test1.val from test1 where dt='20230817';

dfs -ls -R ${hiveconf:hive.metastore.warehouse.dir}/union_target_acid_directinsert_flattened;

select * from union_target_acid_directinsert_flattened;

-- TESTS FOR NON DIRECT & FLATTENED

set hive.acid.direct.insert.enabled=false;

create table union_target_mm_flattened (val string) partitioned by (dt string) stored as avro TBLPROPERTIES ('transactional'='true', 'transactional_properties'='insert_only');

explain insert overwrite table union_target_mm_flattened partition (dt='20230817') select ful.* from (select val from union_target_mm_flattened where dt='20230816') ful left join (select val from test1 where dt='20230817') inc on ful.val=inc.val union all select test1.val from test1 where dt='20230817';

insert overwrite table union_target_mm_flattened partition (dt='20230817') select ful.* from (select val from union_target_mm_flattened where dt='20230816') ful left join (select val from test1 where dt='20230817') inc on ful.val=inc.val union all select test1.val from test1 where dt='20230817';

dfs -ls -R ${hiveconf:hive.metastore.warehouse.dir}/union_target_mm_flattened;

select * from union_target_mm_flattened;

create table union_target_acid_flattened (val string) partitioned by (dt string) stored as ORC TBLPROPERTIES ('transactional'='true');

explain insert into table union_target_acid_flattened partition (dt='20230817') select ful.* from (select val from union_target_acid_flattened where dt='20230816') ful left join (select val from test1 where dt='20230817') inc on ful.val=inc.val union all select test1.val from test1 where dt='20230817';

insert into table union_target_acid_flattened partition (dt='20230817') select ful.* from (select val from union_target_acid_flattened where dt='20230816') ful left join (select val from test1 where dt='20230817') inc on ful.val=inc.val union all select test1.val from test1 where dt='20230817';

dfs -ls -R ${hiveconf:hive.metastore.warehouse.dir}/union_target_acid_flattened;

select * from union_target_acid_flattened;


-- TESTS FOR NON DIRECT & NON FLATTENED

set hive.tez.union.flatten.subdirectories=false;

create table union_target_mm_unflattened (val string) partitioned by (dt string) stored as avro TBLPROPERTIES ('transactional'='true', 'transactional_properties'='insert_only');

explain insert overwrite table union_target_mm_unflattened partition (dt='20230817') select ful.* from (select val from union_target_mm_unflattened where dt='20230816') ful left join (select val from test1 where dt='20230817') inc on ful.val=inc.val union all select test1.val from test1 where dt='20230817';

insert overwrite table union_target_mm_unflattened partition (dt='20230817') select ful.* from (select val from union_target_mm_unflattened where dt='20230816') ful left join (select val from test1 where dt='20230817') inc on ful.val=inc.val union all select test1.val from test1 where dt='20230817';

dfs -ls -R ${hiveconf:hive.metastore.warehouse.dir}/union_target_mm_unflattened;

select * from union_target_mm_unflattened;

create table union_target_acid_unflattened (val string) partitioned by (dt string) stored as ORC TBLPROPERTIES ('transactional'='true');

explain insert into table union_target_acid_unflattened partition (dt='20230817') select ful.* from (select val from union_target_acid_unflattened where dt='20230816') ful left join (select val from test1 where dt='20230817') inc on ful.val=inc.val union all select test1.val from test1 where dt='20230817';

insert into table union_target_acid_unflattened partition (dt='20230817') select ful.* from (select val from union_target_acid_unflattened where dt='20230816') ful left join (select val from test1 where dt='20230817') inc on ful.val=inc.val union all select test1.val from test1 where dt='20230817';

dfs -ls -R ${hiveconf:hive.metastore.warehouse.dir}/union_target_acid_unflattened;

select * from union_target_acid_unflattened;

-- TESTS FOR DIRECT & NON FLATTENED

set hive.acid.direct.insert.enabled=true;

create table union_target_mm_directinsert_unflattened (val string) partitioned by (dt string) stored as avro TBLPROPERTIES ('transactional'='true', 'transactional_properties'='insert_only');

explain insert overwrite table union_target_mm_directinsert_unflattened partition (dt='20230817') select ful.* from (select val from union_target_mm_directinsert_unflattened where dt='20230816') ful left join (select val from test1 where dt='20230817') inc on ful.val=inc.val union all select test1.val from test1 where dt='20230817';

insert overwrite table union_target_mm_directinsert_unflattened partition (dt='20230817') select ful.* from (select val from union_target_mm_directinsert_unflattened where dt='20230816') ful left join (select val from test1 where dt='20230817') inc on ful.val=inc.val union all select test1.val from test1 where dt='20230817';

dfs -ls -R ${hiveconf:hive.metastore.warehouse.dir}/union_target_mm_directinsert_unflattened;

select * from union_target_mm_directinsert_unflattened;

create table union_target_acid_nondirectinsert_flattened (val string) partitioned by (dt string) stored as ORC TBLPROPERTIES ('transactional'='true');

explain insert into table union_target_acid_nondirectinsert_flattened partition (dt='20230817') select ful.* from (select val from union_target_acid_nondirectinsert_flattened where dt='20230816') ful left join (select val from test1 where dt='20230817') inc on ful.val=inc.val union all select test1.val from test1 where dt='20230817';

insert into table union_target_acid_nondirectinsert_flattened partition (dt='20230817') select ful.* from (select val from union_target_acid_nondirectinsert_flattened where dt='20230816') ful left join (select val from test1 where dt='20230817') inc on ful.val=inc.val union all select test1.val from test1 where dt='20230817';

dfs -ls -R ${hiveconf:hive.metastore.warehouse.dir}/union_target_acid_nondirectinsert_flattened;

select * from union_target_acid_nondirectinsert_flattened;