set hive.map.aggr=false;

set hive.stats.column.autogather=true;

drop table src_stats;

create table src_stats as select * from src;

explain analyze analyze table src_stats compute statistics;

explain analyze analyze table src_stats compute statistics for columns;

drop table src_multi2;

create table src_multi2 like src;

explain analyze insert overwrite table src_multi2 select subq.key, src.value from (select * from src union select * from src1)subq join src on subq.key=src.key;

select count(*) from (select * from src union select * from src1)subq;

insert overwrite table src_multi2 select subq.key, src.value from (select * from src union select * from src1)subq join src on subq.key=src.key;

describe formatted src_multi2;


set hive.mapred.mode=nonstrict;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.support.concurrency=true;
set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;


-- SORT_QUERY_RESULTS

create table acid_uami(i int,
                 de decimal(5,2),
                 vc varchar(128)) clustered by (i) into 2 buckets stored as orc TBLPROPERTIES ('transactional'='true');

insert into table acid_uami values 
    (1, 109.23, 'mary had a little lamb'),
    (6553, 923.19, 'its fleece was white as snow');

insert into table acid_uami values 
    (10, 119.23, 'and everywhere that mary went'),
    (65530, 823.19, 'the lamb was sure to go');

select * from acid_uami order by de;

explain analyze update acid_uami set de = 3.14 where de = 109.23 or de = 119.23;

select * from acid_uami order by de;

update acid_uami set de = 3.14 where de = 109.23 or de = 119.23;

select * from acid_uami order by de;

set hive.support.concurrency=true;
set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;
set hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;

dfs ${system:test.dfs.mkdir} ${system:test.tmp.dir}/delete_orig_table;
dfs -copyFromLocal ../../data/files/alltypesorc ${system:test.tmp.dir}/delete_orig_table/00000_0; 

create table acid_dot(
    ctinyint TINYINT,
    csmallint SMALLINT,
    cint INT,
    cbigint BIGINT,
    cfloat FLOAT,
    cdouble DOUBLE,
    cstring1 STRING,
    cstring2 STRING,
    ctimestamp1 TIMESTAMP,
    ctimestamp2 TIMESTAMP,
    cboolean1 BOOLEAN,
    cboolean2 BOOLEAN) clustered by (cint) into 1 buckets stored as orc location '${system:test.tmp.dir}/delete_orig_table' TBLPROPERTIES ('transactional'='true');

select count(*) from acid_dot;

explain analyze delete from acid_dot where cint < -1070551679;

select count(*) from acid_dot;

delete from acid_dot where cint < -1070551679;

select count(*) from acid_dot;

dfs -rmr ${system:test.tmp.dir}/delete_orig_table;
