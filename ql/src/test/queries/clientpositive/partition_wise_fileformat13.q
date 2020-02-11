--! qt:dataset:src
set hive.mapred.mode=nonstrict;
set hive.input.format = org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;

-- This tests that the schema can be changed for partitioned tables for binary serde data for joins
create table T1_n16(key string, value string) partitioned by (dt string) stored as rcfile;
alter table T1_n16 set serde 'org.apache.hadoop.hive.serde2.columnar.LazyBinaryColumnarSerDe';
insert overwrite table T1_n16 partition (dt='1') select * from src where key = 238 or key = 97;
set hive.metastore.disallow.incompatible.col.type.changes=false;
alter table T1_n16 change key key int;

insert overwrite table T1_n16 partition (dt='2') select * from src where key = 238 or key = 97;

alter table T1_n16 change key key string;

create table T2_n10(key string, value string) partitioned by (dt string) stored as rcfile;
insert overwrite table T2_n10 partition (dt='1') select * from src where key = 238 or key = 97;

select /* + MAPJOIN(a) */ count(*) FROM T1_n16 a JOIN T2_n10 b ON a.key = b.key;
select count(*) FROM T1_n16 a JOIN T2_n10 b ON a.key = b.key;
reset hive.metastore.disallow.incompatible.col.type.changes;
