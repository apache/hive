set hive.vectorized.execution.enabled=false;
set hive.mapred.mode=nonstrict;
set hive.metastore.disallow.incompatible.col.type.changes=false;
create table if not exists alltypes_n0 (
 bo boolean,
 ti tinyint,
 si smallint,
 i int,
 bi bigint,
 f float,
 d double,
 de decimal(10,3),
 ts timestamp,
 da date,
 s string,
 c char(5),
 vc varchar(5),
 m map<string, string>,
 l array<int>,
 st struct<c1:int, c2:string>
) row format delimited fields terminated by '|'
collection items terminated by ','
map keys terminated by ':' stored as textfile;

create table if not exists alltypes_orc_n0 (
 bo boolean,
 ti tinyint,
 si smallint,
 i int,
 bi bigint,
 f float,
 d double,
 de decimal(10,3),
 ts timestamp,
 da date,
 s string,
 c char(5),
 vc varchar(5),
 m map<string, string>,
 l array<int>,
 st struct<c1:int, c2:string>
) stored as orc;

load data local inpath '../../data/files/alltypes2.txt' overwrite into table alltypes_n0;

insert overwrite table alltypes_orc_n0 select * from alltypes_n0;

select * from alltypes_orc_n0;

SET hive.exec.schema.evolution=true;

alter table alltypes_orc_n0 change si si int;
select * from alltypes_orc_n0;

alter table alltypes_orc_n0 change si si bigint;
alter table alltypes_orc_n0 change i i bigint;
select * from alltypes_orc_n0;

set hive.vectorized.execution.enabled=true;
set hive.fetch.task.conversion=none;

explain select ti, si, i, bi from alltypes_orc_n0;
select ti, si, i, bi from alltypes_orc_n0;

SET hive.exec.schema.evolution=false;

set hive.exec.dynamic.partition.mode=nonstrict;
create table src_part_orc (key int, value string) partitioned by (ds string) stored as orc;
insert overwrite table src_part_orc partition(ds) select key, value, ds from srcpart where ds is not null;

select * from src_part_orc limit 10;

alter table src_part_orc change key key bigint;
select * from src_part_orc limit 10;
reset hive.metastore.disallow.incompatible.col.type.changes;
