--! qt:dataset:src1
--! qt:dataset:src

set metastore.integral.jdo.pushdown=true;
-- create testing table.
create temporary table alter_coltype_temp(key string, value string) partitioned by (dt string, ts string);

-- insert and create a partition.
insert overwrite table alter_coltype_temp partition(dt='100', ts='6.30') select * from src1;

desc alter_coltype_temp;

-- select with paritition predicate.
select count(*) from alter_coltype_temp where dt = '100';

-- alter partition key column data type for dt column.
alter table alter_coltype_temp partition column (dt int);

-- load a new partition using new data type.
insert overwrite table alter_coltype_temp partition(dt=100, ts='3.0') select * from src1;

-- make sure the partition predicate still works.
select count(*) from alter_coltype_temp where dt = '100';
explain extended select count(*) from alter_coltype_temp where dt = '100';

-- alter partition key column data type for ts column.
alter table alter_coltype_temp partition column (ts double);

alter table alter_coltype_temp partition column (dt string);

-- load a new partition using new data type.
insert overwrite table alter_coltype_temp partition(dt='100', ts=3.0) select * from src1;

--  validate partition key column predicate can still work.
select count(*) from alter_coltype_temp where ts = '6.30';
explain extended select count(*) from alter_coltype_temp where ts = '6.30';

--  validate partition key column predicate on two different partition column data type
--  can still work.
select count(*) from alter_coltype_temp where ts = 3.0 and dt=100;
explain extended select count(*) from alter_coltype_temp where ts = 3.0 and dt=100;

-- query where multiple partition values (of different datatypes) are being selected
select key, value, dt, ts from alter_coltype_temp where dt is not null;
explain extended select key, value, dt, ts from alter_coltype_temp where dt is not null;

select count(*) from alter_coltype_temp where ts = 3.0;

-- make sure the partition predicate still works.
select count(*) from alter_coltype_temp where dt = '100';

desc alter_coltype_temp;
set hive.typecheck.on.insert=false;
desc alter_coltype_temp partition (dt='100', ts='6.30');
desc alter_coltype_temp partition (dt='100', ts=3.0);

drop table alter_coltype_temp;

create database pt;

create temporary table pt.alterdynamic_part_table_temp(intcol string) partitioned by (partcol1 string, partcol2 string);

set hive.exec.dynamic.partition.mode=nonstrict;

insert into table pt.alterdynamic_part_table_temp partition(partcol1, partcol2) select '1', '1', '1' from src where key=150 limit 5;

insert into table pt.alterdynamic_part_table_temp partition(partcol1, partcol2) select '1', '2', '1' from src where key=150 limit 5;
insert into table pt.alterdynamic_part_table_temp partition(partcol1, partcol2) select NULL, '1', '1' from src where key=150 limit 5;

alter table pt.alterdynamic_part_table_temp partition column (partcol1 int);

explain extended select intcol from pt.alterdynamic_part_table_temp where partcol1='1' and partcol2='1';

explain extended select intcol from pt.alterdynamic_part_table_temp where (partcol1='2' and partcol2='1')or (partcol1='1' and partcol2='__HIVE_DEFAULT_PARTITION__');
select intcol from pt.alterdynamic_part_table_temp where (partcol1='2' and partcol2='1')or (partcol1='1' and partcol2='__HIVE_DEFAULT_PARTITION__');

drop table pt.alterdynamic_part_table_temp;
drop database pt;
