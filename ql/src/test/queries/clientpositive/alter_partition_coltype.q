--! qt:dataset:src1
--! qt:dataset:src
-- create testing table.
create table alter_coltype(key string, value string) partitioned by (dt string, ts string);

-- insert and create a partition.
insert overwrite table alter_coltype partition(dt='100', ts='6.30') select * from src1;

desc alter_coltype;

-- select with paritition predicate.
select count(*) from alter_coltype where dt = '100';

-- alter partition key column data type for dt column.
alter table alter_coltype partition column (dt int);

-- load a new partition using new data type.
insert overwrite table alter_coltype partition(dt=100, ts='3.0') select * from src1;

-- make sure the partition predicate still works. 
select count(*) from alter_coltype where dt = '100';
explain extended select count(*) from alter_coltype where dt = '100';

-- alter partition key column data type for ts column.
alter table alter_coltype partition column (ts double);

alter table alter_coltype partition column (dt string);

-- load a new partition using new data type.
insert overwrite table alter_coltype partition(dt='100', ts=3.0) select * from src1;

--  validate partition key column predicate can still work.
select count(*) from alter_coltype where ts = '6.30';
explain extended select count(*) from alter_coltype where ts = '6.30';

--  validate partition key column predicate on two different partition column data type 
--  can still work.
select count(*) from alter_coltype where ts = 3.0 and dt=100;
explain extended select count(*) from alter_coltype where ts = 3.0 and dt=100;

-- query where multiple partition values (of different datatypes) are being selected 
select key, value, dt, ts from alter_coltype where dt is not null;
explain extended select key, value, dt, ts from alter_coltype where dt is not null;

select count(*) from alter_coltype where ts = 3.0;

-- make sure the partition predicate still works. 
select count(*) from alter_coltype where dt = '100';

desc alter_coltype;
set hive.typecheck.on.insert=false;
desc alter_coltype partition (dt='100', ts='6.30');
desc alter_coltype partition (dt='100', ts=3.0);

drop table alter_coltype;

create database pt;

create table pt.alterdynamic_part_table(intcol string) partitioned by (partcol1 string, partcol2 string);


insert into table pt.alterdynamic_part_table partition(partcol1, partcol2) select '1', '1', '1' from src where key=150 limit 5;

insert into table pt.alterdynamic_part_table partition(partcol1, partcol2) select '1', '2', '1' from src where key=150 limit 5;
insert into table pt.alterdynamic_part_table partition(partcol1, partcol2) select NULL, '1', '1' from src where key=150 limit 5;
insert into table pt.alterdynamic_part_table partition(partcol1, partcol2) select '2', '2', NULL;

alter table pt.alterdynamic_part_table partition column (partcol1 int);

explain extended select intcol from pt.alterdynamic_part_table where partcol1='1' and partcol2='1';

explain extended select intcol from pt.alterdynamic_part_table where (partcol1='2' and partcol2='1')or (partcol1='1' and partcol2='__HIVE_DEFAULT_PARTITION__');
select intcol from pt.alterdynamic_part_table where (partcol1='2' and partcol2='1')or (partcol1='1' and partcol2='__HIVE_DEFAULT_PARTITION__');

alter table pt.alterdynamic_part_table partition column (partcol2 int);
select intcol from pt.alterdynamic_part_table where (partcol1=2 and partcol2=1) or (partcol1=2 and isnull(partcol2));

drop table pt.alterdynamic_part_table;
drop database pt;
