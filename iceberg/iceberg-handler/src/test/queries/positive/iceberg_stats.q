--! qt:replace:/(\s+Statistics\: Num rows\: \d+ Data size\:\s+)\S+(\s+Basic stats\: \S+ Column stats\: \S+)/$1#Masked#$2/

set hive.compute.query.using.stats=true;
set hive.explain.user=false;

create table ice01 (id int, key int) Stored by Iceberg stored as ORC 
  TBLPROPERTIES('format-version'='2');

create external table ice02 (id int, key int) Stored by Iceberg stored as ORC
  TBLPROPERTIES('format-version'='2');

insert into ice01 values (1,1),(2,1),(3,1),(4,1),(5,1);
explain select count(*) from ice01;
select count(*) from ice01;

insert into ice02 values (1,1),(2,1),(3,1),(4,1),(5,1);
-- external iceberg also can use fetch task query
explain select count(*) from ice02;
select count(*) from ice02;

-- delete some values
delete from ice01 where id in (2,4);

explain select count(*) from ice01;
select count(*) from ice01;

-- iow
insert overwrite table ice01 select * from ice01;
explain select count(*) from ice01;

-- false means that count(*) query won't use row count stored in HMS
set iceberg.hive.keep.stats=false;

create external table ice03 (id int, key int) Stored by Iceberg stored as ORC
  TBLPROPERTIES('format-version'='2');

insert into ice03 values (1,1),(2,1),(3,1),(4,1),(5,1);
-- Iceberg table can utilize fetch task to directly retrieve the row count from iceberg SnapshotSummary
explain select count(*) from ice03;
select count(*) from ice03;

-- delete some values
delete from ice03 where id in (2,4);

explain select count(*) from ice03;
select count(*) from ice03;

-- iow
insert overwrite table ice03 select * from ice03;
explain select count(*) from ice03;

-- a column analyze brings the basic statistics up to date when no insert gathered them
set hive.stats.autogather=false;

create external table ice04 (id int, key int) Stored by Iceberg stored as ORC
  TBLPROPERTIES('format-version'='2');

insert into ice04 values (1,1),(2,1),(3,1),(4,1),(5,1);
analyze table ice04 compute statistics for columns;
show tblproperties ice04('COLUMN_STATS_ACCURATE');
show tblproperties ice04('numRows');

create external table ice05 (id int) partitioned by (p string) Stored by Iceberg stored as ORC
  TBLPROPERTIES('format-version'='2');

insert into ice05 values (1,'a'),(2,'a'),(3,'b');
analyze table ice05 compute statistics for columns;
show tblproperties ice05('COLUMN_STATS_ACCURATE');
show tblproperties ice05('numRows');

set hive.stats.autogather=true;

-- under the metastore stats source the analyze scan is what brings the row count up to date
set hive.iceberg.stats.source=metastore;

create external table ice06 (id int, key int) Stored by Iceberg stored as ORC
  TBLPROPERTIES('format-version'='2');

insert into ice06 values (1,1),(2,1),(3,1),(4,1),(5,1);
delete from ice06 where id in (2,4);
analyze table ice06 compute statistics for columns;
explain select count(*) from ice06;
select count(*) from ice06;

set hive.iceberg.stats.source=iceberg;

drop table ice01;
drop table ice02;
drop table ice03;
drop table ice04;
drop table ice05;
drop table ice06;
