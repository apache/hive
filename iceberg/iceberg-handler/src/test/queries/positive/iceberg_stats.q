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

-- under the metastore source a plain ANALYZE counts the rows; the commit's row count includes the deleted rows
set hive.iceberg.stats.source=metastore;

create external table ice07 (id int, key int) Stored by Iceberg stored as ORC
  TBLPROPERTIES('format-version'='2');

insert into ice07 values (1,1),(2,1),(3,1),(4,1),(5,1);
delete from ice07 where id in (2,4);
analyze table ice07 compute statistics;
show tblproperties ice07('COLUMN_STATS_ACCURATE');
show tblproperties ice07('numRows');
select count(*) from ice07;

-- with iceberg.hive.keep.stats the delete's commit keeps that row count marked accurate; ANALYZE replaces it
set iceberg.hive.keep.stats=true;

create external table ice08 (id int, key int) Stored by Iceberg stored as ORC
  TBLPROPERTIES('format-version'='2');

insert into ice08 values (1,1),(2,1),(3,1),(4,1),(5,1);
delete from ice08 where id in (2,4);
analyze table ice08 compute statistics;
show tblproperties ice08('COLUMN_STATS_ACCURATE');
show tblproperties ice08('numRows');
select count(*) from ice08;

drop table ice01;
drop table ice02;
drop table ice03;
drop table ice07;
drop table ice08;
