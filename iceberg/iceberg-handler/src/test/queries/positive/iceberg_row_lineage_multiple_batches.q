-- Row lineage must stay correct across multiple vectorized batches per file.
set hive.fetch.task.conversion=none;

-- Parquet small pages, vectorized read (many batches per file).
set hive.vectorized.execution.enabled=true;
drop table if exists ice_rl_vec_multibatch;

create table ice_rl_vec_multibatch (id int)
stored by iceberg stored as parquet
tblproperties (
  'format-version'='3',
  'write.parquet.page-row-limit'='100'
);

insert into ice_rl_vec_multibatch
select pos + 1 as id
from (
  select posexplode(split(repeat('w,', 2499), ',')) as (pos, val)
) t;

select count(*), count(distinct ROW__LINEAGE__ID), min(ROW__LINEAGE__ID), max(ROW__LINEAGE__ID)
from ice_rl_vec_multibatch;

-- Same table layout, non-vectorized read.
set hive.vectorized.execution.enabled=false;
drop table if exists ice_rl_multibatch;

create table ice_rl_multibatch (id int)
stored by iceberg stored as parquet
tblproperties (
  'format-version'='3',
  'write.parquet.page-row-limit'='100'
);

insert into ice_rl_multibatch
select pos + 1 as id
from (
  select posexplode(split(repeat('w,', 2499), ',')) as (pos, val)
) t;

select count(*), count(distinct ROW__LINEAGE__ID), min(ROW__LINEAGE__ID), max(ROW__LINEAGE__ID)
from ice_rl_multibatch;

drop table ice_rl_vec_multibatch;
drop table ice_rl_multibatch;

-- Insert-only table; vectorized then non-vectorized (ids vs row data).
drop table if exists ice_rl;
create table ice_rl (id int) stored by iceberg tblproperties ('format-version'='3');
insert into ice_rl select pos from (select posexplode(split(space(2999), ' '))) t;

set hive.vectorized.execution.enabled=true;
select count(*), count(distinct ROW__LINEAGE__ID), min(ROW__LINEAGE__ID), max(ROW__LINEAGE__ID) from ice_rl;
select count(*) from ice_rl where ROW__LINEAGE__ID = id;

set hive.vectorized.execution.enabled=false;
select count(*), count(distinct ROW__LINEAGE__ID), min(ROW__LINEAGE__ID), max(ROW__LINEAGE__ID) from ice_rl;
select count(*) from ice_rl where ROW__LINEAGE__ID = id;

-- Copy-on-write update; lineage matches carried rows after rewrite.
drop table if exists ice_rl_cow;
create table ice_rl_cow (id int) stored by iceberg
tblproperties ('format-version'='3', 'write.update.mode'='copy-on-write');
insert into ice_rl_cow select pos from (select posexplode(split(space(2999), ' '))) t;
update ice_rl_cow set id = id + 10000 where id % 1000 = 0;

set hive.vectorized.execution.enabled=true;
select count(*), count(distinct ROW__LINEAGE__ID) from ice_rl_cow;
select count(*) from ice_rl_cow where ROW__LINEAGE__ID = id % 10000;

set hive.vectorized.execution.enabled=false;
select count(*), count(distinct ROW__LINEAGE__ID) from ice_rl_cow;
select count(*) from ice_rl_cow where ROW__LINEAGE__ID = id % 10000;

-- Merge with updates and inserts; mixed null and stored lineage in one read.
drop table if exists ice_rl_merge;
create table ice_rl_merge (id int) stored by iceberg
tblproperties ('format-version'='3', 'write.merge.mode'='copy-on-write');
insert into ice_rl_merge select pos from (select posexplode(split(space(2999), ' '))) t;

drop table if exists ice_rl_source;
create table ice_rl_source (id int);
insert into ice_rl_source select if(pos % 2 = 0, -1 - pos, pos * 3) from (select posexplode(split(space(1999), ' '))) t;
merge into ice_rl_merge t using ice_rl_source s on t.id = s.id
when matched then update set id = t.id + 10000
when not matched then insert values (s.id);

set hive.vectorized.execution.enabled=true;
select count(*), count(distinct ROW__LINEAGE__ID), sum(if(id >= 0 and ROW__LINEAGE__ID = id % 10000, 1, 0)) from ice_rl_merge;

set hive.vectorized.execution.enabled=false;
select count(*), count(distinct ROW__LINEAGE__ID), sum(if(id >= 0 and ROW__LINEAGE__ID = id % 10000, 1, 0)) from ice_rl_merge;

drop table ice_rl;
drop table ice_rl_cow;
drop table ice_rl_source;
drop table ice_rl_merge;
