-- Row lineage ids of a file read in several vectorized batches: every row keeps its own id
set hive.fetch.task.conversion=none;

create table ice_rl (id int) stored by iceberg tblproperties ('format-version'='3');
insert into ice_rl select pos from (select posexplode(split(space(2999), ' '))) t;

set hive.vectorized.execution.enabled=true;
select count(*), count(distinct ROW__LINEAGE__ID), min(ROW__LINEAGE__ID), max(ROW__LINEAGE__ID) from ice_rl;
select count(*) from ice_rl where ROW__LINEAGE__ID = id;

set hive.vectorized.execution.enabled=false;
select count(*), count(distinct ROW__LINEAGE__ID), min(ROW__LINEAGE__ID), max(ROW__LINEAGE__ID) from ice_rl;
select count(*) from ice_rl where ROW__LINEAGE__ID = id;

-- a rewritten file stores the ids it carries, read in several batches too
create table ice_rl_cow (id int) stored by iceberg
tblproperties ('format-version'='3', 'write.update.mode'='copy-on-write');
insert into ice_rl_cow select pos from (select posexplode(split(space(2999), ' '))) t;
update ice_rl_cow set id = id + 10000 where id % 1000 = 0;

set hive.vectorized.execution.enabled=true;
select count(*), count(distinct ROW__LINEAGE__ID), min(ROW__LINEAGE__ID), max(ROW__LINEAGE__ID) from ice_rl_cow;
select count(*) from ice_rl_cow where ROW__LINEAGE__ID = id % 10000;

set hive.vectorized.execution.enabled=false;
select count(*), count(distinct ROW__LINEAGE__ID), min(ROW__LINEAGE__ID), max(ROW__LINEAGE__ID) from ice_rl_cow;
select count(*) from ice_rl_cow where ROW__LINEAGE__ID = id % 10000;

-- a rewritten file that also holds inserted rows: their stored ids are NULL, the carried rows keep theirs
create table ice_rl_mrg (id int) stored by iceberg
tblproperties ('format-version'='3', 'write.merge.mode'='copy-on-write');
insert into ice_rl_mrg select pos from (select posexplode(split(space(2999), ' '))) t;
create table ice_rl_src (id int);
insert into ice_rl_src select if(pos % 2 = 0, -1 - pos, pos * 3) from (select posexplode(split(space(1999), ' '))) t;
merge into ice_rl_mrg t using ice_rl_src s on t.id = s.id
when matched then update set id = t.id + 10000
when not matched then insert values (s.id);

set hive.vectorized.execution.enabled=true;
select count(*), count(distinct ROW__LINEAGE__ID), min(ROW__LINEAGE__ID), max(ROW__LINEAGE__ID),
  sum(if(id >= 0 and ROW__LINEAGE__ID = id % 10000, 1, 0)) from ice_rl_mrg;

set hive.vectorized.execution.enabled=false;
select count(*), count(distinct ROW__LINEAGE__ID), min(ROW__LINEAGE__ID), max(ROW__LINEAGE__ID),
  sum(if(id >= 0 and ROW__LINEAGE__ID = id % 10000, 1, 0)) from ice_rl_mrg;
