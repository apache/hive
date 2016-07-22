SET hive.exec.dynamic.partition = true;
SET hive.exec.dynamic.partition.mode = nonstrict;

-- SORT_QUERY_RESULTS

drop table if exists alter_table_src;
drop table if exists alter_table_cascade;

create table alter_table_src(c1 string, c2 string);
load data local inpath '../../data/files/dec.txt' overwrite into table alter_table_src;

create table alter_table_cascade (c1 string) partitioned by (p1 string, p2 string);

insert overwrite table alter_table_cascade partition (p1, p2)
  select c1, 'abc', '123' from alter_table_src
  union all
  select c1, cast(null as string), '123' from alter_table_src;

show partitions alter_table_cascade;
describe alter_table_cascade;
describe alter_table_cascade partition (p1='abc', p2='123');
describe alter_table_cascade partition (p1='__HIVE_DEFAULT_PARTITION__', p2='123');
select * from alter_table_cascade where p1='abc';
select * from alter_table_cascade where p1='__HIVE_DEFAULT_PARTITION__';

-- add columns c2 by replace columns (for HIVE-6131)
-- reload data to existing partition __HIVE_DEFAULT_PARTITION__
-- load data to a new partition xyz
-- querying data (form new or existing partition) should return non-NULL values for the new column
alter table alter_table_cascade replace columns (c1 string, c2 string) cascade;
load data local inpath '../../data/files/dec.txt' overwrite into table alter_table_cascade partition (p1='__HIVE_DEFAULT_PARTITION__',p2='123');
load data local inpath '../../data/files/dec.txt' overwrite into table alter_table_cascade partition (p1='xyz', p2='123');
describe alter_table_cascade;
describe alter_table_cascade partition (p1='xyz', p2='123');
describe alter_table_cascade partition (p1='abc', p2='123');
describe alter_table_cascade partition (p1='__HIVE_DEFAULT_PARTITION__', p2='123');
select * from alter_table_cascade where p1='xyz';
select * from alter_table_cascade where p1='abc';
select * from alter_table_cascade where p1='__HIVE_DEFAULT_PARTITION__';

-- Change c2 to decimal(10,0), the change should cascaded to all partitions
-- the c2 value returned should be in decimal(10,0)
alter table alter_table_cascade change c2 c2 decimal(10,0) comment "change datatype" cascade;
describe alter_table_cascade;
describe alter_table_cascade partition (p1='xyz', p2='123');
describe alter_table_cascade partition (p1='abc', p2='123');
describe alter_table_cascade partition (p1='__HIVE_DEFAULT_PARTITION__', p2='123');
select * from alter_table_cascade where p1='xyz';
select * from alter_table_cascade where p1='abc';
select * from alter_table_cascade where p1='__HIVE_DEFAULT_PARTITION__';

-- rename c1 to c2fromc1 and move it to after c2, the change should cascaded to all partitions
alter table alter_table_cascade change c1 c2fromc1 string comment "change position after" after c2 cascade;
describe alter_table_cascade;
describe alter_table_cascade partition (p1='xyz', p2='123');
describe alter_table_cascade partition (p1='abc', p2='123');
describe alter_table_cascade partition (p1='__HIVE_DEFAULT_PARTITION__', p2='123');

-- rename c2fromc1 back to c1 and move to first as c1, the change should cascaded to all partitions
alter table alter_table_cascade change c2fromc1 c1 string comment "change position first" first cascade;
describe alter_table_cascade;
describe alter_table_cascade partition (p1='xyz', p2='123');
describe alter_table_cascade partition (p1='abc', p2='123');
describe alter_table_cascade partition (p1='__HIVE_DEFAULT_PARTITION__', p2='123');

-- Try out replace columns, the change should cascaded to all partitions
alter table alter_table_cascade replace columns (c1 string) cascade;
describe alter_table_cascade;
describe alter_table_cascade partition (p1='xyz', p2='123');
describe alter_table_cascade partition (p1='abc', p2='123');
describe alter_table_cascade partition (p1='__HIVE_DEFAULT_PARTITION__', p2='123');
select * from alter_table_cascade where p1='xyz';
select * from alter_table_cascade where p1='abc';
select * from alter_table_cascade where p1='__HIVE_DEFAULT_PARTITION__';

-- Try add columns, the change should cascaded to all partitions
alter table alter_table_cascade add columns (c2 decimal(14,4)) cascade;
describe alter_table_cascade;
describe alter_table_cascade partition (p1='xyz', p2='123');
describe alter_table_cascade partition (p1='abc', p2='123');
describe alter_table_cascade partition (p1='__HIVE_DEFAULT_PARTITION__', p2='123');
select * from alter_table_cascade where p1='xyz';
select * from alter_table_cascade where p1='abc';
select * from alter_table_cascade where p1='__HIVE_DEFAULT_PARTITION__';

-- 

drop table if exists alter_table_restrict;

create table alter_table_restrict (c1 string) partitioned by (p1 string, p2 string);
insert overwrite table alter_table_restrict partition (p1, p2)
  select c1, 'abc', '123' from alter_table_src
  union all
  select c1, cast(null as string), '123' from alter_table_src;

show partitions alter_table_restrict;
describe alter_table_restrict;
describe alter_table_restrict partition (p1='abc', p2='123');
describe alter_table_restrict partition (p1='__HIVE_DEFAULT_PARTITION__', p2='123');
select * from alter_table_restrict where p1='abc';
select * from alter_table_restrict where p1='__HIVE_DEFAULT_PARTITION__';

-- add columns c2 by replace columns (for HIVE-6131) without cascade
-- only table column definition has changed, partitions do not
-- after replace, only new partition xyz return the value to new added columns but not existing partitions abc and __HIVE_DEFAULT_PARTITION__
alter table alter_table_restrict replace columns (c1 string, c2 string) restrict;
load data local inpath '../../data/files/dec.txt' overwrite into table alter_table_restrict partition (p1='abc', p2='123');
load data local inpath '../../data/files/dec.txt' overwrite into table alter_table_restrict partition (p1='__HIVE_DEFAULT_PARTITION__',p2='123');
load data local inpath '../../data/files/dec.txt' overwrite into table alter_table_restrict partition (p1='xyz', p2='123');
describe alter_table_restrict;
describe alter_table_restrict partition (p1='xyz', p2='123');
describe alter_table_restrict partition (p1='abc', p2='123');
describe alter_table_restrict partition (p1='__HIVE_DEFAULT_PARTITION__', p2='123');
select * from alter_table_restrict where p1='xyz';
select * from alter_table_restrict where p1='abc';
select * from alter_table_restrict where p1='__HIVE_DEFAULT_PARTITION__';

-- Change c2 to decimal(10,0), only limited to table and new partition
alter table alter_table_restrict change c2 c2 decimal(10,0) restrict;
describe alter_table_restrict;
describe alter_table_restrict partition (p1='xyz', p2='123');
describe alter_table_restrict partition (p1='abc', p2='123');
describe alter_table_restrict partition (p1='__HIVE_DEFAULT_PARTITION__', p2='123');

-- Try out replace columns, only limited to table and new partition
alter table alter_table_restrict replace columns (c1 string);
describe alter_table_restrict;
describe alter_table_restrict partition (p1='xyz', p2='123');
describe alter_table_restrict partition (p1='abc', p2='123');
describe alter_table_restrict partition (p1='__HIVE_DEFAULT_PARTITION__', p2='123');

-- Try add columns, only limited to table and new partition
alter table alter_table_restrict add columns (c2 decimal(14,4));
describe alter_table_restrict;
describe alter_table_restrict partition (p1='xyz', p2='123');
describe alter_table_restrict partition (p1='abc', p2='123');
describe alter_table_restrict partition (p1='__HIVE_DEFAULT_PARTITION__', p2='123');
