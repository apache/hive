-- The records a merge-on-read DELETE/UPDATE/MERGE deletes carry their virtual columns only: the spec, partition, file
-- and position of a record
--! qt:replace:/(.*fromVersion=\[)\S+(\].*)/$1#Masked#$2/
--! qt:replace:/(\s+Version\sinterval\sfrom\:\s+)\d+(\s*)/$1#Masked#/
set hive.explain.user=false;
-- every delete goes through the delete writer
set hive.optimize.delete.metadata.only=false;

create external table ice_src (id int) stored by iceberg;
insert into ice_src values (2), (3), (5), (9), (11);

-- identity, bucket and day partitions, a TIMESTAMP WITH LOCAL TIME ZONE source; struct and array columns
create external table ice_v2 (id int, name string, cat string, ts timestamp, tsltz timestamp with local time zone,
  s struct<`date`:string, `end`:int, `user`:string, `order`:int>, tags array<string>, amount decimal(10,2))
partitioned by spec (cat, bucket(4, id), day(ts), day(tsltz))
stored by iceberg tblproperties ('format-version'='2');

create external table ice_v3 (id int, name string, cat string, ts timestamp, tsltz timestamp with local time zone,
  s struct<`date`:string, `end`:int, `user`:string, `order`:int>, tags array<string>, amount decimal(10,2))
partitioned by spec (cat, bucket(4, id), day(ts), day(tsltz))
stored by iceberg tblproperties ('format-version'='3');

insert into ice_v2 select id, concat('n', id), if(id % 2 = 0, 'even', 'odd'),
  cast(concat('2026-01-0', id) as timestamp), cast(concat('2026-02-0', id, ' 10:00:00') as timestamp with local time zone),
  named_struct('date', concat('d', id), 'end', id * 10, 'user', concat('u', id), 'order', id * 100),
  array(concat('t', id), 'x'), id * 1.5
from (select explode(array(1, 2, 3, 4, 5, 6, 7, 8, 9)) as id) t;
insert into ice_v3 select * from ice_v2;

explain delete from ice_v2 where id = 1;
delete from ice_v2 where id = 1;
delete from ice_v3 where id = 1;

explain update ice_v2 set name = 'upd', s = named_struct('date', 'D', 'end', 0, 'user', 'U', 'order', 0) where id = 4;
update ice_v2 set name = 'upd', s = named_struct('date', 'D', 'end', 0, 'user', 'U', 'order', 0) where id = 4;
update ice_v3 set name = 'upd', s = named_struct('date', 'D', 'end', 0, 'user', 'U', 'order', 0) where id = 4;

explain merge into ice_v2 t using ice_src s on t.id = s.id
when matched and t.id = 2 then update set name = 'mrg', tsltz = cast('2026-03-01 10:00:00' as timestamp with local time zone)
when matched then delete
when not matched then insert values (s.id, 'new', 'odd', cast('2026-01-01' as timestamp),
  cast('2026-03-02 10:00:00' as timestamp with local time zone),
  named_struct('date', 'N', 'end', 1, 'user', 'N', 'order', 1), array('n'), 0);
merge into ice_v2 t using ice_src s on t.id = s.id
when matched and t.id = 2 then update set name = 'mrg', tsltz = cast('2026-03-01 10:00:00' as timestamp with local time zone)
when matched then delete
when not matched then insert values (s.id, 'new', 'odd', cast('2026-01-01' as timestamp),
  cast('2026-03-02 10:00:00' as timestamp with local time zone),
  named_struct('date', 'N', 'end', 1, 'user', 'N', 'order', 1), array('n'), 0);
merge into ice_v3 t using ice_src s on t.id = s.id
when matched and t.id = 2 then update set name = 'mrg', tsltz = cast('2026-03-01 10:00:00' as timestamp with local time zone)
when matched then delete
when not matched then insert values (s.id, 'new', 'odd', cast('2026-01-01' as timestamp),
  cast('2026-03-02 10:00:00' as timestamp with local time zone),
  named_struct('date', 'N', 'end', 1, 'user', 'N', 'order', 1), array('n'), 0);

select * from ice_v2 order by id, name;
select * from ice_v3 order by id, name;
select id, name from ice_v2 where cat = 'even' and ts >= cast('2026-01-04' as timestamp) order by id;
select id, name from ice_v3 where cat = 'even' and ts >= cast('2026-01-04' as timestamp) order by id;
select id, name from ice_v2 where tsltz >= cast('2026-02-05 00:00:00' as timestamp with local time zone) order by id;
select id, name from ice_v3 where tsltz >= cast('2026-02-05 00:00:00' as timestamp with local time zone) order by id;

-- the partition of a DV is the partition of its data file
select d.spec_id, d.`partition`, f.`partition` from default.ice_v3.delete_files d
join default.ice_v3.data_files f on d.referenced_data_file = f.file_path
order by d.spec_id, f.`partition`.cat, f.`partition`.id_bucket, f.`partition`.ts_day;
select count(*) from default.ice_v3.delete_files d
join default.ice_v3.data_files f on d.referenced_data_file = f.file_path
where d.spec_id != f.spec_id or not(d.`partition` <=> f.`partition`);

-- hour, month and truncate partitions; deleted through the row path
create external table ice_xf_v2 (id int, name string, ts timestamp, d date, amount decimal(10,2))
partitioned by spec (hour(ts), month(d), truncate(2, name), truncate(10, amount))
stored by iceberg tblproperties ('format-version'='2');
create external table ice_xf_v3 (id int, name string, ts timestamp, d date, amount decimal(10,2))
partitioned by spec (hour(ts), month(d), truncate(2, name), truncate(10, amount))
stored by iceberg tblproperties ('format-version'='3');

insert into ice_xf_v2 select id, concat('n', id % 3, id), cast(concat('2026-01-01 0', id, ':30:00') as timestamp),
  cast(concat('2026-0', id, '-15') as date), id * 7.25
from (select explode(array(1, 2, 3, 4, 5, 6, 7, 8, 9)) as id) t;
insert into ice_xf_v3 select * from ice_xf_v2;

set hive.vectorized.execution.enabled=false;
delete from ice_xf_v2 where id in (1, 9);
delete from ice_xf_v3 where id in (1, 9);
update ice_xf_v2 set name = 'upd' where id = 4;
update ice_xf_v3 set name = 'upd' where id = 4;
merge into ice_xf_v2 t using ice_src s on t.id = s.id
when matched and t.id = 2 then update set amount = 0
when matched then delete
when not matched then insert values (s.id, 'new', cast('2026-01-01 23:00:00' as timestamp), cast('2026-12-31' as date), 1);
merge into ice_xf_v3 t using ice_src s on t.id = s.id
when matched and t.id = 2 then update set amount = 0
when matched then delete
when not matched then insert values (s.id, 'new', cast('2026-01-01 23:00:00' as timestamp), cast('2026-12-31' as date), 1);
set hive.vectorized.execution.enabled=true;

select * from ice_xf_v2 order by id, name;
select * from ice_xf_v3 order by id, name;
select id, name from ice_xf_v2 where d >= cast('2026-04-01' as date) and name like 'n%' order by id;
select id, name from ice_xf_v3 where d >= cast('2026-04-01' as date) and name like 'n%' order by id;
select count(*) from default.ice_xf_v3.delete_files d
join default.ice_xf_v3.data_files f on d.referenced_data_file = f.file_path
where d.spec_id != f.spec_id or not(d.`partition` <=> f.`partition`);

-- the source column of an earlier spec is not a source of the current spec
create external table ice_evo_v2 (id int, name string, region string, amount int)
partitioned by spec (region) stored by iceberg stored as avro tblproperties ('format-version'='2');
create external table ice_evo_v3 (id int, name string, region string, amount int)
partitioned by spec (region) stored by iceberg stored as orc tblproperties ('format-version'='3');

insert into ice_evo_v2 values (1, 'a', 'eu', 10), (2, 'b', 'us', 20), (3, 'c', 'eu', 30), (4, 'd', 'us', 40);
insert into ice_evo_v3 values (1, 'a', 'eu', 10), (2, 'b', 'us', 20), (3, 'c', 'eu', 30), (4, 'd', 'us', 40);
alter table ice_evo_v2 set partition spec (bucket(2, id));
alter table ice_evo_v3 set partition spec (bucket(2, id));
insert into ice_evo_v2 values (5, 'e', 'eu', 50), (6, 'f', 'us', 60);
insert into ice_evo_v3 values (5, 'e', 'eu', 50), (6, 'f', 'us', 60);

delete from ice_evo_v2 where id in (1, 5);
delete from ice_evo_v3 where id in (1, 5);
update ice_evo_v2 set amount = -1 where id in (2, 6);
update ice_evo_v3 set amount = -1 where id in (2, 6);
merge into ice_evo_v2 t using ice_src s on t.id = s.id
when matched and t.id = 2 then update set name = 'mrg'
when matched then delete
when not matched then insert values (s.id, 'new', 'eu', 0);
merge into ice_evo_v3 t using ice_src s on t.id = s.id
when matched and t.id = 2 then update set name = 'mrg'
when matched then delete
when not matched then insert values (s.id, 'new', 'eu', 0);

select * from ice_evo_v2 order by id, name;
select * from ice_evo_v3 order by id, name;
select id, name, amount from ice_evo_v2 where region = 'us' order by id;
select id, name, amount from ice_evo_v3 where region = 'us' order by id;

-- from unpartitioned through specs of several fields: different transforms, transforms of the same source, a null in
-- one field; a statement deletes from files of every spec
create external table ice_multi_v2 (id int, name string, region string, ts timestamp, dt date)
stored by iceberg tblproperties ('format-version'='2');
create external table ice_multi_v3 (id int, name string, region string, ts timestamp, dt date)
stored by iceberg tblproperties ('format-version'='3');

insert into ice_multi_v2 values (1, 'alpha', 'eu', '2024-01-01 10:00:00', '2024-01-01'),
  (2, 'beta', 'us', '2025-02-02 11:00:00', '2025-02-02');
insert into ice_multi_v3 select * from ice_multi_v2;
alter table ice_multi_v2 set partition spec (region, day(ts), bucket(16, id), truncate(3, name));
alter table ice_multi_v3 set partition spec (region, day(ts), bucket(16, id), truncate(3, name));
insert into ice_multi_v2 values (3, 'gamma', 'eu', '2024-03-03 12:00:00', '2024-03-03'),
  (4, 'delta', null, '2024-04-04 13:00:00', '2024-04-04'), (5, 'epsilon', 'us', null, null);
insert into ice_multi_v3 select * from ice_multi_v2 where id between 3 and 5;
alter table ice_multi_v2 set partition spec (bucket(8, id), truncate(100, id), year(dt), bucket(4, dt));
alter table ice_multi_v3 set partition spec (bucket(8, id), truncate(100, id), year(dt), bucket(4, dt));
insert into ice_multi_v2 values (6, 'zeta', 'eu', '2023-06-06 14:00:00', '2023-06-06'),
  (7, 'eta', 'us', '2022-07-07 15:00:00', '2022-07-07'), (8, 'theta', null, null, null);
insert into ice_multi_v3 select * from ice_multi_v2 where id between 6 and 8;
alter table ice_multi_v2 set partition spec (region, month(ts));
alter table ice_multi_v3 set partition spec (region, month(ts));
insert into ice_multi_v2 values (9, 'iota', 'eu', '2021-09-09 16:00:00', '2021-09-09'),
  (10, 'kappa', 'us', '2021-10-10 17:00:00', '2021-10-10');
insert into ice_multi_v3 select * from ice_multi_v2 where id between 9 and 10;

select spec_id, count(*) from default.ice_multi_v2.data_files group by spec_id order by spec_id;
select spec_id, count(*) from default.ice_multi_v3.data_files group by spec_id order by spec_id;
delete from ice_multi_v2 where id % 2 = 0;
delete from ice_multi_v3 where id % 2 = 0;
update ice_multi_v2 set name = 'upd' where id in (1, 3, 7, 9);
update ice_multi_v3 set name = 'upd' where id in (1, 3, 7, 9);
merge into ice_multi_v2 t using ice_src s on t.id = s.id
when matched and t.id = 3 then update set region = 'ap'
when matched then delete
when not matched then insert values (s.id, 'new', null, null, null);
merge into ice_multi_v3 t using ice_src s on t.id = s.id
when matched and t.id = 3 then update set region = 'ap'
when matched then delete
when not matched then insert values (s.id, 'new', null, null, null);

select * from ice_multi_v2 order by id, name;
select * from ice_multi_v3 order by id, name;
select id, name from ice_multi_v2 where region = 'eu' order by id;
select id, name from ice_multi_v3 where region = 'eu' order by id;
select d.spec_id, count(*) from default.ice_multi_v3.delete_files d
join default.ice_multi_v3.data_files f on d.referenced_data_file = f.file_path group by d.spec_id order by d.spec_id;
select count(*) from default.ice_multi_v3.delete_files d
join default.ice_multi_v3.data_files f on d.referenced_data_file = f.file_path
where d.spec_id != f.spec_id or not(d.`partition` <=> f.`partition`);

-- a field dropped from a V1 spec becomes a void transform: unpartitioned, then a void field next to a real one, then
-- void fields only, then void fields next to a real one; amount is not a partition source: a vectorized Parquet read
-- with a pushed down filter fails when every column of a table is one
create external table ice_void_v2 (id int, name string, region string, amount int)
stored by iceberg tblproperties ('format-version'='1');
create external table ice_void_v3 (id int, name string, region string, amount int)
stored by iceberg tblproperties ('format-version'='1');

insert into ice_void_v2 values (10, 'j', 'eu', 100), (11, 'k', 'us', 110);
insert into ice_void_v3 values (10, 'j', 'eu', 100), (11, 'k', 'us', 110);
alter table ice_void_v2 set partition spec (region, bucket(4, id));
alter table ice_void_v3 set partition spec (region, bucket(4, id));
insert into ice_void_v2 values (1, 'a', 'eu', 10), (2, 'b', 'us', 20), (3, 'c', 'eu', 30);
insert into ice_void_v3 values (1, 'a', 'eu', 10), (2, 'b', 'us', 20), (3, 'c', 'eu', 30);
alter table ice_void_v2 set partition spec (bucket(4, id));
alter table ice_void_v3 set partition spec (bucket(4, id));
insert into ice_void_v2 values (4, 'd', 'eu', 40), (5, 'e', 'us', 50);
insert into ice_void_v3 values (4, 'd', 'eu', 40), (5, 'e', 'us', 50);
alter table ice_void_v2 set partition spec();
alter table ice_void_v3 set partition spec();
insert into ice_void_v2 values (12, 'l', 'eu', 120), (13, 'm', 'us', 130);
insert into ice_void_v3 values (12, 'l', 'eu', 120), (13, 'm', 'us', 130);
-- a DML into a table whose current spec has void fields only is rejected as a write into a partition
alter table ice_void_v2 set partition spec (truncate(1, name));
alter table ice_void_v3 set partition spec (truncate(1, name));
insert into ice_void_v2 values (14, 'n', 'eu', 140), (15, 'o', 'us', 150);
insert into ice_void_v3 values (14, 'n', 'eu', 140), (15, 'o', 'us', 150);
alter table ice_void_v2 set tblproperties ('format-version'='2');
alter table ice_void_v3 set tblproperties ('format-version'='3');

select spec_id, count(*) from default.ice_void_v2.data_files group by spec_id order by spec_id;
delete from ice_void_v2 where id in (1, 4, 10, 12, 14);
delete from ice_void_v3 where id in (1, 4, 10, 12, 14);
update ice_void_v2 set name = 'upd' where id in (2, 5, 11, 13, 15);
update ice_void_v3 set name = 'upd' where id in (2, 5, 11, 13, 15);
merge into ice_void_v2 t using ice_src s on t.id = s.id
when matched and t.id = 3 then update set name = 'mrg'
when matched then delete
when not matched then insert values (s.id, 'new', 'ap', 0);
merge into ice_void_v3 t using ice_src s on t.id = s.id
when matched and t.id = 3 then update set name = 'mrg'
when matched then delete
when not matched then insert values (s.id, 'new', 'ap', 0);

select * from ice_void_v2 order by id, name;
select * from ice_void_v3 order by id, name;
select id, name from ice_void_v2 where region = 'eu' order by id;
select id, name from ice_void_v3 where region = 'eu' order by id;
select count(*) from default.ice_void_v3.delete_files d
join default.ice_void_v3.data_files f on d.referenced_data_file = f.file_path
where d.spec_id != f.spec_id or not(d.`partition` <=> f.`partition`);

-- string partitions that differ only in NULL, '', 'null' and ' '; a truncate of ''
create external table ice_str_v2 (id int, s string) partitioned by spec (s, truncate(1, s))
stored by iceberg tblproperties ('format-version'='2');
create external table ice_str_v3 (id int, s string) partitioned by spec (s, truncate(1, s))
stored by iceberg tblproperties ('format-version'='3');

insert into ice_str_v2 values (1, null), (2, null), (3, ''), (4, ''), (5, 'null'), (6, 'null'), (7, ' '), (8, ' ');
insert into ice_str_v3 values (1, null), (2, null), (3, ''), (4, ''), (5, 'null'), (6, 'null'), (7, ' '), (8, ' ');
select count(*) from default.ice_str_v2.partitions;
delete from ice_str_v2 where id in (1, 3, 5, 7);
delete from ice_str_v3 where id in (1, 3, 5, 7);

select id, s, length(s) from ice_str_v2 order by id;
select id, s, length(s) from ice_str_v3 order by id;
select id from ice_str_v2 where s is null;
select id from ice_str_v2 where s = '';
select id from ice_str_v2 where s = 'null';
select id from ice_str_v2 where s = ' ';
select id from ice_str_v3 where s is null;
select id from ice_str_v3 where s = '';
select id from ice_str_v3 where s = 'null';
select id from ice_str_v3 where s = ' ';
select count(*) from default.ice_str_v3.delete_files d
join default.ice_str_v3.data_files f on d.referenced_data_file = f.file_path
where d.spec_id != f.spec_id or not(d.`partition` <=> f.`partition`);

-- the deletes of a merge-on-read UPDATE and MERGE into a table that deletes in copy-on-write mode
create external table ice_mixed_v2 (id int, name string, cat string)
partitioned by spec (cat, bucket(2, id)) stored by iceberg
tblproperties ('format-version'='2', 'write.delete.mode'='copy-on-write');
create external table ice_mixed_v3 (id int, name string, cat string)
partitioned by spec (cat, bucket(2, id)) stored by iceberg
tblproperties ('format-version'='3', 'write.delete.mode'='copy-on-write');

insert into ice_mixed_v2 values (1, 'a', 'odd'), (2, 'b', 'even'), (3, 'c', 'odd'), (4, 'd', 'even');
insert into ice_mixed_v3 values (1, 'a', 'odd'), (2, 'b', 'even'), (3, 'c', 'odd'), (4, 'd', 'even');

update ice_mixed_v2 set name = 'upd' where id = 4;
update ice_mixed_v3 set name = 'upd' where id = 4;
merge into ice_mixed_v2 t using ice_src s on t.id = s.id
when matched and t.id = 2 then update set name = 'mrg'
when matched then delete
when not matched then insert values (s.id, 'new', 'odd');
merge into ice_mixed_v3 t using ice_src s on t.id = s.id
when matched and t.id = 2 then update set name = 'mrg'
when matched then delete
when not matched then insert values (s.id, 'new', 'odd');

select * from ice_mixed_v2 order by id, name;
select * from ice_mixed_v3 order by id, name;
select id, name from ice_mixed_v2 where cat = 'odd' order by id;
select id, name from ice_mixed_v3 where cat = 'odd' order by id;

-- the deleted records are sorted when the fanout writer is disabled
create external table ice_sorted_v2 (id int, name string, amount int) partitioned by spec (bucket(2, id))
stored by iceberg tblproperties ('format-version'='2', 'write.fanout.enabled'='false');
insert into ice_sorted_v2 values (1, 'a', 10), (2, 'b', 20), (3, 'c', 30), (4, 'd', 40);

explain delete from ice_sorted_v2 where id = 1;
delete from ice_sorted_v2 where id = 1;
update ice_sorted_v2 set amount = -1 where id = 2;
merge into ice_sorted_v2 t using ice_src s on t.id = s.id
when matched and t.id = 3 then update set name = 'mrg'
when matched then delete
when not matched then insert values (s.id, 'new', 0);

select * from ice_sorted_v2 order by id, name;

-- a branch target
alter table ice_v2 create branch b1;
alter table ice_v3 create branch b1;

delete from default.ice_v2.branch_b1 where id = 7;
delete from default.ice_v3.branch_b1 where id = 7;
update default.ice_v2.branch_b1 set name = 'br' where id = 6;
update default.ice_v3.branch_b1 set name = 'br' where id = 6;
merge into default.ice_v2.branch_b1 t using ice_src s on t.id = s.id
when matched and t.id = 11 then update set name = 'brm'
when matched then delete
when not matched then insert values (s.id, 'brn', 'odd', cast('2026-01-01' as timestamp),
  cast('2026-03-02 10:00:00' as timestamp with local time zone),
  named_struct('date', 'B', 'end', 2, 'user', 'B', 'order', 2), array('b'), 0);
merge into default.ice_v3.branch_b1 t using ice_src s on t.id = s.id
when matched and t.id = 11 then update set name = 'brm'
when matched then delete
when not matched then insert values (s.id, 'brn', 'odd', cast('2026-01-01' as timestamp),
  cast('2026-03-02 10:00:00' as timestamp with local time zone),
  named_struct('date', 'B', 'end', 2, 'user', 'B', 'order', 2), array('b'), 0);

select id, name, cat, s from default.ice_v2.branch_b1 order by id, name;
select id, name, cat, s from default.ice_v3.branch_b1 order by id, name;
select id, name from default.ice_v2.branch_b1 where cat = 'odd' order by id;
select id, name from default.ice_v3.branch_b1 where cat = 'odd' order by id;
select count(*) from ice_v2;
select count(*) from ice_v3;

-- the incremental rebuild of a merge-on-read materialized view
set hive.support.concurrency=true;
set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;
set hive.materializedview.rebuild.incremental.factor=0.01;

create external table mv_src (a int, b int) stored by iceberg tblproperties ('format-version'='2');
insert into mv_src values (1, 1), (2, 1), (3, 2);
create materialized view mv_mor stored by iceberg tblproperties ('format-version'='2') as
select b, count(*) c, sum(a) s from mv_src group by b;
insert into mv_src values (4, 1), (5, 3);
explain alter materialized view mv_mor rebuild;
alter materialized view mv_mor rebuild;
select * from mv_mor order by b;
select b, count(*) c, sum(a) s from mv_src group by b order by b;
