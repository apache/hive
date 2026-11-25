--! qt:dataset:src
--! qt:replace:/rowcount = ([0-9.E]+), cumulative cost = \{.*\}, id = [0-9]+/rowcount: $1/

-- HIVE-29625: planner estimates when an NDV is unknown, with known-NDV cases for comparison. The metastore stores
-- no NDV for binary columns.
-- explain cbo joincost keeps Hive's statistics; the replace above keeps its row counts visible. Each estimate is
-- followed by the actual row count.

create table ndv_unknown_bin stored as orc as
select cast(value as binary) as kb, value as ks, cast(key as int) as ki from src;
analyze table ndv_unknown_bin compute statistics for columns;
create table ndv_known_dim stored as orc as select distinct value as v from src where cast(key as int) < 100;
analyze table ndv_known_dim compute statistics for columns;

-- GROUP BY an unknown-NDV key
explain cbo joincost select kb, count(*) from ndv_unknown_bin group by kb;
select count(*) from (select kb from ndv_unknown_bin group by kb) t;

-- GROUP BY an unknown-NDV key and a known one
explain cbo joincost select kb, ki, count(*) from ndv_unknown_bin group by kb, ki;
select count(*) from (select kb, ki from ndv_unknown_bin group by kb, ki) t;

-- Equality filter
explain cbo joincost select ks from ndv_unknown_bin where kb = cast('val_100' as binary);
select count(*) from ndv_unknown_bin where kb = cast('val_100' as binary);

-- Inequality filter
explain cbo joincost select ks from ndv_unknown_bin where kb <> cast('val_100' as binary);
select count(*) from ndv_unknown_bin where kb <> cast('val_100' as binary);

-- IN list
explain cbo joincost select ks from ndv_unknown_bin
where kb in (cast('val_1' as binary), cast('val_2' as binary), cast('val_3' as binary));
select count(*) from ndv_unknown_bin
where kb in (cast('val_1' as binary), cast('val_2' as binary), cast('val_3' as binary));

-- NOT over an IN list
explain cbo joincost select ks from ndv_unknown_bin
where not (kb in (cast('val_1' as binary), cast('val_2' as binary)));
select count(*) from ndv_unknown_bin where not (kb in (cast('val_1' as binary), cast('val_2' as binary)));

-- AND with a known-NDV predicate
explain cbo joincost select ks from ndv_unknown_bin where kb = cast('val_100' as binary) and ks = 'val_100';
select count(*) from ndv_unknown_bin where kb = cast('val_100' as binary) and ks = 'val_100';

-- OR with a known-NDV predicate
explain cbo joincost select ks from ndv_unknown_bin where kb = cast('val_100' as binary) or ks = 'val_200';
select count(*) from ndv_unknown_bin where kb = cast('val_100' as binary) or ks = 'val_200';

-- Equality between an unknown-NDV column and a known-NDV expression
explain cbo joincost select ks from ndv_unknown_bin where kb = cast(ks as binary);
select count(*) from ndv_unknown_bin where kb = cast(ks as binary);

-- Join of two unknown-NDV keys
explain cbo joincost select a.ks, b.ks from ndv_unknown_bin a join ndv_unknown_bin b on a.kb = b.kb;
select count(*) from ndv_unknown_bin a join ndv_unknown_bin b on a.kb = b.kb;

-- Join of an unknown-NDV key with a known-NDV key
explain cbo joincost select f.ks, d.v from ndv_unknown_bin f join ndv_known_dim d on f.kb = cast(d.v as binary);
select count(*) from ndv_unknown_bin f join ndv_known_dim d on f.kb = cast(d.v as binary);

-- Semi join of an unknown-NDV key with a known-NDV key
explain cbo joincost select f.ks from ndv_unknown_bin f where f.kb in (select cast(d.v as binary) from ndv_known_dim d);
select count(*) from ndv_unknown_bin f where f.kb in (select cast(d.v as binary) from ndv_known_dim d);

-- Anti join of an unknown-NDV key with a known-NDV key
explain cbo joincost select f.ks from ndv_unknown_bin f
where not exists (select 1 from ndv_known_dim d where cast(d.v as binary) = f.kb);
select count(*) from ndv_unknown_bin f where not exists (select 1 from ndv_known_dim d where cast(d.v as binary) = f.kb);

-- Left outer join on unknown-NDV keys with a non-equi residual
explain cbo joincost select a.ks, b.ks from ndv_unknown_bin a left outer join ndv_unknown_bin b
on a.kb = b.kb and a.ki < b.ki;
select count(*) from ndv_unknown_bin a left outer join ndv_unknown_bin b on a.kb = b.kb and a.ki < b.ki;

-- A filter above an outer join that the statistics cannot estimate
explain cbo joincost select a.ks, b.ks from ndv_unknown_bin a left outer join ndv_unknown_bin b on a.ks = b.ks
where coalesce(b.kb, a.kb) = cast('val_1' as binary);
select count(*) from ndv_unknown_bin a left outer join ndv_unknown_bin b on a.ks = b.ks
where coalesce(b.kb, a.kb) = cast('val_1' as binary);

-- An aggregate on an unknown-NDV key feeding a three-way join
explain cbo joincost
select g.c, f.ki, d.v from (select kb, count(*) c from ndv_unknown_bin group by kb) g
join ndv_unknown_bin f on g.kb = f.kb join ndv_known_dim d on f.ks = d.v;
select count(*) from (select kb, count(*) c from ndv_unknown_bin group by kb) g
join ndv_unknown_bin f on g.kb = f.kb join ndv_known_dim d on f.ks = d.v;

-- Primary key inferred from statistics (dense range); the foreign key side has an unknown NDV. NOT NULL keeps
-- the key side unfiltered, so the NDV scaling factor is evaluated (unknown here).
create table ndv_unknown_key (k int not null disable novalidate rely, v string) stored as orc;
insert into ndv_unknown_key select row_number() over (order by key, value), value from src;
analyze table ndv_unknown_key compute statistics for columns;
explain cbo joincost select d.v, b.ks from ndv_unknown_key d join ndv_unknown_bin b on d.k = length(b.kb);
explain cbo joincost select d.v, b.ks from ndv_unknown_bin b join ndv_unknown_key d on length(b.kb) = d.k;
select count(*) from ndv_unknown_key d join ndv_unknown_bin b on d.k = length(b.kb);

-- Primary and foreign key from RELY constraints, both sides analyzed, as a plain query and in EXPLAIN
create table ndv_unknown_pk2 (k binary, v string) stored as orc;
create table ndv_unknown_fk2 (k binary, v string) stored as orc;
alter table ndv_unknown_pk2 add constraint ndv_unknown_pk2_pk primary key (k) disable novalidate rely;
alter table ndv_unknown_fk2 add constraint ndv_unknown_fk2_fk foreign key (k) references ndv_unknown_pk2(k)
disable novalidate rely;
insert into ndv_unknown_pk2 values (cast('a' as binary), 'one'), (cast('b' as binary), 'two');
insert into ndv_unknown_fk2 values (cast('a' as binary), 'x'), (cast('c' as binary), 'z');
analyze table ndv_unknown_pk2 compute statistics for columns;
analyze table ndv_unknown_fk2 compute statistics for columns;
explain cbo joincost select p.v, f.v from ndv_unknown_pk2 p join ndv_unknown_fk2 f on p.k = f.k;
select p.v, f.v from ndv_unknown_pk2 p join ndv_unknown_fk2 f on p.k = f.k;

-- Primary and foreign key from RELY constraints; only the foreign key side has column statistics
set hive.stats.column.autogather=false;
create table ndv_unknown_pk (k binary, v string) stored as orc;
create table ndv_unknown_fk (k binary, v string) stored as orc;
alter table ndv_unknown_pk add constraint ndv_unknown_pk_pk primary key (k) disable novalidate rely;
alter table ndv_unknown_fk add constraint ndv_unknown_fk_fk foreign key (k) references ndv_unknown_pk(k)
disable novalidate rely;
insert into ndv_unknown_pk select cast(value as binary), max(key) from src group by value;
insert into ndv_unknown_fk select cast(value as binary), key from src;
analyze table ndv_unknown_fk compute statistics for columns;

explain cbo joincost select p.v, f.v from ndv_unknown_pk p join ndv_unknown_fk f on p.k = f.k;
select count(*) from ndv_unknown_pk p join ndv_unknown_fk f on p.k = f.k;

-- A filter on the primary key column applies the NDV scaling factor
explain cbo joincost select p.v, f.v from ndv_unknown_pk p join ndv_unknown_fk f on p.k = f.k
where p.k > cast('val_3' as binary);
explain cbo joincost select p.v, f.v from ndv_unknown_fk f join ndv_unknown_pk p on f.k = p.k
where p.k > cast('val_3' as binary);
select count(*) from ndv_unknown_pk p join ndv_unknown_fk f on p.k = f.k where p.k > cast('val_3' as binary);

-- Integer and string columns whose stored NDV is -1 (unknown), set after ANALYZE. Unlike binary columns they have
-- literal bounds and min/max, so the planner builds SEARCH expressions over them.
create table ndv_unknown_num stored as orc as select cast(key as int) as ki, value as ks, value as kv from src;
analyze table ndv_unknown_num compute statistics for columns;
alter table ndv_unknown_num update statistics for column ki set ('numDVs'='-1','numNulls'='0');
alter table ndv_unknown_num update statistics for column ks set ('numDVs'='-1','numNulls'='0');

-- Integer equality
explain cbo joincost select kv from ndv_unknown_num where ki = 5;
select count(*) from ndv_unknown_num where ki = 5;

-- Integer IN list in a three-way join (a SEARCH while joins are reordered)
explain cbo joincost select f.kv, b.value from ndv_unknown_num f join src a on f.kv = a.value join src b on a.key = b.key
where f.ki in (5, 7);
select count(*) from ndv_unknown_num f join src a on f.kv = a.value join src b on a.key = b.key where f.ki in (5, 7);

-- Integer BETWEEN in a three-way join
explain cbo joincost select f.kv, b.value from ndv_unknown_num f join src a on f.kv = a.value join src b on a.key = b.key
where f.ki between 10 and 100;
select count(*) from ndv_unknown_num f join src a on f.kv = a.value join src b on a.key = b.key
where f.ki between 10 and 100;

-- Integer two-sided range on a single table
explain cbo joincost select kv from ndv_unknown_num where ki > 10 and ki < 100;
select count(*) from ndv_unknown_num where ki > 10 and ki < 100;

-- String equality, and a string IN list in a three-way join
explain cbo joincost select kv from ndv_unknown_num where ks = 'val_100';
select count(*) from ndv_unknown_num where ks = 'val_100';
explain cbo joincost select f.kv, b.value from ndv_unknown_num f join src a on f.kv = a.value join src b on a.key = b.key
where f.ks in ('val_1', 'val_2', 'val_3');
select count(*) from ndv_unknown_num f join src a on f.kv = a.value join src b on a.key = b.key
where f.ks in ('val_1', 'val_2', 'val_3');

-- GROUP BY an integer column with unknown NDV
explain cbo joincost select ki, count(*) from ndv_unknown_num group by ki;
select count(*) from (select ki from ndv_unknown_num group by ki) t;

-- Primary key inferred from statistics (dense range) on an integer column whose stored NDV is unknown, joined
-- to an integer foreign key whose stored NDV is unknown
create table ndv_unknown_dense (k int not null disable novalidate rely, v string) stored as orc;
insert into ndv_unknown_dense select row_number() over (order by key, value), value from src;
analyze table ndv_unknown_dense compute statistics for columns;
alter table ndv_unknown_dense update statistics for column k set ('numDVs'='-1','numNulls'='0');
explain cbo joincost select d.v, f.kv from ndv_unknown_dense d join ndv_unknown_num f on d.k = f.ki;
select count(*) from ndv_unknown_dense d join ndv_unknown_num f on d.k = f.ki;

-- Two-key join: one key with an unknown NDV, the other with a known NDV
explain cbo joincost select f.ks, d.v from ndv_unknown_bin f join ndv_known_dim d
on f.kb = cast(d.v as binary) and f.ks = d.v;
select count(*) from ndv_unknown_bin f join ndv_known_dim d on f.kb = cast(d.v as binary) and f.ks = d.v;

-- Two-key join: both keys unknown on one side, known on the other
explain cbo joincost select f.kv, s.value from ndv_unknown_num f join src s
on f.ki = cast(s.key as int) and f.ks = s.value;
select count(*) from ndv_unknown_num f join src s on f.ki = cast(s.key as int) and f.ks = s.value;

-- Two-key join: one equality unknown on both sides, the other known on both sides
explain cbo joincost select a.ks, b.ks from ndv_unknown_bin a join ndv_unknown_bin b on a.kb = b.kb and a.ks = b.ks;
select count(*) from ndv_unknown_bin a join ndv_unknown_bin b on a.kb = b.kb and a.ks = b.ks;

-- Join of a known-NDV key (left) with an unknown-NDV key (right)
explain cbo joincost select d.v, f.ks from ndv_known_dim d join ndv_unknown_bin f on cast(d.v as binary) = f.kb;
select count(*) from ndv_known_dim d join ndv_unknown_bin f on cast(d.v as binary) = f.kb;

-- Dedup (rn = 1) over an anti join, window partitioned by an unknown-NDV column: the filter stays above the anti join
explain cbo joincost select * from (select f.ks, f.kb, row_number() over (partition by f.kb order by f.ks) rn from ndv_unknown_bin f where not exists (select 1 from ndv_known_dim d where d.v = f.ks)) t where rn = 1;
select count(*) from (select * from (select f.ks, f.kb, row_number() over (partition by f.kb order by f.ks) rn from ndv_unknown_bin f where not exists (select 1 from ndv_known_dim d where d.v = f.ks)) t where rn = 1) t;

-- Filter above ORDER BY LIMIT over an anti join, OR with an unselective unknown-NDV part
explain cbo joincost select * from (select f.ks, f.kb from ndv_unknown_bin f where not exists (select 1 from ndv_known_dim d where d.v = f.ks) order by f.ks limit 300) t
where t.kb <> cast('val_100' as binary) or t.ks = 'val_200';
select count(*) from (select * from (select f.ks, f.kb from ndv_unknown_bin f where not exists (select 1 from ndv_known_dim d where d.v = f.ks) order by f.ks limit 300) t
where t.kb <> cast('val_100' as binary) or t.ks = 'val_200') t2;

-- HAVING over an anti join: the anti join's own condition is estimated, not a filter
explain cbo joincost select f.ks, count(*) c from ndv_unknown_bin f where not exists (select 1 from ndv_known_dim d where d.v = f.ks) group by f.ks having count(*) > 1;
select count(*) from (select f.ks, count(*) c from ndv_unknown_bin f where not exists (select 1 from ndv_known_dim d where d.v = f.ks) group by f.ks having count(*) > 1) t;

-- Known-NDV filter above ORDER BY LIMIT over an anti join: the unknown-NDV guard does not apply; the filter's
-- selectivity is inverted (pre-existing on master, follow-up)
explain cbo joincost select * from (select f.ks, f.kb from ndv_unknown_bin f where not exists (select 1 from ndv_known_dim d where d.v = f.ks) order by f.ks limit 300) t where t.ks = 'val_100';
select count(*) from (select * from (select f.ks, f.kb from ndv_unknown_bin f where not exists (select 1 from ndv_known_dim d where d.v = f.ks) order by f.ks limit 300) t where t.ks = 'val_100') t2;

-- NOT over a boolean function of a known-NDV column
explain cbo joincost select ks from ndv_unknown_bin where not (array_contains(array(ks), 'val_100'));
select count(*) from (select ks from ndv_unknown_bin where not (array_contains(array(ks), 'val_100'))) t;

-- Integer point and range in one SEARCH, numDVs = -1; the SEARCH is estimated while ordering the 3-way join
explain cbo joincost select f.kv from ndv_unknown_num f join src a on f.kv = a.value join src b on a.key = b.key where f.ki = 5 or f.ki > 400;
select count(*) from (select f.kv from ndv_unknown_num f join src a on f.kv = a.value join src b on a.key = b.key where f.ki = 5 or f.ki > 400) t;

-- The remaining cases reach the null handling in the join, NOT and anti-join paths. The last one fails on HEAD
-- and on master, so it comes last.
-- A filter above a left outer join that reads columns from both sides (selectivity unknown)
explain cbo joincost select a.ks, b.ks from ndv_unknown_bin a left outer join ndv_unknown_bin b on a.ks = b.ks
where concat(cast(b.kb as string), a.ks) = 'val_1val_1';
select count(*) from ndv_unknown_bin a left outer join ndv_unknown_bin b on a.ks = b.ks
where concat(cast(b.kb as string), a.ks) = 'val_1val_1';

-- A non-deterministic filter on an unknown-NDV column, kept above an anti join
explain cbo joincost select f.ks from ndv_unknown_bin f where not exists (select 1 from src s where s.value = f.ks)
and f.kb = if(rand() < 2, cast('val_100' as binary), null);
select count(*) from (select f.ks from ndv_unknown_bin f where not exists (select 1 from src s where s.value = f.ks)
and f.kb = if(rand() < 2, cast('val_100' as binary), null)) t;

-- NOT over a boolean function of an unknown-NDV column
explain cbo joincost select ks from ndv_unknown_bin where not (array_contains(array(kb), cast('val_100' as binary)));
select count(*) from ndv_unknown_bin where not (array_contains(array(kb), cast('val_100' as binary)));

-- A filter above a left outer join whose left input is a lateral view (no distinct-count handler); fails on master
explain cbo joincost select a.x, b.value from (select x from src lateral view explode(array(key, value)) t as x) a
left outer join src b on a.x = b.key where concat(a.x, nvl(b.value, '')) = 'val_1';
select count(*) from (select a.x, b.value from (select x from src lateral view explode(array(key, value)) t as x) a
left outer join src b on a.x = b.key where concat(a.x, nvl(b.value, '')) = 'val_1') t2;
