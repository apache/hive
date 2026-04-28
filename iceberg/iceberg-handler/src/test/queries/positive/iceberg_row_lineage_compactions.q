-- SORT_QUERY_RESULTS

--! qt:replace:/(MAJOR\s+succeeded\s+)[a-zA-Z0-9\-\.\s+]+(\s+manual)/$1#Masked#$2/
--! qt:replace:/(MAJOR\s+refused\s+)[a-zA-Z0-9\-\.\s+]+(\s+manual)/$1#Masked#$2/
--! qt:replace:/(MINOR\s+succeeded\s+)[a-zA-Z0-9\-\.\s+]+(\s+manual)/$1#Masked#$2/
--! qt:replace:/(MINOR\s+refused\s+)[a-zA-Z0-9\-\.\s+]+(\s+manual)/$1#Masked#$2/
-- Mask compaction id as they will be allocated in parallel threads
--! qt:replace:/^(\d+)(\t.*\tmanual\ticeberg\t)/#Masked#$2/

set hive.llap.io.enabled=true;
set hive.vectorized.execution.enabled=true;

create database if not exists ice_comp_all with dbproperties('hive.compactor.worker.pool'='iceberg');
use ice_comp_all;

-- Partitioned table with minor and major compaction
create table part_tbl(
  id int,
  data string
)
partitioned by (dept_id int)
stored by iceberg stored as parquet
tblproperties (
  'format-version'='3',
  'hive.compactor.worker.pool'='iceberg',
  -- Use target.size only to make Parquet data files (~996 bytes) count as fragments.
  -- Default fragment ratio is 8, so fragment_size = target_size / 8.
  -- Pick target_size > 996 * 8 (7968) so files are treated as fragment files and minor compaction is eligible.
  'compactor.threshold.target.size'='8000',
  'compactor.threshold.min.input.files'='2',
  'compactor.threshold.delete.file.ratio'='0.0'
);

insert into part_tbl values (1,'p1', 10);
insert into part_tbl values (2,'p2', 10);
insert into part_tbl values (3,'p3', 20);
insert into part_tbl values (4,'p4', 20);

SELECT *, ROW__LINEAGE__ID, LAST__UPDATED__SEQUENCE__NUMBER
FROM part_tbl
ORDER BY ROW__LINEAGE__ID;

alter table part_tbl compact 'minor' and wait pool 'iceberg';

SELECT *, ROW__LINEAGE__ID, LAST__UPDATED__SEQUENCE__NUMBER
FROM part_tbl
ORDER BY ROW__LINEAGE__ID;

show compactions;

-- For MAJOR eligibility, avoid treating files as "fragments" by lowering target.size
alter table part_tbl set tblproperties ('compactor.threshold.target.size'='1500');

merge into part_tbl t
using (select 1 as id, 'p1_upd' as data, 10 as dept_id) s
on t.dept_id = s.dept_id and t.id = s.id
when matched then update set data = s.data;

SELECT *, ROW__LINEAGE__ID, LAST__UPDATED__SEQUENCE__NUMBER
FROM part_tbl
ORDER BY ROW__LINEAGE__ID;

alter table part_tbl compact 'major' and wait pool 'iceberg';

SELECT *, ROW__LINEAGE__ID, LAST__UPDATED__SEQUENCE__NUMBER
FROM part_tbl
ORDER BY ROW__LINEAGE__ID;

show compactions;

-- Partition evolution
alter table part_tbl set tblproperties ('compactor.threshold.target.size'='8000');

alter table part_tbl set partition spec(dept_id, id);

insert into part_tbl values (5,'p5', 10);
insert into part_tbl values (6,'p6', 20);

SELECT *, ROW__LINEAGE__ID, LAST__UPDATED__SEQUENCE__NUMBER
FROM part_tbl
ORDER BY ROW__LINEAGE__ID;

alter table part_tbl compact 'minor' and wait pool 'iceberg';

SELECT *, ROW__LINEAGE__ID, LAST__UPDATED__SEQUENCE__NUMBER
FROM part_tbl
ORDER BY ROW__LINEAGE__ID;

show compactions;

-- Unpartitioned table with minor and major compaction
create table unpart_tbl(
  id int,
  data string
)
stored by iceberg stored as parquet
tblproperties (
  'format-version'='3',
  'hive.compactor.worker.pool'='iceberg',
  -- Use target.size only to make Parquet data files (~996 bytes) count as fragments (default ratio 8).
  'compactor.threshold.target.size'='8000',
  'compactor.threshold.min.input.files'='2',
  'compactor.threshold.delete.file.ratio'='0.0'
);

insert into unpart_tbl values (1,'a');
insert into unpart_tbl values (2,'b');
insert into unpart_tbl values (3,'c');
insert into unpart_tbl values (4,'d');

SELECT *, ROW__LINEAGE__ID, LAST__UPDATED__SEQUENCE__NUMBER
FROM unpart_tbl
ORDER BY ROW__LINEAGE__ID;

alter table unpart_tbl compact 'minor' and wait pool 'iceberg';

SELECT *, ROW__LINEAGE__ID, LAST__UPDATED__SEQUENCE__NUMBER
FROM unpart_tbl
ORDER BY ROW__LINEAGE__ID;

show compactions;

-- For MAJOR eligibility, avoid treating files as "fragments" by lowering target.size, then create deletes via MERGE.
alter table unpart_tbl set tblproperties ('compactor.threshold.target.size'='1500');

merge into unpart_tbl t
using (select 1 as id, 'a_upd' as data) s
on t.id = s.id
when matched then update set data = s.data;

SELECT *, ROW__LINEAGE__ID, LAST__UPDATED__SEQUENCE__NUMBER
FROM unpart_tbl
ORDER BY ROW__LINEAGE__ID;

alter table unpart_tbl compact 'major' and wait pool 'iceberg';

SELECT *, ROW__LINEAGE__ID, LAST__UPDATED__SEQUENCE__NUMBER
FROM unpart_tbl
ORDER BY ROW__LINEAGE__ID;

show compactions;
