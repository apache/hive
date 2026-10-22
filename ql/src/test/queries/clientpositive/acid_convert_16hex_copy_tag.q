-- Convert a non-ACID managed ORC table to full ACID after renaming the
-- inserted file to the 16-hex per-query "uniqueness tag" copy suffix that
-- form introduced for unstable-rename filesystems (S3A/S3N/S3/GS).
-- Before that change, the ORIGINAL_PATTERN_COPY regex in
-- TransactionalValidationListener only matched `_copy_[0-9]+`, so the
-- pre-existing file would be flagged as an "unexpected data file name
-- format" and the ALTER TABLE ... transactional=true would fail. This
-- test locks in the widened pattern.

set hive.create.as.acid=false;
set hive.create.as.insert.only=false;
set hive.strict.managed.tables=false;

set hive.support.concurrency=true;
set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;
set hive.mapred.mode=nonstrict;

drop table if exists t_acid_convert_16hex;

create table t_acid_convert_16hex (id int, name string)
  stored as orc
  tblproperties ('transactional'='false');

insert into t_acid_convert_16hex values (1, 'a'), (2, 'b');

-- What the insert produced on a stable-rename FS (local test): expect a
-- single `000000_0` (or similar numeric) file.
dfs -ls ${hiveconf:hive.metastore.warehouse.dir}/t_acid_convert_16hex;

-- Rename it to the 16-hex form that Hive.mvFile would have chosen on e.g. S3A
-- This is exactly the shape the widened TransactionalValidationListener.ORIGINAL_PATTERN_COPY has to accept.
dfs -mv ${hiveconf:hive.metastore.warehouse.dir}/t_acid_convert_16hex/000000_0
        ${hiveconf:hive.metastore.warehouse.dir}/t_acid_convert_16hex/000000_0_copy_f0796c02aef8435d;

dfs -ls ${hiveconf:hive.metastore.warehouse.dir}/t_acid_convert_16hex;

-- The conversion. This is what would blow up with
--   IllegalStateException: Unexpected data file name format.
--   Cannot convert default.t_acid_convert_16hex to transactional table.
-- if ORIGINAL_PATTERN_COPY still required a numeric copy index.
alter table t_acid_convert_16hex set tblproperties ('transactional'='true', 'transactional_properties'='default');

describe formatted t_acid_convert_16hex;

-- Original rows still visible after conversion.
select id, name from t_acid_convert_16hex order by id;

-- Sanity: ACID-only ops now work end-to-end.
update t_acid_convert_16hex set name = 'B' where id = 2;
delete from t_acid_convert_16hex where id = 1;
select id, name from t_acid_convert_16hex order by id;

drop table t_acid_convert_16hex;
