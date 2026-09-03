-- Repro for PPD/PCR bug: filtered SELECT returns 0 after COW UPDATE changes an identity
-- partition value. UPDATE uses a data-column predicate (not str_col IS NULL) so this
-- does not depend on HIVE-29819.

drop table if exists iceberg_select_ppd_repro;

create external table iceberg_select_ppd_repro (
  index int,
  string_col string,
  boolean_col boolean,
  str_col string,
  tinyint_col int
) partitioned by spec(str_col, tinyint_col)
stored by iceberg
tblproperties ('write.update.mode'='copy-on-write');

insert into iceberg_select_ppd_repro partition (str_col, tinyint_col)
  values (1, 'a', true, null, 0),
  (2, 'b', false, null, 1);

-- Avoid "where str_col is null" so UPDATE works without HIVE-29819
update iceberg_select_ppd_repro set str_col = 'UPDATEDNULLS' where index = 1;

select * from iceberg_select_ppd_repro;

explain select * from iceberg_select_ppd_repro where str_col = 'UPDATEDNULLS';
select * from iceberg_select_ppd_repro where str_col = 'UPDATEDNULLS';

select count(*) from iceberg_select_ppd_repro where str_col = 'UPDATEDNULLS';

select count(*) from iceberg_select_ppd_repro where tinyint_col = 1;


update iceberg_select_ppd_repro set tinyint_col = 2 where index = 2;

select * from iceberg_select_ppd_repro;

explain select * from iceberg_select_ppd_repro where tinyint_col = 2;
select * from iceberg_select_ppd_repro where tinyint_col = 2;

select count(*) from iceberg_select_ppd_repro where tinyint_col = 2;
