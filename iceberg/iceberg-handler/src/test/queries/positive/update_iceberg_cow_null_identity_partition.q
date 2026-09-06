-- SORT_QUERY_RESULTS

drop table if exists iceberg_cow_partitioned;

create external table iceberg_cow_partitioned (
  index int,
  string_col string,
  boolean_col boolean,
  str_col string,
  tinyint_col int
) partitioned by spec(str_col, tinyint_col)
stored by iceberg
tblproperties ('write.update.mode'='copy-on-write');

insert into iceberg_cow_partitioned partition (str_col, tinyint_col)
  values (1, 'a', true, null, 0);

explain update iceberg_cow_partitioned set str_col = 'UPDATED NULLS' where str_col is null;
update iceberg_cow_partitioned set str_col = 'UPDATED NULLS' where str_col is null;

select * from iceberg_cow_partitioned;

-- Disable vectorization

set hive.vectorized.execution.enabled=false;

insert into iceberg_cow_partitioned partition (str_col, tinyint_col)
  values (2, 'b', false, null, 1);

explain update iceberg_cow_partitioned set str_col = 'UPDATED NULLS' where str_col is null;
update iceberg_cow_partitioned set str_col = 'UPDATED NULLS' where str_col is null;

select * from iceberg_cow_partitioned;
