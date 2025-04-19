-- SORT_QUERY_RESULTS
set hive.cli.print.header=true;
set hive.exec.schema.evolution=true;

drop table if exists tbl_parq;

create external table tbl_parq(a int, b string, c int) partitioned by (d string) stored as parquet;

show columns in tbl_parq;

alter table tbl_parq drop column if exists c;
