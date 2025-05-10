-- SORT_QUERY_RESULTS
set hive.cli.print.header=true;
set hive.exec.schema.evolution=false;

drop table if exists tbl_orc;

create external table tbl_orc(a int, b string, c int) partitioned by (d string) stored as orc;

show columns in tbl_orc;

alter table tbl_orc drop column c cascade;

alter table tbl_orc drop column c cascade;
