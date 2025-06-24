-- SORT_QUERY_RESULTS
set hive.cli.print.header=true;
set hive.exec.schema.evolution=true;

drop table if exists tbl_parq;

create external table tbl_parq(a int, b string, c int) partitioned by (d string) stored as parquet;

desc formatted tbl_parq;

insert into table tbl_parq partition (d='one') values (1, "ab", 1), (2, "bc", 3), (3, "cd", 4);
insert into table tbl_parq partition (d='two') values (4, "bc", 6), (5, "cd", 5);
insert into table tbl_parq partition (d='three') values (6, "ab", 7), (7, "bc", 8), (8, "cd", 9);
insert into table tbl_parq partition (d='four') values (9, "ab", 10);

show partitions tbl_parq;

show columns in tbl_parq;

select * from tbl_parq;

explain alter table tbl_parq drop column if exists c cascade;

alter table tbl_parq drop column if exists c cascade;

alter table tbl_parq drop column if exists c cascade;

desc formatted tbl_parq;

desc formatted tbl_parq partition (d='one');

show columns in tbl_parq;

select * from tbl_parq;

create external table tbl_parq2(a int, b string) stored as parquet;

desc formatted tbl_parq2;

insert into table tbl_parq2 values (1, "ab"), (2, "bc"), (3, "cd");

show columns in tbl_parq2;

select * from tbl_parq2;

explain alter table tbl_parq2 drop column b;

alter table tbl_parq2 drop column b;

desc formatted tbl_parq2;

show columns in tbl_parq2;

select * from tbl_parq2;
