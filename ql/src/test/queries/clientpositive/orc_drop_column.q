-- SORT_QUERY_RESULTS
set hive.cli.print.header=true;
set hive.exec.schema.evolution=false;

drop table if exists tbl_orc;

create external table tbl_orc(a int, b string, c int) partitioned by (d string) stored as orc;

desc formatted tbl_orc;

insert into table tbl_orc partition (d='one') values (1, "ab", 1), (2, "bc", 3), (3, "cd", 4);
insert into table tbl_orc partition (d='two') values (4, "bc", 6), (5, "cd", 5);
insert into table tbl_orc partition (d='three') values (6, "ab", 7), (7, "bc", 8), (8, "cd", 9);
insert into table tbl_orc partition (d='four') values (9, "ab", 10);

show partitions tbl_orc;

show columns in tbl_orc;

select * from tbl_orc;

explain alter table tbl_orc drop column if exists c cascade;

alter table tbl_orc drop column if exists c cascade;

alter table tbl_orc drop column if exists c cascade;

desc formatted tbl_orc;

desc formatted tbl_orc partition (d='one');

show columns in tbl_orc;

select * from tbl_orc;

create external table tbl_orc2(a int, b string) stored as orc;

desc formatted tbl_orc2;

insert into table tbl_orc2 values (1, "ab"), (2, "bc"), (3, "cd");

show columns in tbl_orc2;

select * from tbl_orc2;

explain alter table tbl_orc2 drop column b;

alter table tbl_orc2 drop column b;

desc formatted tbl_orc2;

show columns in tbl_orc2;

select * from tbl_orc2;
