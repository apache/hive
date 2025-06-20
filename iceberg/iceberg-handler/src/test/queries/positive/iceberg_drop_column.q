-- SORT_QUERY_RESULTS
set hive.cli.print.header=true;

create external table ice_tbl (
  strcol string,
  intcol integer,
  pcol string,
  datecol date
) partitioned by spec (pcol, datecol)
stored by iceberg;

desc ice_tbl;

insert into ice_tbl values ('str', 1, 'xya', '2027-01-20');
insert into ice_tbl values ('str', 1, 'xyz', '2026-07-19');
insert into ice_tbl values ('str', 1, 'xyb', '2025-07-18');
insert into ice_tbl values ('str', 2, 'yzb', '2023-07-26');
insert into ice_tbl values ('str', 1, 'yab', '2023-07-26');
insert into ice_tbl values ('str', 1, 'yzb', '2023-07-26');
insert into ice_tbl values ('str', 2, 'xyz', '2026-07-19');
insert into ice_tbl values ('str', 1, 'abc', '2019-02-07');
insert into ice_tbl values ('str', 1, 'a"ab', '2019-02-07');
insert into ice_tbl values ('str', 1, "a'ab", '2019-02-07');

show partitions ice_tbl;

show columns in ice_tbl;

select * from ice_tbl;

explain alter table ice_tbl drop column intcol;

alter table ice_tbl drop column intcol;

show columns in ice_tbl;

select * from ice_tbl;

explain alter table ice_tbl drop column if exists intcol;

alter table ice_tbl drop column if exists intcol;

show columns in ice_tbl;

select * from ice_tbl;

alter table ice_tbl drop column if exists strcol;

show columns in ice_tbl;

select * from ice_tbl;
