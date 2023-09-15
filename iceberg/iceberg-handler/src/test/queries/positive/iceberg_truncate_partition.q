-- SORT_QUERY_RESULTS
create external table ice_parquet_str_date (
  strcol string,
  intcol integer,
  pcol string,
  datecol date
) partitioned by spec (pcol, datecol)
stored by iceberg;

insert into ice_parquet_str_date values ('str', 1, 'xya', '2027-01-20');
insert into ice_parquet_str_date values ('str', 1, 'xyz', '2026-07-19');
insert into ice_parquet_str_date values ('str', 1, 'xyb', '2025-07-18');
insert into ice_parquet_str_date values ('str', 1, 'yzb', '2022-02-07');
insert into ice_parquet_str_date values ('str', 1, 'yab', '2023-07-26');
insert into ice_parquet_str_date values ('str', 1, 'yzb', '2023-07-26');
insert into ice_parquet_str_date values ('str', 1, 'xyz', '2022-02-07');
insert into ice_parquet_str_date values ('str', 1, 'abc', '2019-02-07');
insert into ice_parquet_str_date values ('str', 1, 'a"ab', '2019-02-07');
insert into ice_parquet_str_date values ('str', 1, "a'ab", '2019-02-07');

select `partition` from default.ice_parquet_str_date.partitions;
truncate table ice_parquet_str_date partition (datecol = '2022-02-07');
select `partition` from default.ice_parquet_str_date.partitions;
truncate table ice_parquet_str_date partition (pcol = 'yzb', datecol = '2023-07-26');
select `partition` from default.ice_parquet_str_date.partitions;
truncate table ice_parquet_str_date partition (pcol = 'xyz');
truncate table ice_parquet_str_date partition (pcol = 'a"ab');
truncate table ice_parquet_str_date partition (pcol = "a'ab");

select * from ice_parquet_str_date;

create external table ice_parquet_str_int (
  strcol string,
  intcol integer,
  pcol integer
) partitioned by spec (strcol, pcol)
stored by iceberg;

insert into ice_parquet_str_int values ('str', 1, 14);
insert into ice_parquet_str_int values ('str', 1, 16);
insert into ice_parquet_str_int values ('str', 1, 15);
insert into ice_parquet_str_int values ('str', 1, 17);
insert into ice_parquet_str_int values ('abc', 1, 18);
insert into ice_parquet_str_int values ('def', 1, 18);

select * from ice_parquet_str_int;
select `partition` from default.ice_parquet_str_int.partitions;
truncate table ice_parquet_str_int partition (pcol = 18);
select `partition` from default.ice_parquet_str_int.partitions;

create external table ice_parquet_int (
  strcol string,
  intcol integer,
  pcol integer
) partitioned by spec (pcol)
stored by iceberg;

insert into ice_parquet_int values ('str', 1, 14);
insert into ice_parquet_int values ('str', 1, 14);
insert into ice_parquet_int values ('str', 1, 14);
insert into ice_parquet_int values ('str', 1, 15);
insert into ice_parquet_int values ('abc', 1, 15);
insert into ice_parquet_int values ('def', 1, 15);

select * from ice_parquet_int;
select `partition` from default.ice_parquet_int.partitions;
truncate table ice_parquet_int partition (pcol = 14);
select `partition` from default.ice_parquet_int.partitions;

create external table ice_parquet_bigint (
  strcol string,
  intcol integer,
  pcol bigint
) partitioned by spec (pcol)
stored by iceberg;

insert into ice_parquet_bigint values ('str', 1, 144565734992765839);
insert into ice_parquet_bigint values ('str', 1, 144565734992765839);
insert into ice_parquet_bigint values ('str', 1, 144565734992765839);
insert into ice_parquet_bigint values ('str', 1, 144565734992765839);
insert into ice_parquet_bigint values ('abc', 1, 2345578903479709);
insert into ice_parquet_bigint values ('def', 1, 2345578903479709);

select * from ice_parquet_bigint;
select `partition` from default.ice_parquet_bigint.partitions;
truncate table ice_parquet_bigint partition (pcol = 2345578903479709);
select `partition` from default.ice_parquet_bigint.partitions;

create external table ice_parquet_str (
  strcol string,
  intcol integer,
  pcol string
) partitioned by spec (pcol)
stored by iceberg;

insert into ice_parquet_str values ('str', 1, 'ghutihkklfng');
insert into ice_parquet_str values ('str', 1, 'ghutihkklfng');
insert into ice_parquet_str values ('str', 1, 'iyoorpiyujn');
insert into ice_parquet_str values ('str', 1, 'iyoorpiyujn');
insert into ice_parquet_str values ('abc', 1, 'ghutihkklfng');
insert into ice_parquet_str values ('def', 1, 'iyoorpiyujn');

select * from ice_parquet_str;
select `partition` from default.ice_parquet_str.partitions;
truncate table ice_parquet_str partition (pcol = 'ghutihkklfng');
select `partition` from default.ice_parquet_str.partitions;

create external table ice_parquet_date (
  strcol string,
  intcol integer,
  pcol date
) partitioned by spec (pcol)
stored by iceberg;

insert into ice_parquet_date values ('str', 1, '2022-01-01');
insert into ice_parquet_date values ('str', 1, '2022-02-02');
insert into ice_parquet_date values ('str', 1, '2022-03-03');
insert into ice_parquet_date values ('str', 1, '2022-01-01');
insert into ice_parquet_date values ('abc', 1, '2022-02-02');
insert into ice_parquet_date values ('def', 1, '2022-03-03');

select * from ice_parquet_date;
select `partition` from default.ice_parquet_date.partitions;
truncate table ice_parquet_date partition (pcol = '2022-02-02');
select `partition` from default.ice_parquet_date.partitions;

create external table ice_parquet_double (
  strcol string,
  intcol integer,
  pcol double
) partitioned by spec (pcol)
stored by iceberg;

insert into ice_parquet_double values ('str', 1, '2.45');
insert into ice_parquet_double values ('str', 1, '3.1567');
insert into ice_parquet_double values ('str', 1, '2.45');
insert into ice_parquet_double values ('str', 1, '3.1567');
insert into ice_parquet_double values ('abc', 1, '2.45');
insert into ice_parquet_double values ('def', 1, '3.1567');

select * from ice_parquet_double;
select `partition` from default.ice_parquet_double.partitions;
truncate table ice_parquet_double partition (pcol = '2.45');
select `partition` from default.ice_parquet_double.partitions;

drop table ice_parquet_str_date;
drop table ice_parquet_str_int;
drop table ice_parquet_int;
drop table ice_parquet_bigint;
drop table ice_parquet_str;
drop table ice_parquet_date;
drop table ice_parquet_double;
