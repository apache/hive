-- SORT_QUERY_RESULTS
-- Mask the totalSize value as it can have slight variability, causing test flakiness
--! qt:replace:/(\s+totalSize\s+)\S+(\s+)/$1#Masked#$2/
-- Mask random uuid
--! qt:replace:/(\s+uuid\s+)\S+(\s*)/$1#Masked#$2/
-- Mask a random snapshot id
--! qt:replace:/(\s+current-snapshot-id\s+)\S+(\s*)/$1#Masked#/
-- Mask added file size
--! qt:replace:/(\S\"added-files-size\\\":\\\")(\d+)(\\\")/$1#Masked#$3/
-- Mask total file size
--! qt:replace:/(\S\"total-files-size\\\":\\\")(\d+)(\\\")/$1#Masked#$3/
-- Mask current-snapshot-timestamp-ms
--! qt:replace:/(\s+current-snapshot-timestamp-ms\s+)\S+(\s*)/$1#Masked#$2/
-- Mask removed file size
--! qt:replace:/(\S\"removed-files-size\\\":\\\")(\d+)(\\\")/$1#Masked#$3/
-- Mask number of files
--! qt:replace:/(\s+numFiles\s+)\S+(\s+)/$1#Masked#$2/
-- Mask total data files
--! qt:replace:/(\S\"total-data-files\\\":\\\")(\d+)(\\\")/$1#Masked#$3/
set hive.explain.user=false;
create external table ice_parquet_int(
  strcol string,
  intcol integer
) partitioned by (pcol int)
stored by iceberg;

explain insert into table ice_parquet_int partition(pcol = 1) values ('ABC', 1), ('DEF', 2);
insert into table ice_parquet_int partition(pcol = 1) values ('ABC', 1), ('DEF', 2);
explain insert into table ice_parquet_int partition(pCOL = 2) values ('ABC', 1), ('DEF', 2);
insert into table ice_parquet_int partition(pcol = 2) values ('ABC', 1), ('DEF', 2);
explain insert into table ice_parquet_int partition(pcol = 3) select strcol, intcol from ice_parquet_int where pcol = 2;
insert into table ice_parquet_int partition(pcol = 3) select strcol, intcol from ice_parquet_int where pcol = 2;
explain insert into table ice_parquet_int partition(pcol = 04) select strcol, intcol from ice_parquet_int where pcol = 2;
insert into table ice_parquet_int partition(pcol = 04) select strcol, intcol from ice_parquet_int where pcol = 2;

describe formatted ice_parquet_int;
select * from ice_parquet_int;

create table ice_parquet_string (name string, age int) partitioned by (country string, state string) stored by iceberg;
explain insert into ice_parquet_string partition (state='CA', country='USA') values ('John Doe', 23), ('Jane Doe', 22);
insert into ice_parquet_string partition (state='CA', country='USA') values ('John Doe', 23), ('Jane Doe', 22);
explain insert into ice_parquet_string partition (country='USA', state='CA') values ('Mark Cage', 38), ('Mirna Cage', 37);
insert into ice_parquet_string partition (country='USA', state='CA') values ('Mark Cage', 38), ('Mirna Cage', 37);
explain insert into ice_parquet_string partition (country='USA', state='TX') values ('Bill Rose', 52), ('Maria Full', 50);
insert into ice_parquet_string partition (country='USA', state='TX') values ('Bill Rose', 52), ('Maria Full', 50);

select * from ice_parquet_string;

explain insert into table ice_parquet_string partition(country, state) select * from ice_parquet_string;
insert into table ice_parquet_string partition(country, state) select * from ice_parquet_string;
explain insert into table ice_parquet_string partition(country='USA', state) select name, age, state from ice_parquet_string;
insert into table ice_parquet_string partition(country='USA', state) select name, age, state from ice_parquet_string;
explain insert into table ice_parquet_string partition(state='CA', country) select name, age, country from ice_parquet_string;
insert into table ice_parquet_string partition(state='CA', country) select name, age, country from ice_parquet_string;
explain insert into table ice_parquet_string partition(state='TX') select name, age, country from ice_parquet_string;
insert into table ice_parquet_string partition(state='TX') select name, age, country from ice_parquet_string;
explain insert into table ice_parquet_string partition(country='India') select name, age, state from ice_parquet_string;
insert into table ice_parquet_string partition(country='India') select name, age, state from ice_parquet_string;
explain insert into table ice_parquet_string partition(country='India') select name, '0054', state from ice_parquet_string;
insert into table ice_parquet_string partition(country='India') select name, '0054', state from ice_parquet_string;

describe formatted ice_parquet_string;
select * from ice_parquet_string;

create external table ice_parquet_date(
  bigintcol bigint,
  intcol integer
) partitioned by (pcol date)
stored by iceberg;

explain insert into table ice_parquet_date partition (pcol = '1999-12-31') values (1234567890123345, 2), (23456789012345678, 4);
insert into table ice_parquet_date partition (pcol = '1999-12-31') values (1234567890123345, 2), (23456789012345678, 4);
explain insert into table ice_parquet_date partition (pcol = '1999-12-26') values (1234567890123345, 3), (23456789012345678, 5);
insert into table ice_parquet_date partition (pcol = '1999-12-26') values (1234567890123345, 3), (23456789012345678, 5);
explain insert into table ice_parquet_date partition (pcol = '1999-12-12') values (3456789012345678, 4), (34567890123456789, 6);
insert into table ice_parquet_date partition (pcol = '1999-12-12') values (3456789012345678, 4), (34567890123456789, 6);

select * from ice_parquet_date;

explain insert into table ice_parquet_date partition (pcol = '1999-12-13') select bigintcol, intcol from ice_parquet_date;
insert into table ice_parquet_date partition (pcol = '1999-12-13') select bigintcol, intcol from ice_parquet_date;
explain insert into table ice_parquet_date partition (pcol = '1999-12-02') select 234675894076895090, intcol from ice_parquet_date;
insert into table ice_parquet_date partition (pcol = '1999-12-02') select 234675894076895090, intcol from ice_parquet_date;
describe formatted ice_parquet_string;
select * from ice_parquet_date;

create external table ice_parquet_bigint(
  datecol date,
  intcol integer
) partitioned by (pcol bigint)
stored by iceberg;

explain insert into table ice_parquet_bigint partition (pcol = 34567890123456787) values ('2022-08-07', 2), ('2022-08-09', 4);
insert into table ice_parquet_bigint partition (pcol = 34567890123456787) values ('2022-08-07', 2), ('2022-08-09', 4);
explain insert into table ice_parquet_bigint partition (pcol = 12346577399277578) values ('2022-08-16', 3), ('2022-07-09', 5);
insert into table ice_parquet_bigint partition (pcol = 12346577399277578) values ('2022-08-16', 3), ('2022-07-09', 5);
explain insert into table ice_parquet_bigint partition (pcol = 45637829068876994) values ('2022-07-21', 4), ('2022-05-29', 6);
insert into table ice_parquet_bigint partition (pcol = 45637829068876994) values ('2022-07-21', 4), ('2022-05-29', 6);

select * from ice_parquet_bigint;

explain insert into table ice_parquet_bigint partition (pcol = 45637829068876994) select datecol, intcol from ice_parquet_bigint;
insert into table ice_parquet_bigint partition (pcol = 45637829068876994) select datecol, intcol from ice_parquet_bigint;
explain insert into table ice_parquet_bigint partition (pcol = 12346577399277578) select '2022-01-25', intcol from ice_parquet_bigint;
insert into table ice_parquet_bigint partition (pcol = 12346577399277578) select '2022-01-25', intcol from ice_parquet_bigint;

describe formatted ice_parquet_bigint;
select * from ice_parquet_bigint;

create external table ice_parquet_double(
  datecol date,
  intcol integer
) partitioned by (pcol double)
stored by iceberg;

explain insert into table ice_parquet_double partition (pcol = 3.14786) values ('2022-08-07', 2), ('2022-08-09', 4);
insert into table ice_parquet_double partition (pcol = 3.14786) values ('2022-08-07', 2), ('2022-08-09', 4);
explain insert into table ice_parquet_double partition (pcol = 3.189) values ('2022-08-16', 3), ('2022-07-09', 5);
insert into table ice_parquet_double partition (pcol = 3.189) values ('2022-08-16', 3), ('2022-07-09', 5);
explain insert into table ice_parquet_double partition (pcol = 45.789) values ('2022-07-21', 4), ('2022-05-29', 6);
insert into table ice_parquet_double partition (pcol = 45.789) values ('2022-07-21', 4), ('2022-05-29', 6);

select * from ice_parquet_double;

explain insert into table ice_parquet_double partition (pcol = 3.14786) select datecol, intcol from ice_parquet_double;
insert into table ice_parquet_double partition (pcol = 3.14786) select datecol, intcol from ice_parquet_double;
explain insert into table ice_parquet_double partition (pcol = 3.189) select '2022-01-25', intcol from ice_parquet_double;
insert into table ice_parquet_double partition (pcol = 3.189) select '2022-01-25', intcol from ice_parquet_double;

describe formatted ice_parquet_double;
select * from ice_parquet_double;

create external table ice_parquet_decimal(
  datecol date,
  intcol integer
) partitioned by (pcol decimal(10,6))
stored by iceberg;

explain insert into table ice_parquet_decimal partition (pcol = 3.14786) values ('2022-08-07', 2), ('2022-08-09', 4);
insert into table ice_parquet_decimal partition (pcol = 3.14786) values ('2022-08-07', 2), ('2022-08-09', 4);
explain insert into table ice_parquet_decimal partition (pcol = 3.189) values ('2022-08-16', 3), ('2022-07-09', 5);
insert into table ice_parquet_decimal partition (pcol = 3.189) values ('2022-08-16', 3), ('2022-07-09', 5);
explain insert into table ice_parquet_decimal partition (pcol = 45.789) values ('2022-07-21', 4), ('2022-05-29', 6);
insert into table ice_parquet_decimal partition (pcol = 45.789) values ('2022-07-21', 4), ('2022-05-29', 6);

select * from ice_parquet_decimal;

explain insert into table ice_parquet_decimal partition (pcol = 3.14786) select datecol, intcol from ice_parquet_decimal;
insert into table ice_parquet_decimal partition (pcol = 3.14786) select datecol, intcol from ice_parquet_decimal;
explain insert into table ice_parquet_decimal partition (pcol = 3.189) select '2022-01-25', intcol from ice_parquet_decimal;
insert into table ice_parquet_decimal partition (pcol = 3.189) select '2022-01-25', intcol from ice_parquet_decimal;

describe formatted ice_parquet_decimal;
select * from ice_parquet_decimal;

drop table ice_parquet_int;
drop table ice_parquet_bigint;
drop table ice_parquet_string;
drop table ice_parquet_date;
drop table ice_parquet_decimal;
drop table ice_parquet_double;
