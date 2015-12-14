set hive.mapred.mode=nonstrict;
drop table if exists udf_tb1;
drop table if exists udf_tb2;

create table udf_tb1 (year int, month int);
create table udf_tb2(month int);
insert into udf_tb1 values(2001, 11);
insert into udf_tb2 values(11);

explain
select unix_timestamp(concat(a.year, '-01-01 00:00:00')) from (select * from udf_tb1 where year=2001) a join udf_tb2 b on (a.month=b.month);
select unix_timestamp(concat(a.year, '-01-01 00:00:00')) from (select * from udf_tb1 where year=2001) a join udf_tb2 b on (a.month=b.month);
