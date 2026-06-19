create database t3;
use t3;

create table vector1k(
        t int,
        si int,
        i int,
        b bigint,
        f float,
        d double,
        dc decimal(38,18),
        bo boolean,
        s string,
        s2 string,
        ts timestamp,
        ts2 timestamp,
        dt date)
     row format delimited fields terminated by ',';

load data local inpath "../../data/files/query-hive-28087.csv" OVERWRITE into table vector1k;

create table vectortab10k(
        t int,
        si int,
        i int,
        b bigint,
        f float,
        d double,
        dc decimal(38,18),
        bo boolean,
        s string,
        s2 string,
        ts timestamp,
        ts2 timestamp,
        dt date)
    stored by iceberg
    stored as orc;

insert into vectortab10k select * from vector1k;
select count(*) from vectortab10k ;

create table partition_transform_year(t int, ts timestamp) partitioned by spec(year(ts)) stored by iceberg;
insert into table partition_transform_year select t, ts from vectortab10k;

create table partition_transform_month(t int, ts timestamp) partitioned by spec(month(ts)) stored by iceberg;
insert into table partition_transform_month select t, ts from vectortab10k;

create table partition_transform_day(t int, ts timestamp) partitioned by spec(day(ts)) stored by iceberg;
insert into table partition_transform_day select t, ts from vectortab10k;

create table partition_transform_hour(t int, ts timestamp) partitioned by spec(hour(ts)) stored by iceberg;
insert into table partition_transform_hour select t, ts from vectortab10k;

drop table partition_transform_month;
drop table partition_transform_year;
drop table partition_transform_day;
drop table partition_transform_hour;
