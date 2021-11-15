-- SORT_QUERY_RESULTS

set hive.acid.direct.insert.enabled=true;
set hive.support.concurrency=true;
set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;
set hive.stats.autogather=true;

drop table if exists multiinsert_test_text;
drop table if exists multiinsert_test_text_2;
drop table if exists multiinsert_test_acid;
drop table if exists multiinsert_test_mm;
drop table if exists multiinsert_test_acid_nondi;

create external table multiinsert_test_text (a int, b int, c int) stored as textfile;

insert into multiinsert_test_text values (1111, 11, 1111), (2222, 22, 1111), (3333, 33, 2222), (4444, 44, NULL), (5555, 55, NULL);

create external table multiinsert_test_text_2 (a int, b int, c int) stored as textfile;

insert into multiinsert_test_text_2 values (1111, 11, 1111), (2222, 22, 1111), (3333, 33, 2222), (4444, 44, 4444), (5555, 55, 4444);

create table multiinsert_test_acid (a int, b int) partitioned by (c int) stored as orc tblproperties('transactional'='true');

create table multiinsert_test_mm (a int, b int) partitioned by (c int) stored as orc tblproperties('transactional'='true', 'transactional_properties'='insert_only');

create table multiinsert_test_acid_nondi (a int, b int) partitioned by (c int) stored as orc tblproperties('transactional'='true');

from multiinsert_test_text a
insert overwrite table multiinsert_test_acid partition (c)
select
 a.a,
 a.b,
 a.c
 where a.c is not null
insert overwrite table multiinsert_test_acid partition (c)
select
 a.a,
 a.b,
 a.c
where a.c is null
sort by a.c
;

select * from multiinsert_test_acid;

from multiinsert_test_text a
insert overwrite table multiinsert_test_mm partition (c)
select
 a.a,
 a.b,
 a.c
 where a.c is not null
insert overwrite table multiinsert_test_mm partition (c)
select
 a.a,
 a.b,
 a.c
where a.c is null
sort by a.c
;

select * from multiinsert_test_mm;

set hive.acid.direct.insert.enabled=false;

from multiinsert_test_text a
insert overwrite table multiinsert_test_acid_nondi partition (c)
select
 a.a,
 a.b,
 a.c
 where a.c is not null
insert overwrite table multiinsert_test_acid_nondi partition (c)
select
 a.a,
 a.b,
 a.c
where a.c is null
sort by a.c
;

select * from multiinsert_test_acid_nondi;

set hive.acid.direct.insert.enabled=true;

drop table if exists multiinsert_test_acid;
drop table if exists multiinsert_test_mm;
drop table if exists multiinsert_test_acid_nondi;

create table multiinsert_test_acid (a int, b int) partitioned by (c int) stored as orc tblproperties('transactional'='true');

create table multiinsert_test_mm (a int, b int) partitioned by (c int) stored as orc tblproperties('transactional'='true', 'transactional_properties'='insert_only');

create table multiinsert_test_acid_nondi (a int, b int) partitioned by (c int) stored as orc tblproperties('transactional'='true');

from multiinsert_test_text_2 a
insert overwrite table multiinsert_test_acid partition (c)
select
 a.a,
 a.b,
 a.c
 where a.c is not null
insert overwrite table multiinsert_test_acid partition (c)
select
 a.a,
 a.b,
 a.c
where a.c is null
sort by a.c
;

select * from multiinsert_test_acid;

from multiinsert_test_text_2 a
insert overwrite table multiinsert_test_mm partition (c)
select
 a.a,
 a.b,
 a.c
 where a.c is not null
insert overwrite table multiinsert_test_mm partition (c)
select
 a.a,
 a.b,
 a.c
where a.c is null
sort by a.c
;

select * from multiinsert_test_mm;

set hive.acid.direct.insert.enabled=false;

from multiinsert_test_text_2 a
insert overwrite table multiinsert_test_acid_nondi partition (c)
select
 a.a,
 a.b,
 a.c
 where a.c is not null
insert overwrite table multiinsert_test_acid_nondi partition (c)
select
 a.a,
 a.b,
 a.c
where a.c is null
sort by a.c
;

select * from multiinsert_test_acid_nondi;

set hive.acid.direct.insert.enabled=true;

drop table if exists multiinsert_test_acid;
drop table if exists multiinsert_test_mm;
drop table if exists multiinsert_test_acid_nondi;

create table multiinsert_test_acid (a int, b int) partitioned by (c int) stored as orc tblproperties('transactional'='true');

create table multiinsert_test_mm (a int, b int) partitioned by (c int) stored as orc tblproperties('transactional'='true', 'transactional_properties'='insert_only');

create table multiinsert_test_acid_nondi (a int, b int) partitioned by (c int) stored as orc tblproperties('transactional'='true');

from multiinsert_test_text a
insert into multiinsert_test_acid partition (c)
select
 a.a,
 a.b,
 a.c
 where a.c is not null
insert into multiinsert_test_acid partition (c)
select
 a.a,
 a.b,
 a.c
where a.c is null
sort by a.c
;

select * from multiinsert_test_acid;

from multiinsert_test_text a
insert into multiinsert_test_mm partition (c)
select
 a.a,
 a.b,
 a.c
 where a.c is not null
insert into multiinsert_test_mm partition (c)
select
 a.a,
 a.b,
 a.c
where a.c is null
sort by a.c
;

select * from multiinsert_test_mm;

set hive.acid.direct.insert.enabled=false;

from multiinsert_test_text a
insert into multiinsert_test_acid_nondi partition (c)
select
 a.a,
 a.b,
 a.c
 where a.c is not null
insert into multiinsert_test_acid_nondi partition (c)
select
 a.a,
 a.b,
 a.c
where a.c is null
sort by a.c
;

select * from multiinsert_test_acid_nondi;

drop table if exists multiinsert_test_text;
drop table if exists multiinsert_test_text_2;
drop table if exists multiinsert_test_acid;
drop table if exists multiinsert_test_mm;
drop table if exists multiinsert_test_acid_nondi;