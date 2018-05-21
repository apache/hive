--! qt:dataset:alltypesorc
CREATE TABLE druid_alltypesorc
STORED BY 'org.apache.hadoop.hive.druid.DruidStorageHandler'
TBLPROPERTIES ("druid.segment.granularity" = "HOUR", "druid.query.granularity" = "MINUTE")
AS
SELECT cast (`ctimestamp2` as timestamp with local time zone) as `__time`,
  cstring1,
  cstring2,
  cdouble,
  cfloat,
  ctinyint,
  csmallint,
  cint,
  cbigint,
  cboolean1,
  cboolean2
  FROM alltypesorc where ctimestamp2 IS NOT NULL;

SELECT COUNT(*) FROM druid_alltypesorc;

INSERT INTO TABLE druid_alltypesorc
SELECT cast (`ctimestamp1` as timestamp with local time zone) as `__time`,
  cstring1,
  cstring2,
  cdouble,
  cfloat,
  ctinyint,
  csmallint,
  cint,
  cbigint,
  cboolean1,
  cboolean2
  FROM alltypesorc where ctimestamp1 IS NOT NULL;


SELECT COUNT(*) FROM druid_alltypesorc;

INSERT OVERWRITE TABLE druid_alltypesorc
SELECT cast (`ctimestamp1` as timestamp with local time zone) as `__time`,
  cstring1,
  cstring2,
  cdouble,
  cfloat,
  ctinyint,
  csmallint,
  cint,
  cbigint,
  cboolean1,
  cboolean2
  FROM alltypesorc where ctimestamp1 IS NOT NULL;

SELECT COUNT(*) FROM druid_alltypesorc;

DROP TABLE druid_alltypesorc;

 -- Test create then insert
 
 create database druid_test_create_then_insert;
 use druid_test_create_then_insert;
 
 create table test_table_n9(`timecolumn` timestamp, `userid` string, `num_l` float);
 
 insert into test_table_n9 values ('2015-01-08 00:00:00', 'i1-start', 4);
 insert into test_table_n9 values ('2015-01-08 23:59:59', 'i1-end', 1);
 
 CREATE TABLE druid_table_n1 (`__time` timestamp with local time zone, `userid` string, `num_l` float)
 STORED BY 'org.apache.hadoop.hive.druid.DruidStorageHandler'
 TBLPROPERTIES ("druid.segment.granularity" = "DAY");
 
 
 INSERT INTO TABLE druid_table_n1
 select cast(`timecolumn` as timestamp with local time zone) as `__time`, `userid`, `num_l` FROM test_table_n9;
 
 select count(*) FROM druid_table_n1;
 
 DROP TABLE  test_table_n9;
 DROP TABLE druid_table_n1;
 DROP DATABASE druid_test_create_then_insert;
 
-- Day light saving time test insert into test

create database druid_test_dst;
use druid_test_dst;

create table test_base_table(`timecolumn` timestamp, `userid` string, `num_l` float);
insert into test_base_table values ('2015-03-08 00:00:00', 'i1-start', 4);
insert into test_base_table values ('2015-03-08 23:59:59', 'i1-end', 1);
insert into test_base_table values ('2015-03-09 00:00:00', 'i2-start', 4);
insert into test_base_table values ('2015-03-09 23:59:59', 'i2-end', 1);
insert into test_base_table values ('2015-03-10 00:00:00', 'i3-start', 2);
insert into test_base_table values ('2015-03-10 23:59:59', 'i3-end', 2);

CREATE TABLE druid_test_table_n9
STORED BY 'org.apache.hadoop.hive.druid.DruidStorageHandler'
TBLPROPERTIES ("druid.segment.granularity" = "DAY")
AS
select cast(`timecolumn` as timestamp with local time zone) as `__time`, `userid`, `num_l` FROM test_base_table;

select * FROM druid_test_table_n9;

select * from druid_test_table_n9 where `__time` = cast('2015-03-08 00:00:00' as timestamp with local time zone);
select * from druid_test_table_n9 where `__time` = cast('2015-03-08 23:59:59' as timestamp with local time zone);

select * from druid_test_table_n9 where `__time` = cast('2015-03-09 00:00:00' as timestamp with local time zone);
select * from druid_test_table_n9 where `__time` = cast('2015-03-09 23:59:59' as timestamp with local time zone);

select * from druid_test_table_n9 where `__time` = cast('2015-03-10 00:00:00' as timestamp with local time zone);
select * from druid_test_table_n9 where `__time` = cast('2015-03-10 23:59:59' as timestamp with local time zone);


explain select * from druid_test_table_n9 where `__time` = cast('2015-03-08 00:00:00' as timestamp with local time zone);
explain select * from druid_test_table_n9 where `__time` = cast('2015-03-08 23:59:59' as timestamp with local time zone);

explain select * from druid_test_table_n9 where `__time` = cast('2015-03-09 00:00:00' as timestamp with local time zone);
explain select * from druid_test_table_n9 where `__time` = cast('2015-03-09 23:59:59' as timestamp with local time zone);

explain select * from druid_test_table_n9 where `__time` = cast('2015-03-10 00:00:00' as timestamp with local time zone);
explain select * from druid_test_table_n9 where `__time` = cast('2015-03-10 23:59:59' as timestamp with local time zone);


select * from druid_test_table_n9 where `__time` = cast('2015-03-08 00:00:00' as timestamp );
select * from druid_test_table_n9 where `__time` = cast('2015-03-08 23:59:59' as timestamp );

select * from druid_test_table_n9 where `__time` = cast('2015-03-09 00:00:00' as timestamp );
select * from druid_test_table_n9 where `__time` = cast('2015-03-09 23:59:59' as timestamp );

select * from druid_test_table_n9 where `__time` = cast('2015-03-10 00:00:00' as timestamp );
select * from druid_test_table_n9 where `__time` = cast('2015-03-10 23:59:59' as timestamp );


EXPLAIN select * from druid_test_table_n9 where `__time` = cast('2015-03-08 00:00:00' as timestamp );
EXPLAIN select * from druid_test_table_n9 where `__time` = cast('2015-03-08 23:59:59' as timestamp );

EXPLAIN select * from druid_test_table_n9 where `__time` = cast('2015-03-09 00:00:00' as timestamp );
EXPLAIN select * from druid_test_table_n9 where `__time` = cast('2015-03-09 23:59:59' as timestamp );

EXPLAIN select * from druid_test_table_n9 where `__time` = cast('2015-03-10 00:00:00' as timestamp );
EXPLAIN select * from druid_test_table_n9 where `__time` = cast('2015-03-10 23:59:59' as timestamp );

DROP TABLE test_base_table;
DROP TABLE druid_test_table_n9;

drop   database druid_test_dst;
