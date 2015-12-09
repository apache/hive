set hive.mapred.mode=nonstrict;
SET hive.vectorized.execution.enabled=true;

SET hive.llap.io.enabled=false;

SET hive.exec.orc.default.buffer.size=32768;
SET hive.exec.orc.default.row.index.stride=1000;
SET hive.optimize.index.filter=true;
set hive.auto.convert.join=false;

DROP TABLE cross_numbers;
DROP TABLE orc_llap;
DROP TABLE orc_llap_small;

CREATE TABLE orc_llap(
    ctinyint TINYINT,
    csmallint SMALLINT,
    cint INT,
    cbigint BIGINT,
    cfloat FLOAT,
    cdouble DOUBLE,
    cstring1 STRING,
    cstring2 STRING,
    ctimestamp1 TIMESTAMP,
    ctimestamp2 TIMESTAMP,
    cboolean1 BOOLEAN,
    cboolean2 BOOLEAN)
    STORED AS ORC;

CREATE TABLE orc_llap_small(
    ctinyint TINYINT,
    csmallint SMALLINT,
    cint INT)
    STORED AS ORC;


create table cross_numbers(i int);

insert into table cross_numbers
select distinct csmallint
from alltypesorc where csmallint > 0 order by csmallint limit 10;

insert into table orc_llap
select ctinyint + i, csmallint + i, cint + i, cbigint + i, cfloat + i, cdouble + i, cstring1, cstring2, ctimestamp1, ctimestamp2, cboolean1, cboolean2
from alltypesorc cross join cross_numbers;

insert into table orc_llap_small
select ctinyint, csmallint, cint from alltypesorc where ctinyint is not null and cint is not null limit 15;


SET hive.llap.io.enabled=true;
set hive.auto.convert.join=true;

-- Cross join with no projection - do it on small table
explain
select count(1) from orc_llap_small y join orc_llap_small x;
select count(1) from orc_llap_small y join orc_llap_small x;

-- All row groups selected, no projection
select count(*) from orc_llap_small;

-- All row groups pruned
select count(*) from orc_llap_small where cint < 60000000;

-- Hash cannot be vectorized, so run hash as the last step on a temp table
drop table llap_temp_table;
explain
select cint, csmallint, cbigint from orc_llap where cint > 10 and cbigint is not null;
create table llap_temp_table as
select cint, csmallint, cbigint from orc_llap where cint > 10 and cbigint is not null;
select sum(hash(*)) from llap_temp_table;

drop table llap_temp_table;
explain
select * from orc_llap where cint > 10 and cbigint is not null;
create table llap_temp_table as
select * from orc_llap where cint > 10 and cbigint is not null;
select sum(hash(*)) from llap_temp_table;

drop table llap_temp_table;
explain
select cstring2 from orc_llap where cint > 5 and cint < 10;
create table llap_temp_table as
select cstring2 from orc_llap where cint > 5 and cint < 10;
select sum(hash(*)) from llap_temp_table;


drop table llap_temp_table;
explain
select cstring1, cstring2, count(*) from orc_llap group by cstring1, cstring2;
create table llap_temp_table as
select cstring1, cstring2, count(*) from orc_llap group by cstring1, cstring2;
select sum(hash(*)) from llap_temp_table;

drop table llap_temp_table;
explain
select o1.cstring1, o2.cstring2 from orc_llap o1 inner join orc_llap o2 on o1.csmallint = o2.csmallint where o1.cbigint is not null and o2.cbigint is not null;
create table llap_temp_table as
select o1.cstring1, o2.cstring2 from orc_llap o1 inner join orc_llap o2 on o1.csmallint = o2.csmallint where o1.cbigint is not null and o2.cbigint is not null;
select sum(hash(*)) from llap_temp_table;

-- multi-stripe test
insert into table orc_llap
select ctinyint + i, csmallint + i, cint + i, cbigint + i, cfloat + i, cdouble + i, cstring1, cstring2, ctimestamp1, ctimestamp2, cboolean1, cboolean2
from alltypesorc cross join cross_numbers;

alter table orc_llap concatenate;

drop table llap_temp_table;
explain
select cint, csmallint, cbigint from orc_llap where cint > 10 and cbigint is not null;
create table llap_temp_table as
select cint, csmallint, cbigint from orc_llap where cint > 10 and cbigint is not null;
select sum(hash(*)) from llap_temp_table;

drop table llap_temp_table;
explain
select * from orc_llap where cint > 10 and cbigint is not null;
create table llap_temp_table as
select * from orc_llap where cint > 10 and cbigint is not null;
select sum(hash(*)) from llap_temp_table;

drop table llap_temp_table;
explain
select cstring2 from orc_llap where cint > 5 and cint < 10;
create table llap_temp_table as
select cstring2 from orc_llap where cint > 5 and cint < 10;
select sum(hash(*)) from llap_temp_table;

drop table llap_temp_table;
explain
select cstring1, cstring2, count(*) from orc_llap group by cstring1, cstring2;
create table llap_temp_table as
select cstring1, cstring2, count(*) from orc_llap group by cstring1, cstring2;
select sum(hash(*)) from llap_temp_table;

drop table llap_temp_table;
explain
select o1.cstring1, o2.cstring2 from orc_llap o1 inner join orc_llap o2 on o1.csmallint = o2.csmallint where o1.cbigint is not null and o2.cbigint is not null;
create table llap_temp_table as
select o1.cstring1, o2.cstring2 from orc_llap o1 inner join orc_llap o2 on o1.csmallint = o2.csmallint where o1.cbigint is not null and o2.cbigint is not null;
select sum(hash(*)) from llap_temp_table;

drop table llap_temp_table;


DROP TABLE cross_numbers;
DROP TABLE orc_llap;
DROP TABLE orc_llap_small;
