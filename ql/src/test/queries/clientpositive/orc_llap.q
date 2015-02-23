SET hive.vectorized.execution.enabled=true;

SET hive.llap.io.enabled=false;

SET hive.exec.orc.default.buffer.size=32768;
SET hive.exec.orc.default.row.index.stride=1000;
SET hive.optimize.index.filter=true;

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

create table cross_numbers(i int);

insert into table cross_numbers
select distinct csmallint
from alltypesorc where csmallint > 0 order by csmallint limit 10;

insert into table orc_llap
select ctinyint + i, csmallint + i, cint + i, cbigint + i, cfloat + i, cdouble + i, cstring1, cstring2, ctimestamp1, ctimestamp2, cboolean1, cboolean2
from alltypesorc cross join cross_numbers;

select count(*) from orc_llap;

SET hive.llap.io.enabled=true;
select sum(hash(*)) from (
select cint, csmallint, cbigint from orc_llap where cint > 10 and cbigint is not null) tmp;
select sum(hash(*)) from (
select * from orc_llap where cint > 10 and cbigint is not null) tmp;
select sum(hash(*)) from (
select cstring2 from orc_llap where cint > 5 and cint < 10) tmp;
select sum(hash(*)) from (
select * from orc_llap inner join cross_numbers on csmallint = i) tmp;
select sum(hash(*)) from (
select o1.cstring1, o2.cstring2 from orc_llap o1 inner join orc_llap o2 on o1.csmallint = o2.csmallint where o1.cbigint is not null and o2.cbigint is not null) tmp;

