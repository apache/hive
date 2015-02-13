SET hive.vectorized.execution.enabled=true;

SET hive.llap.io.enabled=false;
SET hive.llap.io.cache.orc.arena.size=16777216;
SET hive.llap.io.cache.orc.size=67108864;
SET hive.llap.io.use.lrfu=false;

SET hive.exec.orc.default.buffer.size=32768;
SET hive.exec.orc.default.row.index.stride=1000;


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
  
select cint, csmallint, cbigint from orc_llap where cint > 10 and cbigint is not null order by cbigint limit 1000;