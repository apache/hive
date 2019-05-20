--! qt:dataset:alltypesorc
SET hive.vectorized.execution.enabled=true;

SET hive.llap.io.enabled=false;

SET hive.exec.orc.default.row.index.stride=1000;
SET hive.optimize.index.filter=true;
set hive.auto.convert.join=false;

DROP TABLE orc_llap_n0;

-- this test mix and matches orc versions and flips config to use decimal64 column vectors
set hive.auto.convert.join=true;
SET hive.llap.io.enabled=true;
CREATE TABLE orc_llap_n0(
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
    cboolean2 BOOLEAN,
    cdecimal1 decimal(10,2),
    cdecimal2 decimal(38,5))
    STORED AS ORC tblproperties ("orc.compress"="NONE");

insert into table orc_llap_n0
select ctinyint, csmallint, cint, cbigint, cfloat, cdouble, cstring1, cstring2, ctimestamp1, ctimestamp2, cboolean1, cboolean2,
 cast("3.345" as decimal(10,2)), cast("5.56789" as decimal(38,5)) from alltypesorc;

alter table orc_llap_n0 set tblproperties ("orc.compress"="NONE", 'orc.write.format'='UNSTABLE-PRE-2.0');

insert into table orc_llap_n0
select ctinyint, csmallint, cint, cbigint, cfloat, cdouble, cstring1, cstring2, ctimestamp1, ctimestamp2, cboolean1, cboolean2,
 cast("4.456" as decimal(10,2)), cast("5.56789" as decimal(38,5)) from alltypesorc;

set hive.vectorized.input.format.supports.enabled=decimal_64;
explain vectorization select cdecimal1,cdecimal2 from orc_llap_n0 where cdecimal1 = cast("3.345" as decimal(10,2)) or cdecimal1 = cast("4.456" as decimal(10,2))
 group by cdecimal1,cdecimal2 limit 2;
select cdecimal1,cdecimal2 from orc_llap_n0 where cdecimal1 = cast("3.345" as decimal(10,2)) or cdecimal1 = cast("4.456" as decimal(10,2))
 group by cdecimal1,cdecimal2 limit 2;

set hive.vectorized.input.format.supports.enabled=none;
explain vectorization select cdecimal1,cdecimal2 from orc_llap_n0 where cdecimal1 = cast("3.345" as decimal(10,2)) or cdecimal1 = cast("4.456" as decimal(10,2))
 group by cdecimal1,cdecimal2 limit 2;
select cdecimal1,cdecimal2 from orc_llap_n0 where cdecimal1 = cast("3.345" as decimal(10,2)) or cdecimal1 = cast("4.456" as decimal(10,2))
 group by cdecimal1,cdecimal2 limit 2;

DROP TABLE orc_llap_n0;
