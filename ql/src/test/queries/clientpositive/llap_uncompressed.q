SET hive.vectorized.execution.enabled=true;

SET hive.llap.io.enabled=false;

SET hive.exec.orc.default.row.index.stride=1000;
SET hive.optimize.index.filter=true;
set hive.auto.convert.join=false;

DROP TABLE orc_llap;

set hive.auto.convert.join=true;
SET hive.llap.io.enabled=false;

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
    STORED AS ORC tblproperties ("orc.compress"="NONE");

insert into table orc_llap
select ctinyint, csmallint, cint, cbigint, cfloat, cdouble, cstring1, cstring2, ctimestamp1, ctimestamp2, cboolean1, cboolean2
from alltypesorc;


SET hive.llap.io.enabled=true;

drop table llap_temp_table;
explain
select * from orc_llap where cint > 10 and cbigint is not null;
create table llap_temp_table as
select * from orc_llap where cint > 10 and cbigint is not null;
select sum(hash(*)) from llap_temp_table;

explain
select * from orc_llap where cint > 10 and cint < 5000000;
select * from orc_llap where cint > 10 and cint < 5000000;

DROP TABLE orc_llap;
drop table llap_temp_table;
