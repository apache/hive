set hive.mapred.mode=nonstrict;
SET hive.vectorized.execution.enabled=true;

SET hive.llap.io.enabled=false;

SET hive.exec.orc.default.buffer.size=32768;
SET hive.exec.orc.default.row.index.stride=1000;
SET hive.optimize.index.filter=true;
set hive.auto.convert.join=false;
set hive.fetch.task.conversion=none;

DROP TABLE orc_create_staging;
DROP TABLE orc_create_complex;
DROP TABLE orc_llap_nonvector;


CREATE TABLE orc_create_staging (
  str STRING,
  mp  MAP<STRING,STRING>,
  lst ARRAY<STRING>,
  strct STRUCT<A:STRING,B:STRING>
) ROW FORMAT DELIMITED
    FIELDS TERMINATED BY '|'
    COLLECTION ITEMS TERMINATED BY ','
    MAP KEYS TERMINATED BY ':';
LOAD DATA LOCAL INPATH '../../data/files/orc_create.txt' OVERWRITE INTO TABLE orc_create_staging;

create table orc_llap_nonvector stored as orc as select *, rand(1234) rdm from alltypesorc order by rdm;

SET hive.llap.io.enabled=true;
set hive.auto.convert.join=true;
SET hive.vectorized.execution.enabled=false;

explain 
select * from orc_llap_nonvector limit 100;
select * from orc_llap_nonvector limit 100;
explain 
select cint, cstring1 from orc_llap_nonvector limit 1025;
select cint, cstring1 from orc_llap_nonvector limit 1025;

DROP TABLE orc_create_staging;
DROP TABLE orc_llap_nonvector;
