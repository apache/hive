set hive.compute.query.using.stats=false;
set hive.strict.checks.cartesian.product=false;
set hive.cli.print.header=true;
set hive.explain.user=false;
set hive.fetch.task.conversion=none;
SET hive.vectorized.execution.enabled=true;
set hive.llap.io.enabled=false;
set hive.mapred.mode=nonstrict;

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

CREATE TABLE orc_create_complex (
  str STRING,
  mp  MAP<STRING,STRING>,
  lst ARRAY<STRING>,
  strct STRUCT<A:STRING,B:STRING>,
  val string
) STORED AS ORC tblproperties("orc.row.index.stride"="1000", "orc.stripe.size"="1000", "orc.compress.size"="10000");

INSERT OVERWRITE TABLE orc_create_complex
SELECT orc_create_staging.*, '0' FROM orc_create_staging;

set hive.llap.io.enabled=true;

SELECT * FROM orc_create_complex;

SELECT str FROM orc_create_complex;

SELECT strct, mp, lst FROM orc_create_complex;

SELECT lst, str FROM orc_create_complex;

SELECT mp, str FROM orc_create_complex;

SELECT strct, str FROM orc_create_complex;

SELECT strct.B, str FROM orc_create_complex;

set hive.llap.io.enabled=false;

INSERT INTO TABLE orc_create_complex
SELECT orc_create_staging.*, src1.key FROM orc_create_staging cross join src src1 cross join orc_create_staging spam1 cross join orc_create_staging spam2;

select count(*) from orc_create_complex;

set hive.llap.io.enabled=true;

SELECT distinct lst, strct FROM orc_create_complex;

SELECT str, count(val)  FROM orc_create_complex GROUP BY str;

SELECT strct.B, count(val) FROM orc_create_complex GROUP BY strct.B;

SELECT strct, mp, lst, str, count(val) FROM orc_create_complex GROUP BY strct, mp, lst, str;



