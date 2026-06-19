SET hive.vectorized.execution.enabled=false;

set hive.metastore.disallow.incompatible.col.type.changes=false;

CREATE TABLE orc_create_staging_n2 (
  str STRING,
  mp  MAP<STRING,STRING>,
  lst ARRAY<STRING>,
  strct STRUCT<A:STRING,B:STRING>
) ROW FORMAT DELIMITED
    FIELDS TERMINATED BY '|'
    COLLECTION ITEMS TERMINATED BY ','
    MAP KEYS TERMINATED BY ':';

LOAD DATA LOCAL INPATH '../../data/files/orc_create.txt' OVERWRITE INTO TABLE orc_create_staging_n2;

CREATE TABLE orc_create_complex_n2 (
  str STRING,
  mp  MAP<STRING,STRING>,
  lst ARRAY<STRING>,
  strct STRUCT<A:STRING,B:STRING>,
  val INT
) STORED AS ORC tblproperties("orc.row.index.stride"="1000", "orc.stripe.size"="1000", "orc.compress.size"="10000");

INSERT OVERWRITE TABLE orc_create_complex_n2 SELECT str,mp,lst,strct,0 FROM orc_create_staging_n2;
INSERT INTO TABLE orc_create_complex_n2 SELECT str,mp,lst,strct,0 FROM orc_create_staging_n2;

dfs -ls ${hiveconf:hive.metastore.warehouse.dir}/orc_create_complex_n2/;
select sum(hash(*)) from orc_create_complex_n2;

-- will be merged as the schema is the same
ALTER TABLE orc_create_complex_n2 CONCATENATE;

dfs -ls ${hiveconf:hive.metastore.warehouse.dir}/orc_create_complex_n2/;
select sum(hash(*)) from orc_create_complex_n2;

ALTER TABLE orc_create_complex_n2
CHANGE COLUMN strct strct STRUCT<A:STRING,B:STRING,C:STRING>;

INSERT INTO TABLE orc_create_complex_n2 SELECT str,mp,lst,NAMED_STRUCT('A',strct.A,'B',strct.B,'C','c'),0 FROM orc_create_staging_n2;

dfs -ls ${hiveconf:hive.metastore.warehouse.dir}/orc_create_complex_n2/;
select sum(hash(*)) from orc_create_complex_n2;

-- schema is different for both files, will not be merged
ALTER TABLE orc_create_complex_n2 CONCATENATE;

dfs -ls ${hiveconf:hive.metastore.warehouse.dir}/orc_create_complex_n2/;
select sum(hash(*)) from orc_create_complex_n2;
