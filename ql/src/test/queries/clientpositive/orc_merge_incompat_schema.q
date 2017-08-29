set hive.metastore.disallow.incompatible.col.type.changes=false;

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
  val INT
) STORED AS ORC tblproperties("orc.row.index.stride"="1000", "orc.stripe.size"="1000", "orc.compress.size"="10000");

INSERT OVERWRITE TABLE orc_create_complex SELECT str,mp,lst,strct,0 FROM orc_create_staging;
INSERT INTO TABLE orc_create_complex SELECT str,mp,lst,strct,0 FROM orc_create_staging;

dfs -ls ${hiveconf:hive.metastore.warehouse.dir}/orc_create_complex/;
select sum(hash(*)) from orc_create_complex;

-- will be merged as the schema is the same
ALTER TABLE orc_create_complex CONCATENATE;

dfs -ls ${hiveconf:hive.metastore.warehouse.dir}/orc_create_complex/;
select sum(hash(*)) from orc_create_complex;

ALTER TABLE orc_create_complex
CHANGE COLUMN strct strct STRUCT<A:STRING,B:STRING,C:STRING>;

INSERT INTO TABLE orc_create_complex SELECT str,mp,lst,NAMED_STRUCT('A',strct.A,'B',strct.B,'C','c'),0 FROM orc_create_staging;

dfs -ls ${hiveconf:hive.metastore.warehouse.dir}/orc_create_complex/;
select sum(hash(*)) from orc_create_complex;

-- schema is different for both files, will not be merged
ALTER TABLE orc_create_complex CONCATENATE;

dfs -ls ${hiveconf:hive.metastore.warehouse.dir}/orc_create_complex/;
select sum(hash(*)) from orc_create_complex;
