CREATE TABLE accumulo_ck_1(key struct<col1:string,col2:string,col3:string>, value string)
STORED BY 'org.apache.hadoop.hive.accumulo.AccumuloStorageHandler'
WITH SERDEPROPERTIES (
    "accumulo.table.name" = "accumulo_custom",
    "accumulo.columns.mapping" = ":rowid,cf:string",
    "accumulo.composite.rowid.factory"="org.apache.hadoop.hive.accumulo.serde.DelimitedAccumuloRowIdFactory",
    "accumulo.composite.delimiter" = "$");

CREATE EXTERNAL TABLE accumulo_ck_2(key string, value string)
STORED BY 'org.apache.hadoop.hive.accumulo.AccumuloStorageHandler'
WITH SERDEPROPERTIES (
    "accumulo.table.name" = "accumulo_custom",
    "accumulo.columns.mapping" = ":rowid,cf:string");

insert overwrite table accumulo_ck_1 select struct('1000','2000','3000'),'value'
from src where key = 100;

select * from accumulo_ck_1;
select * from accumulo_ck_2;

DROP TABLE accumulo_ck_1;
DROP TABLE accumulo_ck_2;
