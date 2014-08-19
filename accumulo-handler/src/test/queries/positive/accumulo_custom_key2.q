CREATE TABLE accumulo_ck_3(key struct<col1:string,col2:string,col3:string>, value string)
STORED BY 'org.apache.hadoop.hive.accumulo.AccumuloStorageHandler'
WITH SERDEPROPERTIES (
    "accumulo.table.name" = "accumulo_custom2",
    "accumulo.columns.mapping" = ":rowid,cf:string",
    "accumulo.composite.rowid"="org.apache.hadoop.hive.accumulo.serde.FirstCharAccumuloCompositeRowId");

insert overwrite table accumulo_ck_3 select struct('abcd','mnop','wxyz'),'value'
from src where key = 100;

select * from accumulo_ck_3;

DROP TABLE accumulo_ck_3;
