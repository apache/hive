--! qt:dataset:src
CREATE EXTERNAL TABLE hbase_ck_1(key struct<col1:string,col2:string,col3:string>, value string)
STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
WITH SERDEPROPERTIES (
    "hbase.table.name" = "hbase_custom",
    "hbase.columns.mapping" = ":key,cf:string",
    "hbase.composite.key.factory"="org.apache.hadoop.hive.hbase.SampleHBaseKeyFactory")
TBLPROPERTIES ("external.table.purge" = "true");

CREATE EXTERNAL TABLE hbase_ck_2(key string, value string)
STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
WITH SERDEPROPERTIES (
    "hbase.table.name" = "hbase_custom",
    "hbase.columns.mapping" = ":key,cf:string");

from src tablesample (1 rows)
insert into table hbase_ck_1 select struct('1000','2000','3000'),'value';

select * from hbase_ck_1;
select * from hbase_ck_2;
