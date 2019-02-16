--! qt:dataset:src
-- HIVE-4375 Single sourced multi insert consists of native and non-native table mixed throws NPE
CREATE TABLE src_x1(key string, value string);
CREATE EXTERNAL TABLE src_x2(key string, value string)
STORED BY 'org.apache.hadoop.hive.accumulo.AccumuloStorageHandler'
WITH SERDEPROPERTIES ("accumulo.columns.mapping" = ":rowid, cf:value")
TBLPROPERTIES ("external.table.purge" = "true");

explain
from src a
insert overwrite table src_x1
select key,"" where a.key > 0 AND a.key < 50
insert overwrite table src_x2
select value,"" where a.key > 50 AND a.key < 100;

from src a
insert overwrite table src_x1
select key,"" where a.key > 0 AND a.key < 50
insert overwrite table src_x2
select value,"" where a.key > 50 AND a.key < 100;

select * from src_x1 order by key;
select * from src_x2 order by key;

DROP TABLE src_x1;
DROP TABLE src_x2;
