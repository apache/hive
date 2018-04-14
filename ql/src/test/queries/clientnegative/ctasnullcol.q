--! qt:dataset:src
set hive.mapred.mode=nonstrict;
drop table if exists orc_table_with_null;
CREATE TABLE orc_table_with_null STORED AS ORC AS SELECT key, null FROM src; 
