SET hive.vectorized.execution.enabled=true;
set hive.fetch.task.conversion=none;

create table map_table_n0 (foo STRING , bar MAP<STRING, STRING>)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
COLLECTION ITEMS TERMINATED BY ','
MAP KEYS TERMINATED BY ':'
STORED AS TEXTFILE;

load data local inpath "../../data/files/map_table.txt" overwrite into table map_table_n0;

explain vectorization detail
select * from map_table_n0;
select * from map_table_n0;