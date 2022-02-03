CREATE TABLE hbase_table(row_key string, c1 boolean, c2 boolean)
STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
WITH SERDEPROPERTIES (
"hbase.columns.mapping" = ":key,cf:c1,cf:c2"
);

explain select * from hbase_table where c1 and c2;
explain select * from hbase_table where c1=true and c2=true;
