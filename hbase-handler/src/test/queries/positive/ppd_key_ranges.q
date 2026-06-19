--! qt:dataset:src
CREATE EXTERNAL TABLE hbase_ppd_keyrange(key int, value string) 
STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
WITH SERDEPROPERTIES ("hbase.columns.mapping" = ":key#binary,cf:string")
TBLPROPERTIES ("external.table.purge" = "true");

INSERT OVERWRITE TABLE hbase_ppd_keyrange 
SELECT *
FROM src;

explain select * from hbase_ppd_keyrange where key > 8 and key < 21;
select * from hbase_ppd_keyrange where key > 8 and key < 21;

explain select * from hbase_ppd_keyrange where key > 8 and key <= 17;
select * from hbase_ppd_keyrange where key > 8 and key <= 17;


explain select * from hbase_ppd_keyrange where key > 8 and key <= 17 and value like '%11%';
select * from hbase_ppd_keyrange where key > 8 and key <= 17 and value like '%11%';

explain select * from hbase_ppd_keyrange where key >= 9 and key < 17 and key = 11;
select * from hbase_ppd_keyrange where key >=9  and key < 17 and key = 11;

drop table  hbase_ppd_keyrange;
