CREATE TABLE testhbase_decimal (
id int,
balance decimal(15,2))
ROW FORMAT DELIMITED
COLLECTION ITEMS TERMINATED BY '~'
STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
WITH SERDEPROPERTIES (
"hbase.columns.mapping"=":key,cf:balance#b");

insert into testhbase_decimal values (1,1), (2, 2.2), (3, 33.33);

select * from testhbase_decimal;

drop table testhbase_decimal;
