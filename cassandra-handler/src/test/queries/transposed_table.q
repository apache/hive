SET hive.support.concurrency=false;

DROP TABLE accessLog;
CREATE EXTERNAL TABLE accessLog
      (row_key string, column_name string,  value string)
      STORED BY 'org.apache.hadoop.hive.cassandra.CassandraStorageHandler'
WITH SERDEPROPERTIES ("cassandra.host" = "127.0.0.1" , "cassandra.port" = "9170")
TBLPROPERTIES ("cassandra.ks.name" = "Hive");

select * from accessLog;

INSERT OVERWRITE TABLE accessLog
select key, 'divide7', value  from src where (key%7)=0;

select row_key, column_name, value from accessLog;

INSERT OVERWRITE TABLE accessLog
select key, 'divide9', value  from src where (key%9)=0;

select row_key from accessLog;

select * from accessLog where column_name != 'divide9';

select row_key, count(*) from accessLog where row_key > 70 group by row_key;

--SuperColumn Mapping
DROP TABLE superLog;
CREATE EXTERNAL TABLE superLog
      (row_key string, column_name string, sub_column_name string, value string)
      STORED BY 'org.apache.hadoop.hive.cassandra.CassandraStorageHandler'
WITH SERDEPROPERTIES ("cassandra.host" = "127.0.0.1" , "cassandra.port" = "9170",  "cassandra.batchmutate.size" = "1")
TBLPROPERTIES ("cassandra.ks.name" = "Hive");

select * from superLog;

INSERT OVERWRITE TABLE superLog
select key, 'divide', '5', value  from src where (key%5)=0;

select * from superLog;

INSERT OVERWRITE TABLE superLog
select key, 'divide', '3', value  from src where (key%3)=0;

select row_key, sub_column_name, value from superLog;

select row_key, value from superLog where column_name = 'divide' and sub_column_name = '3';

select * from accessLog limit 1;
select * from accessLog limit 1;

DROP TABLE accessLog;
DROP TABLE superLog;

