SET hive.support.concurrency=false;

DROP TABLE accessLog;
CREATE EXTERNAL TABLE accessLog
      (row_key string, column_name string,  value string)
      STORED BY 'org.apache.hadoop.hive.cassandra.CassandraStorageHandler'
WITH SERDEPROPERTIES ("cassandra.host" = "127.0.0.1" , "cassandra.port" = "9170")
TBLPROPERTIES ("cassandra.ks.name" = "Hive");

INSERT OVERWRITE TABLE accessLog
select key, 'divide7', value  from src where (key%7)=0;

select * from accessLog;

INSERT OVERWRITE TABLE accessLog
select key, 'divide9', value  from src where (key%9)=0;

select * from accessLog;

select * from accessLog where column_name != 'divide9';

DROP TABLE accessLog;
