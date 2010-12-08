create database db1;

show databases;

create database db2 with dbproperties (
  'mapred.jobtracker.url'='http://my.jobtracker.com:53000',
  'hive.warehouse.dir' = '/user/hive/warehouse',
  'mapred.scratch.dir' = 'hdfs://tmp.dfs.com:50029/tmp');

describe database db2;

describe database extended db2;



