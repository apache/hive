drop table if exists encodelat1;
create table encodelat1 (name STRING) 
 ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
 WITH SERDEPROPERTIES ('serialization.encoding'='ISO8859_1');
load data local inpath '../../data/files/encoding_iso-8859-1.txt' overwrite into table encodelat1;
select * from encodelat1;

