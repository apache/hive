drop table if exists table_with_utf8_encoding;

create table table_with_utf8_encoding (name STRING) 
 ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
 WITH SERDEPROPERTIES ('serialization.encoding'='utf-8');

load data local inpath '../../data/files/encoding-utf8.txt' overwrite into table table_with_utf8_encoding;

select * from table_with_utf8_encoding;

drop table if exists table_with_non_utf8_encoding;

create table table_with_non_utf8_encoding (name STRING) 
 ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
 WITH SERDEPROPERTIES ('serialization.encoding'='ISO8859_1');

insert overwrite table table_with_non_utf8_encoding  select name  from table_with_utf8_encoding;

select * from table_with_non_utf8_encoding;

