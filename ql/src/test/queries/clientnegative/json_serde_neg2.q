drop table if exists json_serde2_1;

create table json_serde2_1 (tiny_value TINYINT, small_value SMALLINT, int_value INT, big_value BIGINT)
  row format serde 'org.apache.hadoop.hive.serde2.JsonSerDe';

LOAD DATA LOCAL INPATH '../../data/files/sampleJson.json' INTO TABLE json_serde2_1;

select * from json_serde2_1;

drop table json_serde2_1;