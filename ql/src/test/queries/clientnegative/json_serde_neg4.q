drop table if exists json_serde4_1;

create table json_serde4_1 (tiny_value TINYINT, small_value SMALLINT, int_value INT, big_value BIGINT)
  row format serde 'org.apache.hive.hcatalog.data.JsonSerDe';

LOAD DATA LOCAL INPATH '../../data/files/sampleJson.json' INTO TABLE json_serde4_1;

select * from json_serde4_1;