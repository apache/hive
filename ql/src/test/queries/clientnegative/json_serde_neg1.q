drop table if exists json_serde1_1;

create table json_serde1_1 (tiny_value TINYINT, small_value SMALLINT, int_value INT, big_value BIGINT)
  row format serde 'org.apache.hadoop.hive.serde2.JsonSerDe';

insert into table json_serde1_1 values (128, 32768, 2147483648, 9223372036854775808);

select * from json_serde1_1;

drop table json_serde1_1;