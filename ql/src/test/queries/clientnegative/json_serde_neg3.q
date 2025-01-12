drop table if exists json_serde3_1;

create table json_serde3_1 (tiny_value TINYINT, small_value SMALLINT, int_value INT, big_value BIGINT)
  row format serde 'org.apache.hive.hcatalog.data.JsonSerDe';

insert into table json_serde3_1 values (127, 32768, 2147483648, 9223372036854775808);

select * from json_serde3_1;