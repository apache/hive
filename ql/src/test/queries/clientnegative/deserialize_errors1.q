drop table if exists deserialize_errors;
add jar ${system:maven.local.repository}/org/apache/hive/hive-it-custom-serde/${system:hive.version}/hive-it-custom-serde-${system:hive.version}.jar;

create table deserialize_errors(col1 string, col2 int) ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.BadRecordsSerDe';
insert into table deserialize_errors values ('foo', 1), ('bar', 2), ('hel', 3), ('wor', 4);

set hive.exec.max.deserialize.errors.pertask=2;

explain
select * from deserialize_errors;
select * from deserialize_errors;

drop table deserialize_errors;
