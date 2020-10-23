drop table if exists deserialize_errors;
add jar ${system:maven.local.repository}/org/apache/hive/hive-it-custom-serde/${system:hive.version}/hive-it-custom-serde-${system:hive.version}.jar;

create table deserialize_errors(col1 string, col2 int) ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.BadRecordsSerDe';
insert into table deserialize_errors values ('foo', 1), ('bar', 2), ('hel', 3), ('wor', 4);

set hive.vectorized.execution.enabled=false;
set hive.exec.max.deserialize.errors.pertask=2;

from deserialize_errors
select col1, count(*)
group by col1;

drop table deserialize_errors;
