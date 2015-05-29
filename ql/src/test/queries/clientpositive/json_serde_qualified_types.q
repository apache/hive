
add jar ${system:maven.local.repository}/org/apache/hive/hcatalog/hive-hcatalog-core/${system:hive.version}/hive-hcatalog-core-${system:hive.version}.jar;

create table json_serde_qualified_types (
  c1 char(10),
  c2 varchar(20),
  c3 decimal(10, 5)
) row format serde 'org.apache.hive.hcatalog.data.JsonSerDe';

describe json_serde_qualified_types;

drop table json_serde_qualified_types;
