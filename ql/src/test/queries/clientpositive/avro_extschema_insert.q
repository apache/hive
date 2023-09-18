set hive.exec.dynamic.partition.mode=nonstrict;

dfs -cp ${system:hive.root}data/files/table1.avsc ${system:test.tmp.dir}/;

create external table avro_extschema_insert1 (name string) partitioned by (p1 string)
  stored as avro tblproperties ('avro.schema.url'='${system:test.tmp.dir}/table1.avsc');

describe avro_extschema_insert1;

create external table avro_extschema_insert2 like avro_extschema_insert1;

insert overwrite table avro_extschema_insert1 partition (p1='part1') values ('col1_value', 1, 'col3_value');

insert overwrite table avro_extschema_insert2 partition (p1) select * from avro_extschema_insert1;
select * from avro_extschema_insert2;

dfs -rm  ${system:test.tmp.dir}/table1.avsc;

drop table avro_extschema_insert1;
drop table avro_extschema_insert2;
