set hive.exec.dynamic.partition.mode=nonstrict;

dfs -cp ${system:hive.root}data/files/table1.avsc ${system:test.tmp.dir}/avro_extschema_insert.avsc;

create external table avro_extschema_insert1 (name string) partitioned by (p1 string)
  stored as avro tblproperties ('avro.schema.url'='${system:test.tmp.dir}/avro_extschema_insert.avsc');

describe avro_extschema_insert1;

create external table avro_extschema_insert2 like avro_extschema_insert1 stored as avro
  tblproperties ('avro.schema.url'='${system:test.tmp.dir}/avro_extschema_insert.avsc');

desc formatted avro_extschema_insert2;

insert overwrite table avro_extschema_insert1 partition (p1='part1') values ('col1_value', 1, 'col3_value');

insert overwrite table avro_extschema_insert2 partition (p1) select * from avro_extschema_insert1;
select * from avro_extschema_insert2;

drop table avro_extschema_insert1;
drop table avro_extschema_insert2;

dfs -rm  ${system:test.tmp.dir}/avro_extschema_insert.avsc;
