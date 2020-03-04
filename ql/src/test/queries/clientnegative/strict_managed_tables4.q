
set metastore.strict.managed.tables=true;

dfs -rm -f ${system:test.tmp.dir}/smt6_schema.avsc;
dfs -cp ${system:hive.root}data/files/grad.avsc ${system:test.tmp.dir}/smt6_schema.avsc;

CREATE EXTERNAL TABLE strict_managed_tables6_tab1
STORED AS AVRO
TBLPROPERTIES ('avro.schema.url'='${system:test.tmp.dir}/smt6_schema.avsc');

describe strict_managed_tables6_tab1;

-- avro table with external schema url not allowed as managed table
CREATE TABLE strict_managed_tables6_tab2
STORED AS AVRO
TBLPROPERTIES ('avro.schema.url'='${system:test.tmp.dir}/smt6_schema.avsc');

dfs -rm ${system:test.tmp.dir}/smt6_schema.avsc;
