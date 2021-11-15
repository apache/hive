drop table if exists table1_n1;
drop table if exists table1_1;

dfs -cp ${system:hive.root}data/files/table1.avsc ${system:test.tmp.dir}/avrotblsjoin.avsc;
dfs -cp ${system:hive.root}data/files/table1_1.avsc ${system:test.tmp.dir}/avrotblsjoin_1.avsc;

create table table1_n1
   ROW FORMAT SERDE
     'org.apache.hadoop.hive.serde2.avro.AvroSerDe'
   STORED AS INPUTFORMAT
     'org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat'
   OUTPUTFORMAT
     'org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat'
   TBLPROPERTIES ('avro.schema.url'='${system:test.tmp.dir}/avrotblsjoin.avsc');
create table table1_1
   ROW FORMAT SERDE
     'org.apache.hadoop.hive.serde2.avro.AvroSerDe'
   STORED AS INPUTFORMAT
     'org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat'
   OUTPUTFORMAT
     'org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat'
   TBLPROPERTIES ('avro.schema.url'='${system:test.tmp.dir}/avrotblsjoin_1.avsc');
insert into table1_n1 values ("1", "2", "3");
insert into table1_1 values (1, "2");
set hive.auto.convert.join=false;
set hive.strict.checks.type.safety=false;
set hive.mapred.mode=nonstrict;
select table1_n1.col1, table1_1.* from table1_n1 join table1_1 on table1_n1.col1=table1_1.col1 where table1_1.col1="1";

dfs -rm ${system:test.tmp.dir}/avrotblsjoin.avsc;
dfs -rm ${system:test.tmp.dir}/avrotblsjoin_1.avsc;
