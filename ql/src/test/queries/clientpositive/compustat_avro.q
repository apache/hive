drop table if exists testAvro;

dfs -cp ${system:hive.root}data/files/grad.avsc ${system:test.tmp.dir}/;

-- File URIs using system:hive.root (using file:/) don't seem to work properly in DDL statements on Windows,
-- so use dfs to copy them over to system:test.tmp.dir (which uses pfile:/), which does appear to work

create table testAvro
   ROW FORMAT SERDE                                                                      
     'org.apache.hadoop.hive.serde2.avro.AvroSerDe'                                      
   STORED AS INPUTFORMAT                                                                 
     'org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat'                        
   OUTPUTFORMAT                                                                          
     'org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat'
   TBLPROPERTIES ('avro.schema.url'='${system:test.tmp.dir}/grad.avsc');

describe formatted testAvro col1;

analyze table testAvro compute statistics for columns col1,col3;

describe formatted testAvro col1;
