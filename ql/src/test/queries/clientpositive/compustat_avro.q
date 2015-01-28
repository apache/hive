drop table if exists testAvro;
create table testAvro
   ROW FORMAT SERDE                                                                      
     'org.apache.hadoop.hive.serde2.avro.AvroSerDe'                                      
   STORED AS INPUTFORMAT                                                                 
     'org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat'                        
   OUTPUTFORMAT                                                                          
     'org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat'
   TBLPROPERTIES ('avro.schema.url'='file://${system:hive.root}data/files/grad.avsc');

describe formatted testAvro.col1;

analyze table testAvro compute statistics for columns col1,col3;

describe formatted testAvro.col1;
