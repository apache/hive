CREATE EXTERNAL TABLE test(code string,name string)
ROW FORMAT SERDE
   'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
 WITH SERDEPROPERTIES (
   'field.delim'='\t')
 STORED AS INPUTFORMAT
   'org.apache.hadoop.mapred.TextInputFormat'
 OUTPUTFORMAT
   'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
   location '${system:test.tmp.dir}/test'
 TBLPROPERTIES (
   'skip.header.line.count'='1',
   'textinputformat.record.delimiter'='|');


LOAD DATA LOCAL INPATH '../../data/files/header_footer_table_4/0003.txt' INTO TABLE test;


SELECT COUNT(*) FROM test;


SELECT * FROM test;