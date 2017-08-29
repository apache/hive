DROP TABLE IF EXISTS test1_vc;
DROP TABLE IF EXISTS test2_vc;
CREATE TABLE test1_vc
 (
   id string)
   PARTITIONED BY (
  cr_year bigint,
  cr_month bigint)
 ROW FORMAT SERDE
  'org.apache.hadoop.hive.serde2.columnar.ColumnarSerDe'
STORED AS INPUTFORMAT
  'org.apache.hadoop.hive.ql.io.RCFileInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.RCFileOutputFormat'
TBLPROPERTIES (
  'serialization.null.format'='' );
CREATE TABLE test2_vc(
    id string
  )
   PARTITIONED BY (
  cr_year bigint,
  cr_month bigint)
ROW FORMAT SERDE
  'org.apache.hadoop.hive.serde2.columnar.ColumnarSerDe'
STORED AS INPUTFORMAT
  'org.apache.hadoop.hive.ql.io.RCFileInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.RCFileOutputFormat'
TBLPROPERTIES (
  'serialization.null.format'=''
 );
set hive.auto.convert.join=false;
set hive.vectorized.execution.enabled = true;
set hive.mapred.mode=nonstrict;
set hive.fetch.task.conversion=none;
SELECT cr.id1 ,
cr.id2
FROM
(SELECT t1.id id1,
 t2.id id2
 from
 (select * from test1_vc ) t1
 left outer join test2_vc  t2
 on t1.id=t2.id) cr;
