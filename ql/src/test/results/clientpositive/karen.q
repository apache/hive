-- txn_components local test:


create table idl_uva_hist_bucket
(i int, tool_id bigint)
PARTITIONED BY (`pdate` string)
CLUSTERED BY (tool_id)   
  INTO 10 BUCKETS   
  ROW FORMAT SERDE                                   
   'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'  
 STORED AS INPUTFORMAT                              
   'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'  
 OUTPUTFORMAT                                       
   'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
tblproperties( 
 'orc.compress'='SNAPPY',                         
   'transactional'='true',                          
   'transactional_properties'='insert_only');

create table ingest_uva_hist_bucket (i int, tool_id bigint, pdate string);

insert into ingest_uva_hist_bucket values (1, 3, "row1"),
(2, 4, "row2")
;

Insert into idl_uva_hist_bucket PARTITION(pdate)
select
i, tool_id, pdate
from ingest_uva_hist_bucket;

drop table idl_uva_hist_bucket;
drop table ingest_uva_hist_bucket;