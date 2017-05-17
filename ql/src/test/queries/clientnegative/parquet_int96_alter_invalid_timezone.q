-- alter table to invalid table property
create table timestamps (ts timestamp) stored as parquet;
alter table timestamps set tblproperties ('parquet.mr.int96.write.zone'='Invalid');

drop table timestamps;
