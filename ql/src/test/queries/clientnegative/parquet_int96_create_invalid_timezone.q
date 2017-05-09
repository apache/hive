-- create table with invalid table property
create table timestamps (ts timestamp) stored as parquet tblproperties('parquet.mr.int96.write.zone'='Invalid');

