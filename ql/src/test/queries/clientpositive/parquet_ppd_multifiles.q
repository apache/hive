CREATE TABLE parquet_ppd_multifiles (
  name string,
  `dec` decimal(5,0)
) stored as parquet;

insert into table parquet_ppd_multifiles values('Jim', 3);
insert into table parquet_ppd_multifiles values('Tom', 5);

set hive.optimize.index.filter=false;
select * from parquet_ppd_multifiles where (name = 'Jim' or `dec` = 5);

set hive.optimize.index.filter=true;
select * from parquet_ppd_multifiles where (name = 'Jim' or `dec` = 5);
