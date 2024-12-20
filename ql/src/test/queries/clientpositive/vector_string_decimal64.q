set hive.explain.user=false;
set hive.fetch.task.conversion=none;

create external table parquet_decimal64(salary string) stored as parquet;      
insert into parquet_decimal64 values ('0'), ('43.5'), ('144.2'), ('45.7'), ('100');

explain select * from parquet_decimal64 
    where cast(salary as decimal(12, 2)) <= 100 and cast(salary as decimal(12, 2)) >= 0.0;  
    
-- SORT_QUERY_RESULTS    
select * from parquet_decimal64 
    where cast(salary as decimal(12, 2)) <= 100 and cast(salary as decimal(12, 2)) >= 0.0;
    
drop table parquet_decimal64;