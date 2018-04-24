SET hive.vectorized.execution.enabled=false;
-- HIVE-5292 Join on decimal columns fails
-- SORT_QUERY_RESULTS

create table src_dec (key decimal(3,0), value string);
load data local inpath '../../data/files/kv1.txt' into table src_dec;

select * from src_dec a join src_dec b on a.key=b.key+450;
