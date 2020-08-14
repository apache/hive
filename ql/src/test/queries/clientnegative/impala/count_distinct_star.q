--! qt:dataset:impala_dataset

create table my_table_n1 (my_id int);

EXPLAIN CBO
SELECT COUNT(DISTINCT *)
 FROM my_table_n1;
