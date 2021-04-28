--! qt:dataset:impala_dataset

create table test_table_n1 (my_id int)
partitioned by (my_date date);

explain
analyze table test_table_n1 partition(my_date='2010-01-01')
compute incremental statistics for columns my_id;
