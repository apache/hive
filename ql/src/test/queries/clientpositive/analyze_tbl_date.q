set hive.fetch.task.conversion=none;

create table test_table_n7(d date);

insert into test_table_n7 values(null), (null), (null);

analyze table test_table_n7 compute statistics for columns;

describe formatted test_table_n7;

explain select * from test_table_n7 where d is not null;

select * from test_table_n7 where d is not null;

