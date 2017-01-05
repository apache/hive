set hive.fetch.task.conversion=none;

create table test_table(d date);

insert into test_table values(null), (null), (null);

analyze table test_table compute statistics for columns;

describe formatted test_table;

explain select * from test_table where d is not null;

select * from test_table where d is not null;

