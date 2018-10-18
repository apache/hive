SET hive.vectorized.execution.enabled=true;
set hive.fetch.task.conversion=none;

create table coalesce_test(a int, b int) stored as orc;

insert into coalesce_test values (1, 2);

-- Add a single NULL row that will come from ORC as isRepeated.
insert into coalesce_test values (NULL, NULL);

explain vectorization detail
select coalesce(a, b) from coalesce_test order by a, b;

select coalesce(a, b) from coalesce_test order by a, b;;