-- Tests a query with union all that can be optimized by removing the union operator

create table if not exists test_table(column1 string, column2 int);
insert into test_table values('a',1),('b',2);

set hive.optimize.union.remove=true;

explain
select column1 from test_table group by column1
union all
select column1 from test_table group by column1;

select column1 from test_table group by column1
union all
select column1 from test_table group by column1;

drop table test_table;