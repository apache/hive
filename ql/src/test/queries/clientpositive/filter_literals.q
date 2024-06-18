
set hive.cbo.fallback.strategy=NEVER;

create table test (a int);

explain cbo
select * from test where 'foo';

explain cbo
select * from test where 'foo' and 'bar';

explain cbo
select * from test where 'foo' and a in (2, 3);

explain cbo 
select * from test where not 'foo';

explain cbo
select * from test where 1;

explain cbo
select * from test where 1 and 2;

explain cbo
select * from test where not 1;
