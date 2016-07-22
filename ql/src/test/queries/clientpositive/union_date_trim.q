drop table if exists testDate;
create table testDate(id int, dt date);
insert into table testDate select 1, '2014-04-07' from src where key=100 limit 1;
insert into table testDate select 2, '2014-04-08' from src where key=100 limit 1;
insert into table testDate select 3, '2014-04-09' from src where key=100 limit 1;
--- without the fix following query will throw HiveException: Incompatible types for union operator
insert into table testDate select id, tm from (select id, dt as tm from testDate where id = 1 union all select id, dt as tm from testDate where id = 2 union all select id, cast(trim(Cast (dt as string)) as date) as tm from testDate where id = 3 ) a;
