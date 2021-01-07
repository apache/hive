set hive.cli.print.header=true;

VALUES(1,2,3),(4,5,6);

SELECT * FROM (VALUES(1,2,3),(4,5,6)) as foo;

with t1 as (values('a', 'b'), ('b', 'c'))
select * from t1 where col1 = 'a'
union all
select * from t1 where col1 = 'b';
