-- SORT_QUERY_RESULTS
set hive.explain.user=false;

create external table target_ice(a int, b string, c int) partitioned by spec (bucket(16, a), truncate(3, b)) stored by iceberg stored as orc tblproperties ('format-version'='2');
create table source(a int, b string, c int);

insert into target_ice values (1, 'match', 50), (2, 'not match', 51), (3, 'delete', 55), (4, 'not delete null', null), (null, 'match null', 56);
insert into source values (1, 'match', 50), (22, 'not match', 51), (3, 'delete', 55), (4, 'not delete null', null), (null, 'match null', 56);

-- merge
explain
merge into target_ice as t using source src ON t.a <=> src.a
when matched and t.c > 50 THEN DELETE
when matched then update set b = concat(t.b, ' Merged'), c = t.c + 10
when not matched then insert values (src.a, concat(src.b, ' New'), src.c);

merge into target_ice as t using source src ON t.a <=> src.a
when matched and t.c > 50 THEN DELETE
when matched then update set b = concat(t.b, ' Merged'), c = t.c + 10
when not matched then insert values (src.a, concat(src.b, ' New'), src.c);

select * from target_ice;
