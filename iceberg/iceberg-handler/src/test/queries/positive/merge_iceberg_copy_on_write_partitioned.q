-- SORT_QUERY_RESULTS
set hive.explain.user=false;

drop table if exists target_ice;
drop table if exists source;

create external table target_ice(a int, b string, c int) partitioned by spec (bucket(16, a), truncate(3, b)) stored by iceberg tblproperties ('format-version'='2', 'write.merge.mode'='copy-on-write');
create table source(a int, b string, c int);

insert into target_ice values (1, 'match', 50), (2, 'not match', 51), (3, 'delete', 55), (4, 'not delete null', null), (null, 'match null', 56);
insert into source values (1, 'match', 50), (22, 'not match', 51), (3, 'delete', 55), (4, 'not delete null', null), (null, 'match null', 56);

-- merge
explain
merge into target_ice as t using source src ON t.a = src.a
when matched and t.c > 50 THEN DELETE
when matched then update set b = concat(t.b, ' Merged'), c = t.c + 10
when not matched then insert values (src.a, concat(src.b, ' New'), src.c);

-- insert clause with a column list
explain
merge into target_ice as t using source src ON t.a = src.a
when matched and t.c > 50 THEN DELETE
when not matched then insert (a, b) values (src.a, concat(src.b, '-merge new 2'));

merge into target_ice as t using source src ON t.a = src.a
when matched and t.c > 50 THEN DELETE
when matched then update set b = concat(t.b, ' Merged'), c = t.c + 10
when not matched then insert values (src.a, concat(src.b, ' New'), src.c);

select * from target_ice;

-- update all
explain
merge into target_ice as t using source src ON t.a = src.a
when matched then update set b = 'Merged', c = t.c - 10;

merge into target_ice as t using source src ON t.a = src.a
when matched then update set b = 'Merged', c = t.c - 10;

select * from target_ice;
