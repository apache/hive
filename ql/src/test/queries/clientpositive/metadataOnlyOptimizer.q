select key from(
select '1' as key from srcpart where ds="2008-04-09"
UNION all
SELECT key from srcpart where ds="2008-04-09" and hr="11"
) tab group by key;

select key from(
SELECT '1' as key from src
UNION all
SELECT key as key from src
) tab group by key;

select max(key) from(
SELECT '1' as key from src
UNION all
SELECT key as key from src
) tab group by key;

select key from(
SELECT '1' as key from src
UNION all
SELECT '2' as key from src
) tab group by key;


select key from(
SELECT '1' as key from src
UNION all
SELECT key as key from src
UNION all
SELECT '2' as key from src
UNION all
SELECT key as key from src
) tab group by key;

select k from (SELECT '1' as k from src limit 0 union all select key as k from src limit 1)tab;

select k from (SELECT '1' as k from src limit 1 union all select key as k from src limit 0)tab;

select max(ds) from srcpart;

select count(ds) from srcpart;


