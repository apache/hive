-- SORT_QUERY_RESULTS
select key from
(
select key from src
union all
select key from src
) tab group by key
union all
select key from src;
