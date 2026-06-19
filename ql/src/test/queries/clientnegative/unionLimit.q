--! qt:dataset:src
-- orderByClause clusterByClause distributeByClause sortByClause limitClause
-- can only be applied to the whole union.

select key from src limit 1
union all
select key from src;




