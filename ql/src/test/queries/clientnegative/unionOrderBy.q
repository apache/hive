--! qt:dataset:src
-- orderByClause clusterByClause distributeByClause sortByClause limitClause
-- can only be applied to the whole union.

select key from src order by key
union all
select key from src;



