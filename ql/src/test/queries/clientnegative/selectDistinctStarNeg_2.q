-- SELECT DISTINCT and GROUP BY can not be in the same query. Error encountered near token ‘key’

select distinct * from src group by key;