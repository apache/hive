--! qt:dataset:src
explain
select *
from src
where (exists(select key from src)) in (select key from src);