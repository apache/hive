--! qt:dataset:src
select *, count(value) from src group by key;
