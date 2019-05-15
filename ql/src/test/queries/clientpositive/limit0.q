--! qt:dataset:src
explain select key from src limit 0;
explain select key from src where key = '238' limit 0;
explain select * from src where key = '238' limit 0;
explain select src.key, count(src.value) from src group by src.key limit 0;
explain select * from ( select key from src limit 3) sq1 limit 0;

select key from src limit 0;
select key from src where key = '238' limit 0;
select * from src where key = '238' limit 0;
select src.key, count(src.value) from src group by src.key limit 0;
select * from ( select key from src limit 3) sq1 limit 0;  
