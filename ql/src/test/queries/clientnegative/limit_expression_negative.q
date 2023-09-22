--! qt:dataset:src
-- Limit and offset queries
select key from src limit 1.5;
select key from src limit 1.5 offset 1;
select key from src limit (1) offset (1.5);
select key from src limit 1 offset (1.5+2);
select key from src limit (1+2.5) offset 1;
select key from src limit (1+2*3.5);
select key from src limit (1+2*3.5) offset (3*4*5.5);
select key from src order by key limit (1+2*3.5) offset (3*4*5.5);