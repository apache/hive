--! qt:dataset:src
-- Limit and offset queries
select key from src limit 1;
select key from src limit 1 offset 1;
select key from src limit (1) offset (1);
select key from src limit 1 offset (1+2);
select key from src limit (1+2) offset 1;
select key from src limit (1+2*3);
