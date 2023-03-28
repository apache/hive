--! qt:dataset:src
-- Limit and offset queries
select key from src limit 1;
select key from src limit 1 offset 1;
select key from src limit (1+2*3);
select key from src limit (1+2*3) offset (3*4*5);
select key from src order by key limit (1+2*3) offset (3*4*5);

-- Nested queries
select key from (select * from src limit (1+2*3)) q1;
select key from (select * from src limit (1+2*3) offset (3*4*5)) q1;