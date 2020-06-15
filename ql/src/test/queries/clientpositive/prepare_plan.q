--! qt:dataset:src

prepare p1 as select * from src where key > $1 limit 10;

-- single parameter
execute p1 (1,0);
select * from src where key > 0 limit 10;

-- different parameter for the same prepare query
execute p1 (1,200);
select * from src where key > 200 limit 10;

-- FAILS: pass string arguments
-- execute p1 (1, '0');

-- multiple parameters
prepare p2 as select * from src where key > $1 + $2 limit 10;

execute p2(1, 0, 2, 255);
select * from src where key > (0 + 255) limit 10;


-- Following fails
--execute p1 (1,-34);

