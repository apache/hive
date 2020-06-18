--! qt:dataset:src
--! qt:dataset:alltypesorc

set hive.explain.user=false;
explain select * from src where key > '0' limit 10;

explain prepare p1 as select * from src where key > $1 limit 10;
prepare p1 as select * from src where key > $1 limit 10;

-- single param
execute p1 (1,'200');
select * from src where key > '200' limit 10;

-- same query, different param
execute p1 (1, '0');
select * from src where key > '0' limit 10;

-- different param type
execute p1 (1,0);
select * from src where key > 0 limit 10;


-- TODO: following fails
-- multiple parameters
--explain prepare p2 as select min(ctinyint), max(cbigint) from alltypesorc where cint > ($1 + $2 + $3) group by ctinyint;
--prepare p2 as select min(ctinyint), max(cbigint) from alltypesorc where cint > ($1 + $2 + $3) group by ctinyint;
--
--select min(ctinyint), max(cbigint) from alltypesorc where cint > (0 + 1 + 2) group by ctinyint;
--execute p2 ( 1,0, 2,1, 3, 2);
--select min(ctinyint), max(cbigint) from alltypesorc where cint > (0 + 1 + 2) group by ctinyint;
--
----TODO: concat
--explain prepare p3 as select * from src where key > concat($1, $2);
--prepare p3 as select * from src where key > concat($1, $2);
--
--execute p3 (1, '1', 2 '20');
--select * from src where key > concat('1', '20');
--

-- TODO: FAILS

-- wrong results
-- execute p1 (1,200);

-- parsing fails
--execute p1 (1,-34);

