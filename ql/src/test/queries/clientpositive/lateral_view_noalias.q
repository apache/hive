--! qt:dataset:src
--! qt:dataset:part
set hive.fetch.task.conversion=more;
set hive.cbo.fallback.strategy=NEVER;

--HIVE-2608 Do not require AS a,b,c part in LATERAL VIEW
EXPLAIN SELECT myTab.* from src LATERAL VIEW explode(map('key1', 100, 'key2', 200)) myTab limit 2;
SELECT myTab.* from src LATERAL VIEW explode(map('key1', 100, 'key2', 200)) myTab limit 2;

EXPLAIN SELECT explode(map('key1', 100, 'key2', 200)) from src limit 2;
SELECT explode(map('key1', 100, 'key2', 200)) from src limit 2;

-- view
create view lv_noalias as SELECT myTab.* from src LATERAL VIEW explode(map('key1', 100, 'key2', 200)) myTab limit 2;

explain select * from lv_noalias a join lv_noalias b on a.key=b.key;
select * from lv_noalias a join lv_noalias b on a.key=b.key;
