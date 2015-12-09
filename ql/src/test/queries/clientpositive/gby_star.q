set hive.mapred.mode=nonstrict;
explain
select *, sum(key) from src group by key, value limit 10;
select *, sum(key) from src group by key, value limit 10;

explain
select *, sum(key) from src where key < 100 group by key, value limit 10;
select *, sum(key) from src where key < 100 group by key, value limit 10;

explain
select *, sum(key) from (select key from src where key < 100) a group by key limit 10;
select *, sum(key) from (select key from src where key < 100) a group by key limit 10;

explain
select a.*, sum(src.key) from (select key from src where key < 100) a 
inner join src on a.key = src.key group by a.key limit 10;
select a.*, sum(src.key) from (select key from src where key < 100) a 
inner join src on a.key = src.key group by a.key limit 10;
