set hive.compute.query.using.stats=true;

explain select count(key) from (select null as key from src)src;

select count(key) from (select null as key from src)src;
