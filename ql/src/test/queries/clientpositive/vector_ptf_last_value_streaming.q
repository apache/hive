set hive.query.results.cache.enabled=false;
create temporary table test2(id STRING,name STRING,event_dt date) stored as orc;

insert into test2 values ('100','A','2019-08-15'), ('100','A','2019-10-12');

explain vectorization detail SELECT name, event_dt, last_value(event_dt) over (PARTITION BY name ORDER BY event_dt desc ROWS BETWEEN unbounded preceding and unbounded following) last_event_dt FROM test2;
SELECT name, event_dt, last_value(event_dt) over (PARTITION BY name ORDER BY event_dt desc ROWS BETWEEN unbounded preceding and unbounded following) last_event_dt FROM test2;
SELECT name, event_dt, last_value(event_dt) over (PARTITION BY name ORDER BY event_dt desc ROWS BETWEEN unbounded preceding and current row) last_event_dt FROM test2;
SELECT name, event_dt, first_value(event_dt) over (PARTITION BY name ORDER BY event_dt asc ROWS BETWEEN unbounded preceding and current row) last_event_dt FROM test2;
