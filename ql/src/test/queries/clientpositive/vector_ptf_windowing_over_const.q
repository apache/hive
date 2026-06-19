create table vectptf(age int, name string) stored as orc;
insert into vectptf values(20, 'PQR'), (10, 'ABC'), (30, 'XYZ');

select cast(row_number() over(order by NULL) as STRING), age, name from vectptf;
select cast(rank() over(order by INTERVAL '1' DAY) as STRING), age, name from vectptf;

set hive.vectorized.execution.ptf.enabled=false;

select cast(row_number() over(order by NULL) as STRING), age, name from vectptf;
select cast(rank() over(order by INTERVAL '1' DAY) as STRING), age, name from vectptf;
