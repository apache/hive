explain
select count(1) from src where (case key when '238' then true else false end);
explain 
select count(1) from src where (case key when '238' then 1=2 else 1=1 end);
explain 
select count(1) from src where (case key when '238' then 1=2 else 1=31 end);
explain 
select count(1) from src where (case key when '238' then true else 1=1 end);
explain
select count(1) from src where (case key when '238' then 1=1 else 1=null end);
explain 
select count(1) from src where (case key when '238' then 1=null  end);
explain 
select count(1) from src where (case key when '238' then 2 = cast('2' as bigint) end);
explain 
select (case key when '238' then null else false end) from src where (case key when '238' then 2 = cast('1' as bigint)  else true end);
explain 
select (case key when '238' then null else null end) from src where (case key when '238' then 2 = null else 3 = null  end);
explain 
select count(1) from src where (case key when '238' then null else 1=1 end);
explain 
select (CASE WHEN (-2) >= 0  THEN SUBSTRING(key, 1,CAST((-2) AS INT)) ELSE NULL END)
from src;
explain
select (CASE WHEN key = value THEN '1' WHEN true THEN '0' ELSE NULL END)
from src;
