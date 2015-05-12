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
select count(1) from src where (case key when '238' then null else 1=1 end);
