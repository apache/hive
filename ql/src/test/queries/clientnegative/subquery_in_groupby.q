--! qt:dataset:src


select count(*) 
from src 
group by src.key in (select key from src s1 where s1.key > '9')