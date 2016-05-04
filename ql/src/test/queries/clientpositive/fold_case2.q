drop table test1; 
drop table test2;

create table test1 (id int, desc string) stored as orc; 
create table test2 (id int, desc string) stored as orc;

select 
case 
when (case when a.desc='test' then 1 else 0 end)=0 then 'test' 
else null 
end as val 
FROM test1 a 
JOIN test2 b ON a.id=b.id;
