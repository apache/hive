create table ctas1 
location 'file:${system:test.tmp.dir}/ctastmpfolder' 
as 
select * from src limit 3;

create table ctas2
location 'file:${system:test.tmp.dir}/ctastmpfolder'
as 
select * from src limit 2;

