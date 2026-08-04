select 'a' as c, 'b' as c;
select 'a' as c, 'b' as c order by c;
select * from (select 'a' as c, 'b' as c) t;
create table dup_alias_ins (x string, y string);
insert into dup_alias_ins select 'a' as c, 'b' as c;
select x, y from dup_alias_ins;

set hive.cbo.enable=false;

select 'a' as c, 'b' as c;
select 'a' as c, 'b' as c order by c;
select * from (select 'a' as c, 'b' as c) t;
insert into dup_alias_ins select 'c' as c, 'd' as c;
select x, y from dup_alias_ins order by x;
