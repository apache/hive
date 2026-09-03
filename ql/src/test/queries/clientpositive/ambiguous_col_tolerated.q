select 'a' as c, 'b' as c;
select 'a' as c, 'b' as c order by c;
select * from (select 'a' as c, 'b' as c) t;
create table dup_alias_ins (x string, y string);
insert into dup_alias_ins select 'a' as c, 'b' as c;
select x, y from dup_alias_ins;
-- no non-CBO mirror for this one: CTE column lists are CBO-only (non-CBO ignores them)
with bse as (select 'a' as c, 'b' as c), renamed(a, b) as (select * from bse) select renamed.a from renamed;

set hive.cbo.enable=false;

select 'a' as c, 'b' as c;
select 'a' as c, 'b' as c order by c;
select * from (select 'a' as c, 'b' as c) t;
insert into dup_alias_ins select 'c' as c, 'd' as c;
select x, y from dup_alias_ins order by x;
