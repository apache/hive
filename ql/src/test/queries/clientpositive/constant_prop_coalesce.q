explain
select * from (
select
	case when b.a=1
		then
			cast (from_unixtime(unix_timestamp(cast(20210309 as string),'yyyyMMdd') - 86400,'yyyyMMdd') as bigint)
		else
			20210309
	   end
as col
from
(select stack(2,1,2) as (a))
 as b
) t
where t.col is not null;

select * from (
select
	case when b.a=1
		then
			cast (from_unixtime(unix_timestamp(cast(20210309 as string),'yyyyMMdd') - 86400,'yyyyMMdd') as bigint)
		else
			20210309
	   end
as col
from
(select stack(2,1,2) as (a))
 as b
) t
where t.col is not null;