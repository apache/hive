--! qt:dataset:alltypesorc

set hive.test.currenttimestamp =2024-01-18 01:02:03;

-- desc
desc function yesterday;
desc function extended yesterday;

-- query
select
	yesterday() - yesterday(),
	yesterday()
from alltypesorc limit 5;

select yesterday();

-- as argument for various date udfs
select 
	to_date(yesterday()),
	year(yesterday()),
	month(yesterday()),
	day(yesterday()),
	weekofyear(yesterday()),
	datediff(yesterday(), yesterday()),
	to_date(date_add(yesterday(), 31)),
	to_date(date_sub(yesterday(), 31)),
	last_day(yesterday()),
	next_day(yesterday(),'FRIDAY')
from alltypesorc limit 5;

-- insert 
drop table if exists tmp_runtimeconstant;
create temporary table tmp_runtimeconstant(d date);
insert into table tmp_runtimeconstant
select yesterday() from alltypesorc limit 5;
select 
	d = date_sub(current_date, 1),
	d = date_add(current_date, -1),
	d = date_sub(from_unixtime(unix_timestamp()), 1),
	d = date_add(from_unixtime(unix_timestamp()), -1),
	d = from_unixtime(unix_timestamp() - 1 * 24 * 60 * 60, 'yyyy-MM-dd'),
	d = date_sub(current_timestamp, 1),
	d = date_add(current_timestamp, -1)
from tmp_runtimeconstant;

-- where
select 
	count(*) as cnt
from tmp_runtimeconstant
where yesterday() != d;
