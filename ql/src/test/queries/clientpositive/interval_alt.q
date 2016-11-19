select
	interval 1 second,
	interval 2  seconds,
        interval 1 minute,
	interval 2  minutes,
	interval 1 hour,
	interval 2  hours,
	interval 1 day,
	interval 2  days,
	interval 1 month,
	interval 2  months,
	interval 1 year,
	interval 2  years;

select date '2012-01-01' + interval 30 days;
select date '2012-01-01' - interval 30 days;

create table t (dt int);
insert into t values (1),(2);

-- expressions/columnref
explain
select
	date '2012-01-01' + interval 1 day + interval '2' days
	from t;

select
        date '2012-01-01' + interval 1 day + interval '2' days
        from t;
