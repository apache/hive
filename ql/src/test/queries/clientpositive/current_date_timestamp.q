--! qt:dataset:srcpart
--! qt:dataset:src
--! qt:dataset:alltypesorc
select current_timestamp = current_timestamp(), current_date = current_date() from src limit 5;

set hive.test.currenttimestamp =2012-01-01 01:02:03;

explain cbo select current_timestamp() from alltypesorc;

--ensure that timestamp is same for all the rows while using current_timestamp() query should return single row
select count(*) from (select current_timestamp() from alltypesorc union select current_timestamp() from src limit 5 ) subq;

select count(*) from (select current_timestamp() from alltypesorc
                        union
                      select current_timestamp() from src
                      limit 5 ) subqr;

--current_timestamp() should appear as expression
explain extended select current_timestamp() from alltypesorc;

--current_timestamp() + insert
create temporary table tmp_runtimeconstant(
                      ts1 timestamp,
                      ts2 timestamp,
                      dt date,
                      s string,
                      v varchar(50),
                      c char(50)
                    );
insert into table tmp_runtimeconstant
                      select current_timestamp(),
                             cast(current_timestamp() as timestamp),
                             cast(current_timestamp() as date),
                             cast(current_timestamp() as string),
                             cast(current_timestamp() as varchar(50)),
                             cast(current_timestamp() as char(50))
                      from alltypesorc limit 5;
select ts1 = ts2,
        to_date(ts2) = dt,
        s = v,
        v = c
from tmp_runtimeconstant;

--current_date() + insert
drop table if exists tmp_runtimeconstant;
create temporary table tmp_runtimeconstant(d date, t timestamp);
insert into table tmp_runtimeconstant
                      select current_date(), current_timestamp() from alltypesorc limit 5;
select to_date(t)=d from tmp_runtimeconstant;

--current_timestamp() + current_date() + where
drop table if exists tmp_runtimeconstant;
create temporary table tmp_runtimeconstant(t timestamp, d date);
insert into table tmp_runtimeconstant
    select current_timestamp(), current_date() from alltypesorc limit 5;
select count(*) from tmp_runtimeconstant
                      where current_timestamp() >= t
                      and current_date <> d;


--current_timestamp() as argument for unix_timestamp(), hour(), minute(), second()
select unix_timestamp(current_timestamp()),
                           hour(current_timestamp()),
                           minute(current_timestamp()),
                           second(current_timestamp())
                    from alltypesorc limit 5;

--current_timestamp() as argument for various date udfs
select to_date(current_timestamp()),
                           year(current_timestamp()),
                           month(current_timestamp()),
                           day(current_timestamp()),
                           weekofyear(current_timestamp()),
                           datediff(current_timestamp(),current_timestamp),
                           to_date(date_add(current_timestamp(), 31)),
                           to_date(date_sub(current_timestamp(), 31)),
                           last_day(current_timestamp()),
                           next_day(current_timestamp(),'FRIDAY')
                    from alltypesorc limit 5;

--current_date() as argument for various date udfs
select to_date(current_date()),
                           year(current_date()),
                           month(current_date()),
                           day(current_date()),
                           weekofyear(current_date()),
                           datediff(current_date(),current_date),
                           to_date(date_add(current_date(), 31)),
                           to_date(date_sub(current_date(), 31)),
                           last_day(current_date()),
                           next_day(current_date(),'FRIDAY')
                    from alltypesorc limit 5;

select current_timestamp() - current_timestamp(),
       current_timestamp() - current_date(),
      current_date() - current_timestamp(),
      current_date() - current_date()
                    from alltypesorc limit 1;

select ctimestamp1 - current_date(),
        ctimestamp1- ctimestamp2,
        current_date() - current_date(),
        current_date() - ctimestamp2
from alltypesorc
where ctimestamp1 is not null
            and ctimestamp2 is not null
            limit 5;
select current_date, current_timestamp from src limit 5;

set hive.support.quoted.identifiers=none;
select `[kv]+.+` from srcpart order by key;
