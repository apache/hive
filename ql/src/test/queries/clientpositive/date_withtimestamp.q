select "2016-12-29 23:59:59"  < cast("2016-12-30" as date);
select "2016-12-30 00:00:00"  = cast("2016-12-30" as date);
select "2016-12-31 00:00:01"  > cast("2016-12-30" as date);
