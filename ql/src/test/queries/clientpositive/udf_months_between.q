--! qt:dataset:part
describe function months_between;
desc function extended months_between;

--test string format
explain select months_between('1995-02-02', '1995-01-01');

select
  months_between('1995-02-02', '1995-01-01'),
  months_between('2003-07-17', '2005-07-06'),
  months_between('2001-06-30', '2000-05-31'),
  months_between('2000-06-01', '2004-07-01'),
  months_between('2002-02-28', '2002-03-01'),
  months_between('2012-02-29', '2012-03-01'),
  months_between('1976-01-01 00:00:00', '1975-12-31 23:59:59'),
  months_between('1976-01-01', '1975-12-31 23:59:59'),
  months_between('1997-02-28 10:30:00', '1996-10-30'),
  -- if both are last day of the month then time part should be ignored
  months_between('2002-03-31', '2002-02-28'),
  months_between('2002-03-31', '2002-02-28 10:30:00'),
  months_between('2002-03-31 10:30:00', '2002-02-28'),
  -- if the same day of the month then time part should be ignored
  months_between('2002-03-24', '2002-02-24'),
  months_between('2002-03-24', '2002-02-24 10:30:00'),
  months_between('2002-03-24 10:30:00', '2002-02-24'),
  -- partial time. time part will be skipped
  months_between('1995-02-02 10:39', '1995-01-01'),
  months_between('1995-02-02', '1995-01-01 10:39'),
  -- no leading 0 for month and day should work
  months_between('1995-02-2', '1995-1-01'),
  months_between('1995-2-02', '1995-01-1'),
  -- short year should work
  months_between('495-2-02', '495-01-1'),
  months_between('95-2-02', '95-01-1'),
  months_between('5-2-02', '5-01-1');

--test timestamp format
select
  months_between(cast('1995-02-02 00:00:00' as timestamp), cast('1995-01-01 00:00:00' as timestamp)),
  months_between(cast('2003-07-17 00:00:00' as timestamp), cast('2005-07-06 00:00:00' as timestamp)),
  months_between(cast('2001-06-30 00:00:00' as timestamp), cast('2000-05-31 00:00:00' as timestamp)),
  months_between(cast('2000-06-01 00:00:00' as timestamp), cast('2004-07-01 00:00:00' as timestamp)),
  months_between(cast('2002-02-28 00:00:00' as timestamp), cast('2002-03-01 00:00:00' as timestamp)),
  months_between(cast('2012-02-29 00:00:00' as timestamp), cast('2012-03-01 00:00:00' as timestamp)),
  months_between(cast('1976-01-01 00:00:00' as timestamp), cast('1975-12-31 23:59:59' as timestamp)),
  months_between(cast('1976-01-01' as date), cast('1975-12-31 23:59:59' as timestamp)),
  months_between(cast('1997-02-28 10:30:00' as timestamp), cast('1996-10-30' as date)),
  -- if both are last day of the month then time part should be ignored
  months_between(cast('2002-03-31 00:00:00' as timestamp), cast('2002-02-28 00:00:00' as timestamp)),
  months_between(cast('2002-03-31 00:00:00' as timestamp), cast('2002-02-28 10:30:00' as timestamp)),
  months_between(cast('2002-03-31 10:30:00' as timestamp), cast('2002-02-28 00:00:00' as timestamp)),
  -- if the same day of the month then time part should be ignored
  months_between(cast('2002-03-24 00:00:00' as timestamp), cast('2002-02-24 00:00:00' as timestamp)),
  months_between(cast('2002-03-24 00:00:00' as timestamp), cast('2002-02-24 10:30:00' as timestamp)),
  months_between(cast('2002-03-24 10:30:00' as timestamp), cast('2002-02-24 00:00:00' as timestamp));

--test date format
select
  months_between(cast('1995-02-02' as date), cast('1995-01-01' as date)),
  months_between(cast('2003-07-17' as date), cast('2005-07-06' as date)),
  months_between(cast('2001-06-30' as date), cast('2000-05-31' as date)),
  months_between(cast('2000-06-01' as date), cast('2004-07-01' as date)),
  months_between(cast('2002-02-28' as date), cast('2002-03-01' as date)),
  months_between(cast('2012-02-29' as date), cast('2012-03-01' as date));


--test misc with null
select
  months_between(cast(null as string), '2012-03-01'),
  months_between(cast(null as timestamp), cast(null as date)),
  months_between(cast(null as string), cast('2012-03-01 00:00:00' as timestamp)),
  months_between(cast(null as timestamp), cast('2012-03-01' as string)),
  months_between('2012-02-10', cast(null as string)),
  months_between(cast(null as string), '2012-02-10'),
  months_between(cast(null as string), cast(null as string)),
  months_between('2012-02-10', cast(null as timestamp)),
  months_between(cast(null as timestamp), '2012-02-10'),
  months_between(cast(null as timestamp), cast(null as timestamp));

