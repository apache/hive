DESCRIBE FUNCTION quarter;
DESC FUNCTION EXTENDED quarter;

explain select quarter('2015-04-24');

-- string date
select
quarter('2014-01-10'),
quarter('2014-02-10'),
quarter('2014-03-31'),
quarter('2014-04-02'),
quarter('2014-05-28'),
quarter('2016-06-03'),
quarter('2016-07-28'),
quarter('2016-08-29'),
quarter('2016-09-29'),
quarter('2016-10-29'),
quarter('2016-11-29'),
quarter('2016-12-29'),
-- null string
quarter(cast(null as string)),
-- negative Unix time
quarter('1966-01-01'),
quarter('1966-03-31'),
quarter('1966-04-01'),
quarter('1966-12-31');

-- string timestamp
select
quarter('2014-01-10 00:00:00'),
quarter('2014-02-10 15:23:00'),
quarter('2014-03-31 15:23:00'),
quarter('2014-04-02 15:23:00'),
quarter('2014-05-28 15:23:00'),
quarter('2016-06-03 15:23:00'),
quarter('2016-07-28 15:23:00'),
quarter('2016-08-29 15:23:00'),
quarter('2016-09-29 15:23:00'),
quarter('2016-10-29 15:23:00'),
quarter('2016-11-29 15:23:00'),
quarter('2016-12-29 15:23:00'),
-- null VOID type
quarter(null),
-- negative Unix time
quarter('1966-01-01 00:00:00'),
quarter('1966-03-31 23:59:59.999'),
quarter('1966-04-01 00:00:00'),
quarter('1966-12-31 23:59:59.999');

-- date
select
quarter(cast('2014-01-10' as date)),
quarter(cast('2014-02-10' as date)),
quarter(cast('2014-03-31' as date)),
quarter(cast('2014-04-02' as date)),
quarter(cast('2014-05-28' as date)),
quarter(cast('2016-06-03' as date)),
quarter(cast('2016-07-28' as date)),
quarter(cast('2016-08-29' as date)),
quarter(cast('2016-09-29' as date)),
quarter(cast('2016-10-29' as date)),
quarter(cast('2016-11-29' as date)),
quarter(cast('2016-12-29' as date)),
-- null date
quarter(cast(null as date)),
-- negative Unix time
quarter(cast('1966-01-01' as date)),
quarter(cast('1966-03-31' as date)),
quarter(cast('1966-04-01' as date)),
quarter(cast('1966-12-31' as date));

-- timestamp
select
quarter(cast('2014-01-10 00:00:00' as timestamp)),
quarter(cast('2014-02-10 15:23:00' as timestamp)),
quarter(cast('2014-03-31 15:23:00' as timestamp)),
quarter(cast('2014-04-02 15:23:00' as timestamp)),
quarter(cast('2014-05-28 15:23:00' as timestamp)),
quarter(cast('2016-06-03 15:23:00' as timestamp)),
quarter(cast('2016-07-28 15:23:00' as timestamp)),
quarter(cast('2016-08-29 15:23:00' as timestamp)),
quarter(cast('2016-09-29 15:23:00' as timestamp)),
quarter(cast('2016-10-29 15:23:00' as timestamp)),
quarter(cast('2016-11-29 15:23:00' as timestamp)),
quarter(cast('2016-12-29 15:23:00' as timestamp)),
-- null timestamp
quarter(cast(null as timestamp)),
-- negative Unix time
quarter(cast('1966-01-01 00:00:00' as timestamp)),
quarter(cast('1966-03-31 23:59:59.999' as timestamp)),
quarter(cast('1966-04-01 00:00:00' as timestamp)),
quarter(cast('1966-12-31 23:59:59.999' as timestamp));
