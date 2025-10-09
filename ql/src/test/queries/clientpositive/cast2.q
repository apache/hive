
select cast('1' as tinyint), cast('1' as smallint), cast('1' as int), cast('1' as bigint), cast('1' as float), cast('1' as double), cast('1' as decimal(10,2));

-- Check that leading/trailing space is handled consistently for numeric types
select cast(' 1 ' as tinyint), cast(' 1 ' as smallint), cast(' 1 ' as int), cast(' 1 ' as bigint), cast(' 1 ' as float), cast(' 1 ' as double), cast(' 1 ' as decimal(10,2));

-- Decimal cast with spaces/without digits before dot & only dot.
select cast(".0000 " as decimal(8,4)), cast(" .0000" as decimal(8,4)), cast(" .0000  " as decimal(8,4)), cast("." as decimal(8,4)), cast(".  " as decimal(8,4)), cast("  .  " as decimal(8,4)), cast(".00 00 " as decimal(8,4));
