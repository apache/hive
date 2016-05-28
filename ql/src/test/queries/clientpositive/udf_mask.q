DESCRIBE FUNCTION mask;
DESC FUNCTION EXTENDED mask;

explain select mask('TestString-123', 'X', 'x', '0', '1');

select mask('TestString-123', 'X', 'x', '0', ':'),
       mask(cast('TestString-123' as varchar(24)), 'X', 'x', '0', ':'),
       mask(cast('TestString-123' as char(24)), 'X', 'x', '0', ':'),
       mask(cast(123 as tinyint), -1, -1, -1, -1, '5'),
       mask(cast(12345 as smallint), -1, -1, -1, -1, '5'),
       mask(cast(12345 as int), -1, -1, -1, -1, '5'),
       mask(cast(12345 as bigint), -1, -1, -1, -1, '5'),
       mask(cast('2016-04-20' as date), -1, -1, -1, -1, -1, 0, 0, 0),
       mask(cast('2016-04-20' as date), -1, -1, -1, -1, -1, -1, 0, 0),
       mask(cast('2016-04-20' as date), -1, -1, -1, -1, -1, 1, -1, 0),
       mask(cast('2016-04-20' as date), -1, -1, -1, -1, -1, 1, 0, -1),
       mask(cast('2016-04-20' as date), -1, -1, -1, -1, -1, 1, -1, -1),
       mask(cast('2016-04-20' as date), -1, -1, -1, -1, -1, -1, 0, -1),
       mask(cast('2016-04-20' as date), -1, -1, -1, -1, -1, -1, -1, 0),
       mask(cast('2016-04-20' as date), -1, -1, -1, -1, -1, -1, -1, -1);
