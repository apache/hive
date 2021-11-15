DESCRIBE FUNCTION mask_show_last_n;
DESC FUNCTION EXTENDED mask_show_last_n;

explain select mask_show_last_n('TestString-123', 4, 'X', 'x', '0', '1');

select mask_show_last_n('TestString-123', 4, 'X', 'x', '0', ':'),
       mask_show_last_n(cast('TestString-123' as varchar(24)), 4, 'X', 'x', '0', ':'),
       mask_show_last_n(cast('TestString-123' as char(24)), 4, 'X', 'x', '0', ':'),
       mask_show_last_n(0),
       mask_show_last_n(0, 0),
       mask_show_last_n(0, 0, -1, -1, -1, -1, '5'),
       mask_show_last_n(cast(123 as tinyint), 4, -1, -1, -1, -1, '5'),
       mask_show_last_n(cast(12345 as smallint), 4, -1, -1, -1, -1, '5'),
       mask_show_last_n(cast(12345 as int), 4, -1, -1, -1, -1, '5'),
       mask_show_last_n(cast(12345 as bigint), 4, -1, -1, -1, -1, '5'),
       mask_show_last_n(cast('2016-04-20' as date), 4, -1, -1, -1, -1, -1, 0, 0, 0);
