DESCRIBE FUNCTION date_add;
DESCRIBE FUNCTION EXTENDED date_add;

-- Test different numeric data types for date_add
SELECT date_add('1900-01-01', cast(10 as tinyint)),
       date_add('1900-01-01', cast(10 as smallint)),
       date_add('1900-01-01', cast(10 as int));