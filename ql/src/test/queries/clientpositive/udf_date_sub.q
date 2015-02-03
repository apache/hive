DESCRIBE FUNCTION date_sub;
DESCRIBE FUNCTION EXTENDED date_sub;

-- Test different numeric data types for date_add
SELECT date_sub('1900-01-01', cast(10 as tinyint)),
       date_sub('1900-01-01', cast(10 as smallint)),
       date_sub('1900-01-01', cast(10 as int));