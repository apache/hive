--! qt:dataset:src
set hive.fetch.task.conversion=more;

-- Round to avoid decimal precision difference due to JDK-4477961
explain
select round(radians(57.2958), 15) FROM src tablesample (1 rows);

select round(radians(57.2958), 15) FROM src tablesample (1 rows);
select radians(143.2394) FROM src tablesample (1 rows);

DESCRIBE FUNCTION radians;
DESCRIBE FUNCTION EXTENDED radians;
explain 
select round(radians(57.2958), 15) FROM src tablesample (1 rows);

select round(radians(57.2958), 15) FROM src tablesample (1 rows);
select radians(143.2394) FROM src tablesample (1 rows);

DESCRIBE FUNCTION radians;
DESCRIBE FUNCTION EXTENDED radians;