-- SORT_QUERY_RESULTS

use default;
-- Test sort_array_by() UDF

DESCRIBE FUNCTION sort_array_by;
DESCRIBE FUNCTION EXTENDED sort_array_by;

DROP TABLE IF EXISTS sort_array_by_table;

CREATE TABLE sort_array_by_table
STORED AS TEXTFILE
AS
SELECT "Google" as company,
        array(
        named_struct('empId',800 ,'name','Able' ,'age',28 ,'salary',80000),
        named_struct('empId',756 ,'name','Able' ,'age',23 ,'salary',76889),
        named_struct('empId',100 ,'name','Boo' ,'age',21 ,'salary',70000),
        named_struct('empId',130 ,'name','Boo' ,'age',22 ,'salary',79000),
        named_struct('empId',900 ,'name','Hary' ,'age',21 ,'salary',50000),
        named_struct('empId',76 ,'name','Hary' ,'age',87 ,'salary',10000)
        ) as employee,
        "IN" as country
UNION ALL
SELECT "Facebook" as company,
        array(
        named_struct('empId',200 ,'name','Keiko' ,'age',28 ,'salary',80000),
        named_struct('empId',206 ,'name','Keiko' ,'age',41 ,'salary',80500),
        named_struct('empId',390 ,'name','Ben' ,'age',21 ,'salary',70000),
        named_struct('empId',310 ,'name','Ben' ,'age',31 ,'salary',21000),
        named_struct('empId',700 ,'name','Aron' ,'age',21 ,'salary',50000),
        named_struct('empId',320 ,'name','Aron' ,'age',18 ,'salary',70000)
        ) as employee,
        "US" as country
UNION ALL
SELECT "Microsoft" as company,
        array(
        named_struct('empId',900 ,'name','Spiro' ,'age',28 ,'salary',80000),
        named_struct('empId',300 ,'name','Spiro' ,'age',38 ,'salary',80300),
        named_struct('empId',600 ,'name','James' ,'age',21 ,'salary',70000),
        named_struct('empId',313 ,'name','James' ,'age',11 ,'salary',30000),
        named_struct('empId',260 ,'name','Eden' ,'age',31 ,'salary',50020),
        named_struct('empId',730 ,'name','Eden' ,'age',45 ,'salary',20300)
        ) as employee,
        "UK" as country
;


--Sort tuple array by field name:salary(single column) by default ascending order
select company,country,sort_array_by(employee,'salary') as single_field_sort from sort_array_by_table;

--Sort tuple array by field name:salary(single column) by ascending order
select company,country,sort_array_by(employee,'salary','ASC') as single_field_sort from sort_array_by_table;

--Sort tuple array by field name:salary(single column) by descending order
select company,country,sort_array_by(employee,'salary','desc') as single_field_sort from sort_array_by_table;

--Above three in one query
select company,country,
sort_array_by(employee,'salary') as single_field_sort,
sort_array_by(employee,'salary','ASC') as single_field_sort_asc,
sort_array_by(employee,'salary','DESC') as single_field_sort_desc
from sort_array_by_table;


--Sort tuple array by field names : name,salary(multiple columns) by default ascending order
select company,country,sort_array_by(employee,'name','salary') as multiple_field_sort from sort_array_by_table;

--Sort tuple array by field names : name,salary(multiple columns) ascending order
select company,country,sort_array_by(employee,'name','salary','asc') as multiple_field_sort from sort_array_by_table;

--Sort tuple array by field names : name,salary(multiple columns) descending order
select company,country,sort_array_by(employee,'name',"salary","DESC") as multiple_field_sort from sort_array_by_table;


--Above three in one query
select company,country,
sort_array_by(employee,'name','salary') as multiple_field_sort,
sort_array_by(employee,'name','salary','ASC') as multiple_field_sort_asc,
sort_array_by(employee,'name',"salary","DESC") as multiple_field_sort_desc
from sort_array_by_table;


-- Test for order name ('ASC' and 'DESC') as tuple field names and and order name
DROP TABLE IF EXISTS sort_array_by_order_name;

CREATE TABLE sort_array_by_order_name
STORED AS TEXTFILE
AS
SELECT "Google" as company,
        array(
        named_struct('asc','Able' ,'DESC','Keiko','salary',28),
        named_struct('asc','Boo' ,'DESC','Aron','salary',70000),
        named_struct('asc','Hary' ,'DESC','James' ,'salary',50000)
        ) as employee  ;

-- select asc,desc as filed name with default sorting
select
company,
sort_array_by(employee,'asc') as col1,
sort_array_by(employee,'DESC') as col2
from sort_array_by_order_name ;

--select asc,desc as field name and explicitly provided sorting ordering.
--If argument length's size are more than two (first: tuple list,second: desired minimum a field name)
--then we always check whether the last argument is any sorting order name(ASC or DESC)
select
company,
sort_array_by(employee,'asc','ASC') as col1,
sort_array_by(employee,'DESC','desc') as col2
from
sort_array_by_order_name ;

-- similarity of sorting order check between this UDF and LATERAL VIEW explode(array).

DROP TABLE IF EXISTS sort_array_by_table_order;

CREATE TABLE sort_array_by_table_order
STORED AS TEXTFILE
AS
SELECT  array(
        named_struct('name','Able' ,'age',28),
        named_struct('name','Able' ,'age',23),
        named_struct('name','Boo' ,'age',21),
        named_struct('name','Boo' ,'age',22),
        named_struct('name','Hary' ,'age',21),
        named_struct('name','Hary' ,'age',87)
        ) as a_struct_array
;

SELECT a_struct FROM sort_array_by_table_order LATERAL VIEW explode(a_struct_array) structTable AS a_struct ORDER BY a_struct.name DESC;

SELECT a_struct FROM sort_array_by_table_order LATERAL VIEW explode(sort_array_by(a_struct_array, 'name', 'DESC')) structTable AS a_struct;

SELECT a_struct FROM sort_array_by_table_order LATERAL VIEW explode(a_struct_array) structTable AS a_struct ORDER BY a_struct.name DESC,a_struct.age DESC ;

SELECT a_struct FROM sort_array_by_table_order LATERAL VIEW explode(sort_array_by(a_struct_array, 'name','age', 'DESC')) structTable AS a_struct ;

