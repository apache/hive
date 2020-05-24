--! qt:dataset:src

CREATE FUNCTION qtest_get_java_boolean AS 'org.apache.hadoop.hive.ql.udf.generic.GenericUDFTestGetJavaBoolean';
select qtest_get_java_boolean('true'), qtest_get_java_boolean('false') from src limit 1;

describe function extended qtest_get_java_boolean;

create database mydb;
explain create function mydb.func1 as 'org.apache.hadoop.hive.ql.udf.generic.GenericUDFUpper';
create function mydb.func1 as 'org.apache.hadoop.hive.ql.udf.generic.GenericUDFUpper';

show functions like mydb.func1;

describe function extended mydb.func1;


select mydb.func1('abc') from src limit 1;

explain drop function mydb.func1;
drop function mydb.func1;

-- function should now be gone
show functions like mydb.func1;

explain create temporary function temp_func1 as 'org.apache.hadoop.hive.ql.udf.generic.GenericUDFUpper';
create temporary function temp_func1 as 'org.apache.hadoop.hive.ql.udf.generic.GenericUDFUpper';

explain drop temporary function temp_func1;
drop temporary function temp_func1;

explain reload functions;
reload functions;

-- old format, still supported due to backward compatibility
explain reload function;
reload function;

-- To test function name resolution
create function mydb.qtest_get_java_boolean as 'org.apache.hadoop.hive.ql.udf.generic.GenericUDFUpper';

use default;
-- unqualified function should resolve to one in default db
select qtest_get_java_boolean('abc'), default.qtest_get_java_boolean('abc'), mydb.qtest_get_java_boolean('abc') from default.src limit 1;

use mydb;
-- unqualified function should resolve to one in mydb db
select qtest_get_java_boolean('abc'), default.qtest_get_java_boolean('abc'), mydb.qtest_get_java_boolean('abc') from default.src limit 1;

drop function default.qtest_get_java_boolean;
drop function mydb.qtest_get_java_boolean;

drop database mydb cascade;
