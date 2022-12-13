--! qt:authorizer
set user.name=hive_admin_user;

-- admin required for create function
set role ADMIN;

set hive.security.authorization.manager=org.apache.hadoop.hive.ql.security.authorization.DefaultHiveAuthorizationProvider;
set hive.security.authorization.enabled=true;

drop function if exists udf_upper;

drop function if exists udf_lower;

drop table if exists base_table;

drop view if exists view_using_udf;

drop database if exists test;

create function udf_upper as 'org.apache.hadoop.hive.ql.udf.generic.GenericUDFUpper';

create table base_table(city string);

create view view_using_udf as select udf_upper(city) as upper_city from base_table;

select * from view_using_udf;

create function udf_lower as 'org.apache.hadoop.hive.ql.udf.generic.GenericUDFLower';

select udf_lower(upper_city) from view_using_udf;

create database test;

create function test.UDF_upper as 'org.apache.hadoop.hive.ql.udf.generic.GenericUDFUpper';

select test.UDF_Upper(upper_city) from view_using_udf;

set hive.security.authorization.functions.in.view=false;

select * from view_using_udf;

select udf_lower(upper_city) from view_using_udf;

select test.UDF_Upper(upper_city) from view_using_udf;

drop function test.UDF_Upper;

drop database test;

drop function udf_lower;

drop function udf_upper;

drop view view_using_udf;

drop table base_table;