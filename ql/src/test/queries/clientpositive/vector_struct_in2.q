set hive.fetch.task.conversion=none;

create table test (a string) partitioned by (y string, m string);
insert into test values ('aa', 2022, 9);

--=== original bug report, complex query ===============================================================================
select * from test where (y=year(date_sub('2022-09-11',4)) and m=month(date_sub('2022-09-11',4))) or (y=year(date_sub('2022-09-11',10)) and m=month(date_sub('2022-09-11',10)) );


--=== simple test cases for the distinct causes of the failure of the complex query ====================================

--this is needed not to optimize away the problematic parts of the queries
set hive.cbo.enable=false;

--embedded expression in struct - used to yield empty result
select * from test where (struct(cast(y as int)) IN (struct(2022)));

--first argument of in expression is const struct - used to yield empty result
select * from test where (struct(2022) IN (struct(2022)));

--these are needed not to optimize away the problematic part of the query
set hive.optimize.constant.propagation=false;
set hive.optimize.ppd=false;

--first argument of in expression is const primitive - used to cause error
select * from test where (2022 IN (2022));
