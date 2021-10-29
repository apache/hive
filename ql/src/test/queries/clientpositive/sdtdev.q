create table test ( col1 decimal(10,3) );
insert into test values (10230.72),(10230.72),(10230.72),(10230.72),(10230.72),(10230.72),(10230.72);
select STDDEV_SAMP(col1) AS STDDEV_6M , STDDEV(col1) as STDDEV ,STDDEV_POP(col1) as STDDEV_POP , variance(col1) as variance,var_pop(col1) as var_pop,var_samp(col1) as var_samp from test;

create table testpoint ( col1 decimal(10,3));
insert into testpoint values (0.12345678),(0.25362123),(0.62437485),(0.65133746),(0.98765432),(0.12435647),(0.7654321445);
select STDDEV_SAMP(col1) AS STDDEV_6M , STDDEV(col1) as STDDEV ,STDDEV_POP(col1) as STDDEV_POP , variance(col1) as variance,var_pop(col1) as var_pop,var_samp(col1) as var_samp from testpoint;

create table testint(col1 int);
insert into testint values (85),(86),(100),(76),(81),(93),(84),(99),(71),(69),(93),(85),(81),(87),(89);
select STDDEV_SAMP(col1) AS STDDEV_6M , STDDEV(col1) as STDDEV ,STDDEV_POP(col1) as STDDEV_POP, variance(col1) as variance,var_pop(col1) as var_pop,var_samp(col1) as var_samp from testint;

