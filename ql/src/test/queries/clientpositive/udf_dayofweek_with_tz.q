--! qt:timezone:Asia/Shanghai

CREATE EXTERNAL TABLE dayOfWeek_test(
`fund_code` string,
`test_date` string
);

INSERT INTO dayOfWeek_test(fund_code,test_date)
values('SEC016210079','2023-04-13');

set hive.vectorized.execution.enabled=false;

SELECT fund_code,
 test_date,
 dayofweek(test_date) AS SR,
 CASE
     WHEN dayofweek(test_date) = 1 THEN 7
     ELSE dayofweek(test_date) - 1
 END AS week_day
FROM dayOfWeek_test; 

set hive.vectorized.execution.enabled=true;

SELECT fund_code,
 test_date,
 dayofweek(test_date) AS SR,
 CASE
     WHEN dayofweek(test_date) = 1 THEN 7
     ELSE dayofweek(test_date) - 1
 END AS week_day
FROM dayOfWeek_test; 