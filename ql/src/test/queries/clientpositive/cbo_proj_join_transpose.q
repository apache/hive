create table table1 (acct_num string, interest_rate decimal(10,7)) stored as orc;
create table table2 (act_id string) stored as orc;

explain cbo
CREATE TABLE temp_output AS
SELECT act_nbr, row_num
FROM (SELECT t2.act_id as act_nbr,
row_number() over (PARTITION BY trim(acct_num) ORDER BY interest_rate DESC) AS row_num
FROM table1 t1
INNER JOIN table2 t2
ON trim(acct_num) = t2.act_id) t
WHERE t.row_num = 1;

CREATE TABLE temp_output AS
SELECT act_nbr, row_num
FROM (SELECT t2.act_id as act_nbr,
row_number() over (PARTITION BY trim(acct_num) ORDER BY interest_rate DESC) AS row_num
FROM table1 t1
INNER JOIN table2 t2
ON trim(acct_num) = t2.act_id) t
WHERE t.row_num = 1;
