set hive.vectorized.testing.reducer.batch.size=2;

CREATE TABLE vector_ptf_lead_lag_int(name string, rowindex int, mynumber int) stored as orc;

INSERT INTO vector_ptf_lead_lag_int values
-- a partition
('first', 1, 1),
('first', 2, 2),
('first', 3, 2),
('first', 4, NULL),
('first', 5, 3),
('first', 6, 3),
('first', 7, 4),
('first', 8, NULL),
('first', 9, 4),
('first', 10, 4),
('first', 11, 5),
('first', 12, 5),
('first', 13, NULL),
('first', 14, 5),
('first', 15, 5),
('first', 16, 6),
('first', 17, 6),
('first', 18, 6),
('first', 19, NULL),
('first', 20, 6),
('first', 21, 6),
-- another partition
('second', 22, 1),
('second', 23, 2),
('second', 24, 2),
('second', 25, NULL),
('second', 26, 3),
('second', 27, 3),
('second', 28, 4),
('second', 29, NULL),
('second', 30, 4),
('second', 31, 4),
('second', 32, 5),
('second', 33, 5),
('second', 34, NULL),
('second', 35, 5),
('second', 36, 5),
('second', 37, 6),
('second', 38, 6),
('second', 39, 6),
('second', 40, NULL),
('second', 41, 6),
('second', 42, 6),
-- null partition
(NULL, 43, 7),
(NULL, 44, 7);

select "************ INT, NON-VECTORIZED ************";
set hive.vectorized.execution.ptf.enabled=false;
select name, rowindex, mynumber,
lag(mynumber) over (partition by name order by mynumber) as lag1,
lag(mynumber, 2) over (partition by name order by mynumber) as lag2,
lag(mynumber, 3, 100) over (partition by name order by mynumber) as lag3_default100,
lag(mynumber, 4, mynumber) over (partition by name order by mynumber) as lag4_default_col,
lead(mynumber) over (partition by name order by mynumber) as lead1,
lead(mynumber, 2) over (partition by name order by mynumber) as lead2,
lead(mynumber, 3, 100) over (partition by name order by mynumber) as lead3_default100,
lead(mynumber, 4, mynumber) over (partition by name order by mynumber) as lead4_default_col
from vector_ptf_lead_lag_int;

set hive.vectorized.execution.ptf.enabled=true;
explain vectorization detail select name, rowindex, mynumber,
lag(mynumber) over (partition by name order by mynumber) as lag1,
lag(mynumber, 2) over (partition by name order by mynumber) as lag2,
lag(mynumber, 3, 100) over (partition by name order by mynumber) as lag3_default100,
lag(mynumber, 4, mynumber) over (partition by name order by mynumber) as lag4_default_col,
lead(mynumber) over (partition by name order by mynumber) as lead1,
lead(mynumber, 2) over (partition by name order by mynumber) as lead2,
lead(mynumber, 3, 100) over (partition by name order by mynumber) as lead3_default100,
lead(mynumber, 4, mynumber) over (partition by name order by mynumber) as lead4_default_col
from vector_ptf_lead_lag_int;

select "************ INT, VECTORIZED ************";
select name, rowindex, mynumber,
lag(mynumber) over (partition by name order by mynumber) as lag1,
lag(mynumber, 2) over (partition by name order by mynumber) as lag2,
lag(mynumber, 3, 100) over (partition by name order by mynumber) as lag3_default100,
lag(mynumber, 4, mynumber) over (partition by name order by mynumber) as lag4_default_col,
lead(mynumber) over (partition by name order by mynumber) as lead1,
lead(mynumber, 2) over (partition by name order by mynumber) as lead2,
lead(mynumber, 3, 100) over (partition by name order by mynumber) as lead3_default100,
lead(mynumber, 4, mynumber) over (partition by name order by mynumber) as lead4_default_col
from vector_ptf_lead_lag_int;




CREATE TABLE vector_ptf_lead_lag_double(name string, rowindex int, mynumber double) stored as orc;
INSERT INTO vector_ptf_lead_lag_double SELECT * from vector_ptf_lead_lag_int;

select "************ DOUBLE, NON-VECTORIZED ************";
set hive.vectorized.execution.ptf.enabled=false;
select name, rowindex, mynumber,
lag(mynumber) over (partition by name order by mynumber) as lag1,
lag(mynumber, 2) over (partition by name order by mynumber) as lag2,
lag(mynumber, 3, 100) over (partition by name order by mynumber) as lag3_default100,
lag(mynumber, 4, mynumber) over (partition by name order by mynumber) as lag4_default_col,
lead(mynumber) over (partition by name order by mynumber) as lead1,
lead(mynumber, 2) over (partition by name order by mynumber) as lead2,
lead(mynumber, 3, 100) over (partition by name order by mynumber) as lead3_default100,
lead(mynumber, 4, mynumber) over (partition by name order by mynumber) as lead4_default_col
from vector_ptf_lead_lag_double;

set hive.vectorized.execution.ptf.enabled=true;
explain vectorization detail select name, rowindex, mynumber,
lag(mynumber) over (partition by name order by mynumber) as lag1,
lag(mynumber, 2) over (partition by name order by mynumber) as lag2,
lag(mynumber, 3, 100) over (partition by name order by mynumber) as lag3_default100,
lag(mynumber, 4, mynumber) over (partition by name order by mynumber) as lag4_default_col,
lead(mynumber) over (partition by name order by mynumber) as lead1,
lead(mynumber, 2) over (partition by name order by mynumber) as lead2,
lead(mynumber, 3, 100) over (partition by name order by mynumber) as lead3_default100,
lead(mynumber, 4, mynumber) over (partition by name order by mynumber) as lead4_default_col
from vector_ptf_lead_lag_double;

select "************ DOUBLE, VECTORIZED ************";
select name, rowindex, mynumber,
lag(mynumber) over (partition by name order by mynumber) as lag1,
lag(mynumber, 2) over (partition by name order by mynumber) as lag2,
lag(mynumber, 3, 100) over (partition by name order by mynumber) as lag3_default100,
lag(mynumber, 4, mynumber) over (partition by name order by mynumber) as lag4_default_col,
lead(mynumber) over (partition by name order by mynumber) as lead1,
lead(mynumber, 2) over (partition by name order by mynumber) as lead2,
lead(mynumber, 3, 100) over (partition by name order by mynumber) as lead3_default100,
lead(mynumber, 4, mynumber) over (partition by name order by mynumber) as lead4_default_col
from vector_ptf_lead_lag_double;




CREATE TABLE vector_ptf_lead_lag_decimal(name string, rowindex int, mynumber decimal) stored as orc;
INSERT INTO vector_ptf_lead_lag_decimal SELECT * from vector_ptf_lead_lag_int;

select "************ DECIMAL, NON-VECTORIZED ************";
set hive.vectorized.execution.ptf.enabled=false;
select name, rowindex, mynumber,
lag(mynumber) over (partition by name order by mynumber) as lag1,
lag(mynumber, 2) over (partition by name order by mynumber) as lag2,
lag(mynumber, 3, 100) over (partition by name order by mynumber) as lag3_default100,
lag(mynumber, 4, mynumber) over (partition by name order by mynumber) as lag4_default_col,
lead(mynumber) over (partition by name order by mynumber) as lead1,
lead(mynumber, 2) over (partition by name order by mynumber) as lead2,
lead(mynumber, 3, 100) over (partition by name order by mynumber) as lead3_default100,
lead(mynumber, 4, mynumber) over (partition by name order by mynumber) as lead4_default_col
from vector_ptf_lead_lag_decimal;

set hive.vectorized.execution.ptf.enabled=true;
explain vectorization detail select name, rowindex, mynumber,
lag(mynumber) over (partition by name order by mynumber) as lag1,
lag(mynumber, 2) over (partition by name order by mynumber) as lag2,
lag(mynumber, 3, 100) over (partition by name order by mynumber) as lag3_default100,
lag(mynumber, 4, mynumber) over (partition by name order by mynumber) as lag4_default_col,
lead(mynumber) over (partition by name order by mynumber) as lead1,
lead(mynumber, 2) over (partition by name order by mynumber) as lead2,
lead(mynumber, 3, 100) over (partition by name order by mynumber) as lead3_default100,
lead(mynumber, 4, mynumber) over (partition by name order by mynumber) as lead4_default_col
from vector_ptf_lead_lag_decimal;

select "************ DECIMAL, VECTORIZED ************";
select name, rowindex, mynumber,
lag(mynumber) over (partition by name order by mynumber) as lag1,
lag(mynumber, 2) over (partition by name order by mynumber) as lag2,
lag(mynumber, 3, 100) over (partition by name order by mynumber) as lag3_default100,
lag(mynumber, 4, mynumber) over (partition by name order by mynumber) as lag4_default_col,
lead(mynumber) over (partition by name order by mynumber) as lead1,
lead(mynumber, 2) over (partition by name order by mynumber) as lead2,
lead(mynumber, 3, 100) over (partition by name order by mynumber) as lead3_default100,
lead(mynumber, 4, mynumber) over (partition by name order by mynumber) as lead4_default_col
from vector_ptf_lead_lag_decimal;
