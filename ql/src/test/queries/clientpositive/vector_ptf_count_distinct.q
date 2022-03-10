CREATE TABLE ptf_count_distinct (
  id int,
  txt1 string,
  txt2 string);

INSERT INTO ptf_count_distinct VALUES
  (1,'2010005759','7164335675012038'),
  (2,'2010005759','7164335675012038');

SELECT "*** Testing STRING, same values ***";
set hive.vectorized.execution.ptf.enabled=true;
SELECT "*** Distinct on partitioning column, vectorized ptf: expecting 1 in aggregations ***";
explain SELECT
    txt1,
    txt2,
    count(distinct txt1) over(partition by txt1) as n,
    count(distinct txt2) over(partition by txt2) as m
FROM ptf_count_distinct;

EXPLAIN VECTORIZATION DETAIL SELECT
    txt1,
    txt2,
    count(distinct txt1) over(partition by txt1) as n,
    count(distinct txt2) over(partition by txt2) as m
FROM ptf_count_distinct;

SELECT
    txt1,
    txt2,
    count(distinct txt1) over(partition by txt1) as n,
    count(distinct txt2) over(partition by txt2) as m
FROM ptf_count_distinct;

set hive.vectorized.execution.ptf.enabled=false;
SELECT "*** Distinct on partitioning column, non-vectorized ptf: expecting 1 in aggregations ***";
EXPLAIN SELECT
    txt1,
    txt2,
    count(distinct txt1) over(partition by txt1) as n,
    count(distinct txt2) over(partition by txt2) as m
FROM ptf_count_distinct;

SELECT
    txt1,
    txt2,
    count(distinct txt1) over(partition by txt1) as n,
    count(distinct txt2) over(partition by txt2) as m
FROM ptf_count_distinct;

set hive.vectorized.execution.ptf.enabled=true;
SELECT "*** Distinct on another column, vectorized ptf: expecting 1 in aggregations (both columns only have 1 distinct value) ***";
EXPLAIN VECTORIZATION DETAIL SELECT
    txt1,
    txt2,
    count(distinct txt1) over(partition by txt2) as n,
    count(distinct txt2) over(partition by txt1) as m
FROM ptf_count_distinct;

SELECT
    txt1,
    txt2,
    count(distinct txt1) over(partition by txt2) as n,
    count(distinct txt2) over(partition by txt1) as m
FROM ptf_count_distinct;

set hive.vectorized.execution.ptf.enabled=false;
SELECT "*** Distinct on another column, non-vectorized ptf: expecting 1 in aggregations (both columns have only 1 distinct value) ***";
EXPLAIN VECTORIZATION DETAIL SELECT
    txt1,
    txt2,
    count(distinct txt1) over(partition by txt2) as n,
    count(distinct txt2) over(partition by txt1) as m
FROM ptf_count_distinct;

SELECT
    txt1,
    txt2,
    count(distinct txt1) over(partition by txt2) as n,
    count(distinct txt2) over(partition by txt1) as m
FROM ptf_count_distinct;


CREATE TABLE ptf_count_distinct_different_values (
  id int,
  txt1 string,
  txt2 string);

INSERT INTO ptf_count_distinct_different_values VALUES
  (1,'1010005758','7164335675012038'),
  (2,'2010005759','7164335675012038');


SELECT "*** Testing STRING, different values ***";
set hive.vectorized.execution.ptf.enabled=true;
SELECT "*** Distinct on another column, vectorized ptf: expecting 2 for distinct_txt1_over_txt2, 1 for distinct_txt2_over_txt1 ***";

EXPLAIN VECTORIZATION DETAIL SELECT
    txt1,
    txt2,
    count(distinct txt1) over(partition by txt2) as distinct_txt1_over_txt2,
    count(distinct txt2) over(partition by txt1) as distinct_txt2_over_txt1
FROM ptf_count_distinct_different_values
ORDER BY txt1;

SELECT
    txt1,
    txt2,
    count(distinct txt1) over(partition by txt2) as distinct_txt1_over_txt2,
    count(distinct txt2) over(partition by txt1) as distinct_txt2_over_txt1
FROM ptf_count_distinct_different_values
ORDER BY txt1;

SELECT "*** Distinct on partitioning column, vectorized ptf: expecting 1 for both distinct_txt1_over_txt2 and distinct_txt2_over_txt1 ***";
EXPLAIN VECTORIZATION DETAIL SELECT
    txt1,
    txt2,
    count(distinct txt1) over(partition by txt1) as n,
    count(distinct txt2) over(partition by txt2) as m
FROM ptf_count_distinct_different_values
ORDER BY txt1;

SELECT
    txt1,
    txt2,
    count(distinct txt1) over(partition by txt1) as n,
    count(distinct txt2) over(partition by txt2) as m
FROM ptf_count_distinct_different_values
ORDER BY txt1;

set hive.vectorized.execution.ptf.enabled=false;
SELECT "*** Distinct on another column, non-vectorized ptf: expecting 2 for distinct_txt1_over_txt2, 1 for distinct_txt2_over_txt1 ***";

EXPLAIN VECTORIZATION DETAIL SELECT
    txt1,
    txt2,
    count(distinct txt1) over(partition by txt2) as distinct_txt1_over_txt2,
    count(distinct txt2) over(partition by txt1) as distinct_txt2_over_txt1
FROM ptf_count_distinct_different_values
ORDER BY txt1;

SELECT
    txt1,
    txt2,
    count(distinct txt1) over(partition by txt2) as distinct_txt1_over_txt2,
    count(distinct txt2) over(partition by txt1) as distinct_txt2_over_txt1
FROM ptf_count_distinct_different_values
ORDER BY txt1;

SELECT "*** Distinct on partitioning column, non-vectorized ptf: expecting 1 for both distinct_txt1_over_txt2 and distinct_txt2_over_txt1 ***";
EXPLAIN VECTORIZATION DETAIL SELECT
    txt1,
    txt2,
    count(distinct txt1) over(partition by txt1) as distinct_txt1_over_txt2,
    count(distinct txt2) over(partition by txt2) as distinct_txt2_over_txt1
FROM ptf_count_distinct_different_values
ORDER BY txt1;

SELECT
    txt1,
    txt2,
    count(distinct txt1) over(partition by txt1) as distinct_txt1_over_txt2,
    count(distinct txt2) over(partition by txt2) as distinct_txt2_over_txt1
FROM ptf_count_distinct_different_values
ORDER BY txt1;








SELECT "*** Testing INT/LONG ***";
CREATE TABLE ptf_count_distinct_different_values_long (
  id int,
  long1 int,
  long2 int);

INSERT INTO ptf_count_distinct_different_values_long VALUES
  (1, 1010005758, 716433567),
  (2, 2010005759, 716433567);

set hive.vectorized.execution.ptf.enabled=true;
SELECT "*** Distinct on another column, vectorized ptf: expecting 2 for distinct_long1_over_long2, 1 for distinct_long2_over_long1 ***";

EXPLAIN VECTORIZATION DETAIL SELECT
    long1,
    long2,
    count(distinct long1) over(partition by long2) as distinct_long1_over_long2,
    count(distinct long2) over(partition by long1) as distinct_long2_over_long1
FROM ptf_count_distinct_different_values_long
ORDER BY long1;

SELECT
    long1,
    long2,
    count(distinct long1) over(partition by long2) as distinct_long1_over_long2,
    count(distinct long2) over(partition by long1) as distinct_long2_over_long1
FROM ptf_count_distinct_different_values_long
ORDER BY long1;

SELECT "*** Distinct on partitioning column, vectorized ptf: expecting 1 for both distinct_long1_over_long1 and distinct_long2_over_long2 ***";
EXPLAIN VECTORIZATION DETAIL SELECT
    long1,
    long2,
    count(distinct long1) over(partition by long1) as distinct_long1_over_long1,
    count(distinct long2) over(partition by long2) as distinct_long2_over_long2
FROM ptf_count_distinct_different_values_long
ORDER BY long1;

SELECT
    long1,
    long2,
    count(distinct long1) over(partition by long1) as distinct_long1_over_long1,
    count(distinct long2) over(partition by long2) as distinct_long2_over_long2
FROM ptf_count_distinct_different_values_long
ORDER BY long1;

set hive.vectorized.execution.ptf.enabled=false;
SELECT "*** Distinct on another column, non-vectorized ptf: expecting 2 for distinct_long1_over_long2, 1 for distinct_long2_over_long1 ***";

EXPLAIN VECTORIZATION DETAIL SELECT
    long1,
    long2,
    count(distinct long1) over(partition by long2) as distinct_long1_over_long2,
    count(distinct long2) over(partition by long1) as distinct_long2_over_long1
FROM ptf_count_distinct_different_values_long
ORDER BY long1;

SELECT
    long1,
    long2,
    count(distinct long1) over(partition by long2) as distinct_long1_over_long2,
    count(distinct long2) over(partition by long1) as distinct_long2_over_long1
FROM ptf_count_distinct_different_values_long
ORDER BY long1;

SELECT "*** Distinct on partitioning column, non-vectorized ptf: expecting 1 for both distinct_long1_over_long1 and distinct_long2_over_long2 ***";
EXPLAIN VECTORIZATION DETAIL SELECT
    long1,
    long2,
    count(distinct long1) over(partition by long1) as distinct_long1_over_long1,
    count(distinct long2) over(partition by long2) as distinct_long2_over_long2
FROM ptf_count_distinct_different_values_long
ORDER BY long1;

SELECT
    long1,
    long2,
    count(distinct long1) over(partition by long1) as distinct_long1_over_long1,
    count(distinct long2) over(partition by long2) as distinct_long2_over_long2
FROM ptf_count_distinct_different_values_long
ORDER BY long1;








SELECT "*** Testing DOUBLE ***";
CREATE TABLE ptf_count_distinct_different_values_double (
  id int,
  double1 double,
  double2 double);

INSERT INTO ptf_count_distinct_different_values_double VALUES
  (1, 1010005758.1, 716433567.1),
  (2, 2010005759.1, 716433567.1);

set hive.vectorized.execution.ptf.enabled=true;
SELECT "*** Distinct on another column, vectorized ptf: expecting 2 for distinct_double1_over_double2, 1 for distinct_double2_over_double1 ***";

EXPLAIN VECTORIZATION DETAIL SELECT
    double1,
    double2,
    count(distinct double1) over(partition by double2) as distinct_double1_over_double2,
    count(distinct double2) over(partition by double1) as distinct_double2_over_double1
FROM ptf_count_distinct_different_values_double
ORDER BY double1;

SELECT
    double1,
    double2,
    count(distinct double1) over(partition by double2) as distinct_double1_over_double2,
    count(distinct double2) over(partition by double1) as distinct_double2_over_double1
FROM ptf_count_distinct_different_values_double
ORDER BY double1;

SELECT "*** Distinct on partitioning column, vectorized ptf: expecting 1 for both distinct_double1_over_double1 and distinct_double2_over_double2 ***";
EXPLAIN VECTORIZATION DETAIL SELECT
    double1,
    double2,
    count(distinct double1) over(partition by double1) as distinct_double1_over_double1,
    count(distinct double2) over(partition by double2) as distinct_double2_over_double2
FROM ptf_count_distinct_different_values_double
ORDER BY double1;

SELECT
    double1,
    double2,
    count(distinct double1) over(partition by double1) as distinct_double1_over_double1,
    count(distinct double2) over(partition by double2) as distinct_double2_over_double2
FROM ptf_count_distinct_different_values_double
ORDER BY double1;

set hive.vectorized.execution.ptf.enabled=false;
SELECT "*** Distinct on another column, non-vectorized ptf: expecting 2 for distinct_double1_over_double2, 1 for distinct_double2_over_double1 ***";

EXPLAIN VECTORIZATION DETAIL SELECT
    double1,
    double2,
    count(distinct double1) over(partition by double2) as distinct_double1_over_double2,
    count(distinct double2) over(partition by double1) as distinct_double2_over_double1
FROM ptf_count_distinct_different_values_double
ORDER BY double1;

SELECT
    double1,
    double2,
    count(distinct double1) over(partition by double2) as distinct_double1_over_double2,
    count(distinct double2) over(partition by double1) as distinct_double2_over_double1
FROM ptf_count_distinct_different_values_double
ORDER BY double1;

SELECT "*** Distinct on partitioning column, non-vectorized ptf: expecting 1 for both distinct_double1_over_double1 and distinct_double2_over_double2 ***";
EXPLAIN VECTORIZATION DETAIL SELECT
    double1,
    double2,
    count(distinct double1) over(partition by double1) as distinct_double1_over_double1,
    count(distinct double2) over(partition by double2) as distinct_double2_over_double2
FROM ptf_count_distinct_different_values_double
ORDER BY double1;

SELECT
    double1,
    double2,
    count(distinct double1) over(partition by double1) as distinct_double1_over_double1,
    count(distinct double2) over(partition by double2) as distinct_double2_over_double2
FROM ptf_count_distinct_different_values_double
ORDER BY double1;









SELECT "*** Testing DECIMAL ***";
CREATE TABLE ptf_count_distinct_different_values_decimal (
  id int,
  decimal1 decimal,
  decimal2 decimal);

INSERT INTO ptf_count_distinct_different_values_decimal VALUES
  (1, 1010005758.1, 716433567.1),
  (2, 2010005759.1, 716433567.1);

set hive.vectorized.execution.ptf.enabled=true;
SELECT "*** Distinct on another column, vectorized ptf: expecting 2 for distinct_decimal1_over_decimal2, 1 for distinct_decimal2_over_decimal1 ***";

EXPLAIN VECTORIZATION DETAIL SELECT
    decimal1,
    decimal2,
    count(distinct decimal1) over(partition by decimal2) as distinct_decimal1_over_decimal2,
    count(distinct decimal2) over(partition by decimal1) as distinct_decimal2_over_decimal1
FROM ptf_count_distinct_different_values_decimal
ORDER BY decimal1;

SELECT
    decimal1,
    decimal2,
    count(distinct decimal1) over(partition by decimal2) as distinct_decimal1_over_decimal2,
    count(distinct decimal2) over(partition by decimal1) as distinct_decimal2_over_decimal1
FROM ptf_count_distinct_different_values_decimal
ORDER BY decimal1;

SELECT "*** Distinct on partitioning column, vectorized ptf: expecting 1 for both distinct_decimal1_over_decimal1 and distinct_decimal2_over_decimal2 ***";
EXPLAIN VECTORIZATION DETAIL SELECT
    decimal1,
    decimal2,
    count(distinct decimal1) over(partition by decimal1) as distinct_decimal1_over_decimal1,
    count(distinct decimal2) over(partition by decimal2) as distinct_decimal2_over_decimal2
FROM ptf_count_distinct_different_values_decimal
ORDER BY decimal1;

SELECT
    decimal1,
    decimal2,
    count(distinct decimal1) over(partition by decimal1) as distinct_decimal1_over_decimal1,
    count(distinct decimal2) over(partition by decimal2) as distinct_decimal2_over_decimal2
FROM ptf_count_distinct_different_values_decimal
ORDER BY decimal1;

set hive.vectorized.execution.ptf.enabled=false;
SELECT "*** Distinct on another column, non-vectorized ptf: expecting 2 for distinct_decimal1_over_decimal2, 1 for distinct_decimal2_over_decimal1 ***";

EXPLAIN VECTORIZATION DETAIL SELECT
    decimal1,
    decimal2,
    count(distinct decimal1) over(partition by decimal2) as distinct_decimal1_over_decimal2,
    count(distinct decimal2) over(partition by decimal1) as distinct_decimal2_over_decimal1
FROM ptf_count_distinct_different_values_decimal
ORDER BY decimal1;

SELECT
    decimal1,
    decimal2,
    count(distinct decimal1) over(partition by decimal2) as distinct_decimal1_over_decimal2,
    count(distinct decimal2) over(partition by decimal1) as distinct_decimal2_over_decimal1
FROM ptf_count_distinct_different_values_decimal
ORDER BY decimal1;

SELECT "*** Distinct on partitioning column, non-vectorized ptf: expecting 1 for both distinct_decimal1_over_decimal1 and distinct_decimal2_over_decimal2 ***";
EXPLAIN VECTORIZATION DETAIL SELECT
    decimal1,
    decimal2,
    count(distinct decimal1) over(partition by decimal1) as distinct_decimal1_over_decimal1,
    count(distinct decimal2) over(partition by decimal2) as distinct_decimal2_over_decimal2
FROM ptf_count_distinct_different_values_decimal
ORDER BY decimal1;

SELECT
    decimal1,
    decimal2,
    count(distinct decimal1) over(partition by decimal1) as distinct_decimal1_over_decimal1,
    count(distinct decimal2) over(partition by decimal2) as distinct_decimal2_over_decimal2
FROM ptf_count_distinct_different_values_decimal
ORDER BY decimal1;









SELECT "*** Testing TIMESTAMP ***";
CREATE TABLE ptf_count_distinct_different_values_timestamp (
  id int,
  timestamp1 timestamp,
  timestamp2 timestamp);

INSERT INTO ptf_count_distinct_different_values_timestamp VALUES
  (1, '2015-11-29 09:30:00', '2020-11-29 09:30:00'),
  (2, '2015-11-29 09:30:01', '2020-11-29 09:30:00');

set hive.vectorized.execution.ptf.enabled=true;
SELECT "*** Distinct on another column, vectorized ptf: expecting 2 for distinct_timestamp1_over_timestamp2, 1 for distinct_timestamp2_over_timestamp1 ***";

EXPLAIN VECTORIZATION DETAIL SELECT
    timestamp1,
    timestamp2,
    count(distinct timestamp1) over(partition by timestamp2) as distinct_timestamp1_over_timestamp2,
    count(distinct timestamp2) over(partition by timestamp1) as distinct_timestamp2_over_timestamp1
FROM ptf_count_distinct_different_values_timestamp
ORDER BY timestamp1;

SELECT
    timestamp1,
    timestamp2,
    count(distinct timestamp1) over(partition by timestamp2) as distinct_timestamp1_over_timestamp2,
    count(distinct timestamp2) over(partition by timestamp1) as distinct_timestamp2_over_timestamp1
FROM ptf_count_distinct_different_values_timestamp
ORDER BY timestamp1;

SELECT "*** Distinct on partitioning column, vectorized ptf: expecting 1 for both distinct_timestamp1_over_timestamp1 and distinct_timestamp2_over_timestamp2 ***";
EXPLAIN VECTORIZATION DETAIL SELECT
    timestamp1,
    timestamp2,
    count(distinct timestamp1) over(partition by timestamp1) as distinct_timestamp1_over_timestamp1,
    count(distinct timestamp2) over(partition by timestamp2) as distinct_timestamp2_over_timestamp2
FROM ptf_count_distinct_different_values_timestamp
ORDER BY timestamp1;

SELECT
    timestamp1,
    timestamp2,
    count(distinct timestamp1) over(partition by timestamp1) as distinct_timestamp1_over_timestamp1,
    count(distinct timestamp2) over(partition by timestamp2) as distinct_timestamp2_over_timestamp2
FROM ptf_count_distinct_different_values_timestamp
ORDER BY timestamp1;

set hive.vectorized.execution.ptf.enabled=false;
SELECT "*** Distinct on another column, non-vectorized ptf: expecting 2 for distinct_timestamp1_over_timestamp2, 1 for distinct_timestamp2_over_timestamp1 ***";

EXPLAIN VECTORIZATION DETAIL SELECT
    timestamp1,
    timestamp2,
    count(distinct timestamp1) over(partition by timestamp2) as distinct_timestamp1_over_timestamp2,
    count(distinct timestamp2) over(partition by timestamp1) as distinct_timestamp2_over_timestamp1
FROM ptf_count_distinct_different_values_timestamp
ORDER BY timestamp1;

SELECT
    timestamp1,
    timestamp2,
    count(distinct timestamp1) over(partition by timestamp2) as distinct_timestamp1_over_timestamp2,
    count(distinct timestamp2) over(partition by timestamp1) as distinct_timestamp2_over_timestamp1
FROM ptf_count_distinct_different_values_timestamp
ORDER BY timestamp1;

SELECT "*** Distinct on partitioning column, non-vectorized ptf: expecting 1 for both distinct_timestamp1_over_timestamp1 and distinct_timestamp2_over_timestamp2 ***";
EXPLAIN VECTORIZATION DETAIL SELECT
    timestamp1,
    timestamp2,
    count(distinct timestamp1) over(partition by timestamp1) as distinct_timestamp1_over_timestamp1,
    count(distinct timestamp2) over(partition by timestamp2) as distinct_timestamp2_over_timestamp2
FROM ptf_count_distinct_different_values_timestamp
ORDER BY timestamp1;

SELECT
    timestamp1,
    timestamp2,
    count(distinct timestamp1) over(partition by timestamp1) as distinct_timestamp1_over_timestamp1,
    count(distinct timestamp2) over(partition by timestamp2) as distinct_timestamp2_over_timestamp2
FROM ptf_count_distinct_different_values_timestamp
ORDER BY timestamp1;