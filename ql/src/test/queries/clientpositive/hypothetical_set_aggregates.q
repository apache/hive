DESCRIBE FUNCTION rank;
DESCRIBE FUNCTION EXTENDED rank;
DESCRIBE FUNCTION dense_rank;
DESCRIBE FUNCTION EXTENDED dense_rank;
DESCRIBE FUNCTION percent_rank;
DESCRIBE FUNCTION EXTENDED percent_rank;
DESCRIBE FUNCTION cume_dist;
DESCRIBE FUNCTION EXTENDED cume_dist;


CREATE TABLE t_test (
  col1 int,
  col2 int
);
INSERT INTO t_test VALUES
(NULL, NULL),
(3, 0),
(5, 1),
(5, 1),
(5, 2),
(5, 3),
(10, 20.0),
(NULL, NULL),
(NULL, NULL),
(11, 10.0),
(15, 7.0),
(15, 15.0),
(15, 16.0),
(8, 8.0),
(7, 7.0),
(8, 8.0),
(NULL, NULL);

set hive.map.aggr = false;
set hive.groupby.skewindata = false;

select
rank(1) WITHIN GROUP (ORDER BY col1),
rank(2) WITHIN GROUP (ORDER BY col1),
rank(3) WITHIN GROUP (ORDER BY col1),
rank(4) WITHIN GROUP (ORDER BY col1),
rank(5) WITHIN GROUP (ORDER BY col1),
rank(6) WITHIN GROUP (ORDER BY col1),
rank(7) WITHIN GROUP (ORDER BY col1),
rank(8) WITHIN GROUP (ORDER BY col1)
from t_test;

select
rank(4) WITHIN GROUP (ORDER BY col1 nulls first),
rank(4) WITHIN GROUP (ORDER BY col1 nulls last),
rank(4) WITHIN GROUP (ORDER BY col1 desc),
rank(4) WITHIN GROUP (ORDER BY col1 desc nulls first),
rank(4) WITHIN GROUP (ORDER BY col1 desc nulls last)
from t_test;

select
rank(1, 3) WITHIN GROUP (ORDER BY col1, col2),
rank(2, 3) WITHIN GROUP (ORDER BY col1, col2),
rank(3, 3) WITHIN GROUP (ORDER BY col1, col2),
rank(4, 3) WITHIN GROUP (ORDER BY col1, col2),
rank(5, 3) WITHIN GROUP (ORDER BY col1, col2),
rank(6, 3) WITHIN GROUP (ORDER BY col1, col2),
rank(7, 3) WITHIN GROUP (ORDER BY col1, col2),
rank(8, 3) WITHIN GROUP (ORDER BY col1, col2)
from t_test;

select
dense_rank(1) WITHIN GROUP (ORDER BY col1),
dense_rank(2) WITHIN GROUP (ORDER BY col1),
dense_rank(3) WITHIN GROUP (ORDER BY col1),
dense_rank(4) WITHIN GROUP (ORDER BY col1),
dense_rank(5) WITHIN GROUP (ORDER BY col1),
dense_rank(6) WITHIN GROUP (ORDER BY col1),
dense_rank(7) WITHIN GROUP (ORDER BY col1),
dense_rank(8) WITHIN GROUP (ORDER BY col1)
from t_test;

select
dense_rank(1, 1) WITHIN GROUP (ORDER BY col1, col2),
dense_rank(2, 1) WITHIN GROUP (ORDER BY col1, col2),
dense_rank(3, 1) WITHIN GROUP (ORDER BY col1, col2),
dense_rank(4, 1) WITHIN GROUP (ORDER BY col1, col2),
dense_rank(5, 1) WITHIN GROUP (ORDER BY col1, col2),
dense_rank(6, 1) WITHIN GROUP (ORDER BY col1, col2),
dense_rank(7, 1) WITHIN GROUP (ORDER BY col1, col2),
dense_rank(8, 1) WITHIN GROUP (ORDER BY col1, col2)
from t_test;

select
percent_rank(1) WITHIN GROUP (ORDER BY col1),
percent_rank(2) WITHIN GROUP (ORDER BY col1),
percent_rank(3) WITHIN GROUP (ORDER BY col1),
percent_rank(4) WITHIN GROUP (ORDER BY col1),
percent_rank(5) WITHIN GROUP (ORDER BY col1),
percent_rank(6) WITHIN GROUP (ORDER BY col1),
percent_rank(7) WITHIN GROUP (ORDER BY col1),
percent_rank(8) WITHIN GROUP (ORDER BY col1)
from t_test;

select
cume_dist(7) WITHIN GROUP (ORDER BY col1),
cume_dist(2) WITHIN GROUP (ORDER BY col1),
cume_dist(3) WITHIN GROUP (ORDER BY col1),
cume_dist(4) WITHIN GROUP (ORDER BY col1),
cume_dist(5) WITHIN GROUP (ORDER BY col1),
cume_dist(6) WITHIN GROUP (ORDER BY col1),
cume_dist(7) WITHIN GROUP (ORDER BY col1),
cume_dist(8) WITHIN GROUP (ORDER BY col1)
from t_test;


set hive.map.aggr = true;
set hive.groupby.skewindata = false;

select
rank(1) WITHIN GROUP (ORDER BY col1),
rank(2) WITHIN GROUP (ORDER BY col1),
rank(3) WITHIN GROUP (ORDER BY col1),
rank(4) WITHIN GROUP (ORDER BY col1),
rank(5) WITHIN GROUP (ORDER BY col1),
rank(6) WITHIN GROUP (ORDER BY col1),
rank(7) WITHIN GROUP (ORDER BY col1),
rank(8) WITHIN GROUP (ORDER BY col1)
from t_test;

select
rank(1, 3) WITHIN GROUP (ORDER BY col1, col2),
rank(2, 3) WITHIN GROUP (ORDER BY col1, col2),
rank(3, 3) WITHIN GROUP (ORDER BY col1, col2),
rank(4, 3) WITHIN GROUP (ORDER BY col1, col2),
rank(5, 3) WITHIN GROUP (ORDER BY col1, col2),
rank(6, 3) WITHIN GROUP (ORDER BY col1, col2),
rank(7, 3) WITHIN GROUP (ORDER BY col1, col2),
rank(8, 3) WITHIN GROUP (ORDER BY col1, col2)
from t_test;

select
dense_rank(1) WITHIN GROUP (ORDER BY col1),
dense_rank(2) WITHIN GROUP (ORDER BY col1),
dense_rank(3) WITHIN GROUP (ORDER BY col1),
dense_rank(4) WITHIN GROUP (ORDER BY col1),
dense_rank(5) WITHIN GROUP (ORDER BY col1),
dense_rank(6) WITHIN GROUP (ORDER BY col1),
dense_rank(7) WITHIN GROUP (ORDER BY col1),
dense_rank(8) WITHIN GROUP (ORDER BY col1)
from t_test;

select
dense_rank(1, 1) WITHIN GROUP (ORDER BY col1, col2),
dense_rank(2, 1) WITHIN GROUP (ORDER BY col1, col2),
dense_rank(3, 1) WITHIN GROUP (ORDER BY col1, col2),
dense_rank(4, 1) WITHIN GROUP (ORDER BY col1, col2),
dense_rank(5, 1) WITHIN GROUP (ORDER BY col1, col2),
dense_rank(6, 1) WITHIN GROUP (ORDER BY col1, col2),
dense_rank(7, 1) WITHIN GROUP (ORDER BY col1, col2),
dense_rank(8, 1) WITHIN GROUP (ORDER BY col1, col2)
from t_test;

select
percent_rank(1) WITHIN GROUP (ORDER BY col1),
percent_rank(2) WITHIN GROUP (ORDER BY col1),
percent_rank(3) WITHIN GROUP (ORDER BY col1),
percent_rank(4) WITHIN GROUP (ORDER BY col1),
percent_rank(5) WITHIN GROUP (ORDER BY col1),
percent_rank(6) WITHIN GROUP (ORDER BY col1),
percent_rank(7) WITHIN GROUP (ORDER BY col1),
percent_rank(8) WITHIN GROUP (ORDER BY col1)
from t_test;

select
cume_dist(7) WITHIN GROUP (ORDER BY col1),
cume_dist(2) WITHIN GROUP (ORDER BY col1),
cume_dist(3) WITHIN GROUP (ORDER BY col1),
cume_dist(4) WITHIN GROUP (ORDER BY col1),
cume_dist(5) WITHIN GROUP (ORDER BY col1),
cume_dist(6) WITHIN GROUP (ORDER BY col1),
cume_dist(7) WITHIN GROUP (ORDER BY col1),
cume_dist(8) WITHIN GROUP (ORDER BY col1)
from t_test;


set hive.map.aggr = false;
set hive.groupby.skewindata = true;


select
rank(1) WITHIN GROUP (ORDER BY col1),
rank(2) WITHIN GROUP (ORDER BY col1),
rank(3) WITHIN GROUP (ORDER BY col1),
rank(4) WITHIN GROUP (ORDER BY col1),
rank(5) WITHIN GROUP (ORDER BY col1),
rank(6) WITHIN GROUP (ORDER BY col1),
rank(7) WITHIN GROUP (ORDER BY col1),
rank(8) WITHIN GROUP (ORDER BY col1)
from t_test;

select
rank(1, 3) WITHIN GROUP (ORDER BY col1, col2),
rank(2, 3) WITHIN GROUP (ORDER BY col1, col2),
rank(3, 3) WITHIN GROUP (ORDER BY col1, col2),
rank(4, 3) WITHIN GROUP (ORDER BY col1, col2),
rank(5, 3) WITHIN GROUP (ORDER BY col1, col2),
rank(6, 3) WITHIN GROUP (ORDER BY col1, col2),
rank(7, 3) WITHIN GROUP (ORDER BY col1, col2),
rank(8, 3) WITHIN GROUP (ORDER BY col1, col2)
from t_test;

select
dense_rank(1) WITHIN GROUP (ORDER BY col1),
dense_rank(2) WITHIN GROUP (ORDER BY col1),
dense_rank(3) WITHIN GROUP (ORDER BY col1),
dense_rank(4) WITHIN GROUP (ORDER BY col1),
dense_rank(5) WITHIN GROUP (ORDER BY col1),
dense_rank(6) WITHIN GROUP (ORDER BY col1),
dense_rank(7) WITHIN GROUP (ORDER BY col1),
dense_rank(8) WITHIN GROUP (ORDER BY col1)
from t_test;

select
dense_rank(1, 1) WITHIN GROUP (ORDER BY col1, col2),
dense_rank(2, 1) WITHIN GROUP (ORDER BY col1, col2),
dense_rank(3, 1) WITHIN GROUP (ORDER BY col1, col2),
dense_rank(4, 1) WITHIN GROUP (ORDER BY col1, col2),
dense_rank(5, 1) WITHIN GROUP (ORDER BY col1, col2),
dense_rank(6, 1) WITHIN GROUP (ORDER BY col1, col2),
dense_rank(7, 1) WITHIN GROUP (ORDER BY col1, col2),
dense_rank(8, 1) WITHIN GROUP (ORDER BY col1, col2)
from t_test;

select
percent_rank(1) WITHIN GROUP (ORDER BY col1),
percent_rank(2) WITHIN GROUP (ORDER BY col1),
percent_rank(3) WITHIN GROUP (ORDER BY col1),
percent_rank(4) WITHIN GROUP (ORDER BY col1),
percent_rank(5) WITHIN GROUP (ORDER BY col1),
percent_rank(6) WITHIN GROUP (ORDER BY col1),
percent_rank(7) WITHIN GROUP (ORDER BY col1),
percent_rank(8) WITHIN GROUP (ORDER BY col1)
from t_test;

select
cume_dist(7) WITHIN GROUP (ORDER BY col1),
cume_dist(2) WITHIN GROUP (ORDER BY col1),
cume_dist(3) WITHIN GROUP (ORDER BY col1),
cume_dist(4) WITHIN GROUP (ORDER BY col1),
cume_dist(5) WITHIN GROUP (ORDER BY col1),
cume_dist(6) WITHIN GROUP (ORDER BY col1),
cume_dist(7) WITHIN GROUP (ORDER BY col1),
cume_dist(8) WITHIN GROUP (ORDER BY col1)
from t_test;


set hive.map.aggr = true;
set hive.groupby.skewindata = true;


select
rank(1) WITHIN GROUP (ORDER BY col1),
rank(2) WITHIN GROUP (ORDER BY col1),
rank(3) WITHIN GROUP (ORDER BY col1),
rank(4) WITHIN GROUP (ORDER BY col1),
rank(5) WITHIN GROUP (ORDER BY col1),
rank(6) WITHIN GROUP (ORDER BY col1),
rank(7) WITHIN GROUP (ORDER BY col1),
rank(8) WITHIN GROUP (ORDER BY col1)
from t_test;

select
rank(1, 3) WITHIN GROUP (ORDER BY col1, col2),
rank(2, 3) WITHIN GROUP (ORDER BY col1, col2),
rank(3, 3) WITHIN GROUP (ORDER BY col1, col2),
rank(4, 3) WITHIN GROUP (ORDER BY col1, col2),
rank(5, 3) WITHIN GROUP (ORDER BY col1, col2),
rank(6, 3) WITHIN GROUP (ORDER BY col1, col2),
rank(7, 3) WITHIN GROUP (ORDER BY col1, col2),
rank(8, 3) WITHIN GROUP (ORDER BY col1, col2)
from t_test;

select
dense_rank(1) WITHIN GROUP (ORDER BY col1),
dense_rank(2) WITHIN GROUP (ORDER BY col1),
dense_rank(3) WITHIN GROUP (ORDER BY col1),
dense_rank(4) WITHIN GROUP (ORDER BY col1),
dense_rank(5) WITHIN GROUP (ORDER BY col1),
dense_rank(6) WITHIN GROUP (ORDER BY col1),
dense_rank(7) WITHIN GROUP (ORDER BY col1),
dense_rank(8) WITHIN GROUP (ORDER BY col1)
from t_test;

select
dense_rank(1, 1) WITHIN GROUP (ORDER BY col1, col2),
dense_rank(2, 1) WITHIN GROUP (ORDER BY col1, col2),
dense_rank(3, 1) WITHIN GROUP (ORDER BY col1, col2),
dense_rank(4, 1) WITHIN GROUP (ORDER BY col1, col2),
dense_rank(5, 1) WITHIN GROUP (ORDER BY col1, col2),
dense_rank(6, 1) WITHIN GROUP (ORDER BY col1, col2),
dense_rank(7, 1) WITHIN GROUP (ORDER BY col1, col2),
dense_rank(8, 1) WITHIN GROUP (ORDER BY col1, col2)
from t_test;

select
percent_rank(1) WITHIN GROUP (ORDER BY col1),
percent_rank(2) WITHIN GROUP (ORDER BY col1),
percent_rank(3) WITHIN GROUP (ORDER BY col1),
percent_rank(4) WITHIN GROUP (ORDER BY col1),
percent_rank(5) WITHIN GROUP (ORDER BY col1),
percent_rank(6) WITHIN GROUP (ORDER BY col1),
percent_rank(7) WITHIN GROUP (ORDER BY col1),
percent_rank(8) WITHIN GROUP (ORDER BY col1)
from t_test;

select
cume_dist(7) WITHIN GROUP (ORDER BY col1),
cume_dist(2) WITHIN GROUP (ORDER BY col1),
cume_dist(3) WITHIN GROUP (ORDER BY col1),
cume_dist(4) WITHIN GROUP (ORDER BY col1),
cume_dist(5) WITHIN GROUP (ORDER BY col1),
cume_dist(6) WITHIN GROUP (ORDER BY col1),
cume_dist(7) WITHIN GROUP (ORDER BY col1),
cume_dist(8) WITHIN GROUP (ORDER BY col1)
from t_test;

set hive.cbo.enable=false;

select
cume_dist(7) WITHIN GROUP (ORDER BY col1),
dense_rank(2, 1) WITHIN GROUP (ORDER BY col1, col2)
from t_test;

DROP TABLE t_test;

