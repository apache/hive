set hive.fetch.task.conversion=more;
set hive.mapred.mode=nonstrict;
set mapred.max.split.size=100;
set mapred.min.split.size.per.node=100;
set mapred.min.split.size.per.rack=100;
set mapred.max.split.size=100;
set tez.grouping.max-size=100;
set tez.grouping.min-size=100;

DESCRIBE FUNCTION get_splits;
DESCRIBE FUNCTION execute_splits;

select r1, r2, floor(length(r3)/100000)
from
  (select
    get_splits(
      "select key, count(*) from srcpart where key % 2 = 0 group by key",
      5) as (r1, r2, r3)) t;

select r1, r2, floor(length(r3)/100000)
from
  (select
    get_splits(
      "select key from srcpart where key % 2 = 0",
      5) as (r1, r2, r3)) t;

show tables;

select r1, r2
from
  (select
    execute_splits(
      "select key from srcpart where key % 2 = 0",
      1) as (r1, r2)) t;

select r1, r2
from
  (select
    execute_splits(
      "select key from srcpart where key % 2 = 0",
      5) as (r1, r2)) t;

select count(*) from (select key from srcpart where key % 2 = 0) t;
