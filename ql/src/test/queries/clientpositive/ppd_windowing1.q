set hive.mapred.mode=nonstrict;
set hive.optimize.ppd=true;
set hive.ppd.remove.duplicatefilters=false;

-- Test simple PPD through Windowing
EXPLAIN select * from (SELECT key, sum(key) over(partition by key) as c1 from src)r1 where key > '2';
EXPLAIN select * from (SELECT key, sum(key) over(partition by key) as c1 from src)r1 where key > 2;
EXPLAIN select * from (SELECT key, sum(key) over(partition by key) as c1 from src)r1 where cast(key as int) > 2;
EXPLAIN select * from (SELECT key, sum(key) over(partition by key) as c1 from src)r1 where (cast(key as int) + 1) > 2;
EXPLAIN select * from (SELECT (cast(key as int))+2 as key, sum(key) over(partition by key) as c1 from src)r1 where (cast(key as int) + 1) > 2;


-- Test PPD through Windowing where predicate is a subset of partition keys
EXPLAIN select * from (SELECT key, sum(key) over(partition by key, value) as c1  from src)r1 where key > '2';
EXPLAIN select * from (SELECT key, sum(key) over(partition by key, value) as c1  from src)r1 where key > 2;
EXPLAIN select * from (SELECT key, sum(key) over(partition by key, value) as c1  from src)r1 where cast(key as int) > 2;
EXPLAIN select * from (SELECT key, sum(key) over(partition by key, value) as c1  from src)r1 where (cast(key as int) + 1) > 2;
EXPLAIN select * from (SELECT (cast(key as int))+2 as key, sum(key) over(partition by key, value) as c1  from src)r1 where (cast(key as int) + 1) > 2;


-- Test PPD through Windowing where predicate is a subset of partition keys, multiple windows are involved and UDAF is same
EXPLAIN select * from (SELECT key, sum(key) over(partition by key,value) as c1, sum(key) over(partition by key) as c2  from src)r1 where key > '2';
EXPLAIN select * from (SELECT key, sum(key) over(partition by key,value) as c1, sum(key) over(partition by key) as c2  from src)r1 where key > 2;
EXPLAIN select * from (SELECT key, sum(key) over(partition by key,value) as c1, sum(key) over(partition by key) as c2  from src)r1 where (cast(key as int) + 1) > 2;
EXPLAIN select * from (SELECT (cast(key as int))+2 as key, sum(key) over(partition by key,value) as c1, sum(key) over(partition by key) as c2  from src)r1 where (cast(key as int) + 1) > 2;


-- Test PPD through Windowing where predicate is a subset of partition keys, multiple windows are involved and UDAF has different args 
EXPLAIN select * from (SELECT key, sum(key) over(partition by key,value) as c1, sum(value) over(partition by key) as c2  from src)r1 where key > '2';
EXPLAIN select * from (SELECT key, sum(key) over(partition by key,value) as c1, sum(value) over(partition by key) as c2  from src)r1 where key > 2;
EXPLAIN select * from (SELECT key, sum(key) over(partition by key,value) as c1, sum(value) over(partition by key) as c2  from src)r1 where (cast(key as int) + 1) > 2;
EXPLAIN select * from (SELECT (cast(key as int))+2 as key, sum(key) over(partition by key,value) as c1, sum(value) over(partition by key) as c2  from src)r1 where (cast(key as int) + 1) > 2;


-- Test predicate is not getting pushed down when multiple windows are involved and they don't have common partition keys 
EXPLAIN select * from (SELECT key, sum(key) over(partition by key,value) as c1, avg(value) over(partition by value) as c2  from src)r1 where key > '2';


-- Test predicate is not getting pushed down when window has compound partition key
EXPLAIN select * from (SELECT key, sum(key) over(partition by key + 2) as c1 from src)r1 where key > '2';
EXPLAIN select * from (SELECT key, sum(key) over(partition by key + value) as c1 from src)r1 where key > '2';

-- Test predicate is not getting pushed down when predicate involves more than one col 
EXPLAIN select * from (SELECT key, value, sum(key) over(partition by key, value) as c1 from src)r1 where (key + value) > '2';
EXPLAIN select * from (SELECT key, value, sum(key) over(partition by key + value) as c1 from src)r1 where (key + value) > '2';
EXPLAIN select * from (SELECT (cast(key as int))+(cast(value as int)) as key, sum(key) over(partition by key) as c1 from src)r1 where key > 2;
