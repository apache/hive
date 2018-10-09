set hive.strict.checks.cartesian.product=false;

CREATE TABLE `customer_removal_n0`(
  `c_custkey` bigint,
  `c_name` string,
  `c_address` string,
  `c_city` string,
  `c_nation` string,
  `c_region` string,
  `c_phone` string,
  `c_mktsegment` string,
  primary key (`c_custkey`) disable rely);

CREATE TABLE `dates_removal_n0`(
  `d_datekey` bigint,
  `d_id` bigint,
  `d_date` string,
  `d_dayofweek` string,
  `d_month` string,
  `d_year` int,
  `d_yearmonthnum` int,
  `d_yearmonth` string,
  `d_daynuminweek` int,
  `d_daynuminmonth` int,
  `d_daynuminyear` int,
  `d_monthnuminyear` int,
  `d_weeknuminyear` int,
  `d_sellingseason` string,
  `d_lastdayinweekfl` int,
  `d_lastdayinmonthfl` int,
  `d_holidayfl` int ,
  `d_weekdayfl`int,
  primary key (`d_datekey`, `d_id`) disable rely);

  -- group by key has single primary key
  EXPLAIN SELECT c_custkey from customer_removal_n0 where c_nation IN ('USA', 'INDIA') group by c_custkey;

  -- mix of primary + non-primary keys
  EXPLAIN SELECT c_custkey from customer_removal_n0 where c_nation IN ('USA', 'INDIA') group by c_custkey, c_nation;

  -- multiple keys
  EXPLAIN SELECT d_datekey from dates_removal_n0 where d_year IN (1985, 2004) group by d_datekey, d_id;

  -- multiple keys + non-keys + different order
  EXPLAIN SELECT d_datekey from dates_removal_n0 where d_year IN (1985, 2004) group by d_id, d_datekey, d_sellingseason
    order by d_datekey limit 10;

 -- multiple keys in different order and mixed with non-keys
  EXPLAIN SELECT d_datekey from dates_removal_n0 where d_year IN (1985, 2004) group by d_id, d_daynuminmonth, d_datekey,
  d_sellingseason order by d_datekey limit 10;

  -- same as above but with aggregate
  EXPLAIN SELECT count(d_datekey) from dates_removal_n0 where d_year IN (1985, 2004) group by d_id, d_daynuminmonth, d_datekey,
  d_sellingseason order by d_datekey limit 10;

  -- join
  insert into dates_removal_n0(d_datekey, d_id)  values(3, 0);
  insert into dates_removal_n0(d_datekey, d_id)  values(3, 1);
  insert into customer_removal_n0 (c_custkey) values(3);

  EXPLAIN SELECT d_datekey from dates_removal_n0 join customer_removal_n0 on d_datekey = c_custkey group by d_datekey, d_id;
  SELECT d_datekey from dates_removal_n0 join customer_removal_n0 on d_datekey = c_custkey group by d_datekey, d_id;

  -- group by keys are not primary keys
  EXPLAIN SELECT d_datekey from dates_removal_n0 where d_year IN (1985, 2004) group by d_datekey, d_sellingseason
    order by d_datekey limit 10;

  -- negative
  -- with aggregate function
  EXPLAIN SELECT count(c_custkey) from customer_removal_n0 where c_nation IN ('USA', 'INDIA')
    group by c_custkey, c_nation;

  DROP TABLE customer_removal_n0;
  DROP TABLE dates_removal_n0;

  -- group by reduction optimization
  create table dest_g21 (key1 int, value1 double, primary key(key1) disable rely);
  insert into dest_g21 values(1, 2), (2,2), (3, 1), (4,4), (5, null), (6, null);

  -- value1 will removed because it is unused, then whole group by will be removed because key1 is unique
  explain select key1 from dest_g21 group by key1, value1;
  select key1 from dest_g21 group by key1, value1;
  -- same query but with filter
  explain select key1 from dest_g21 where value1 > 1 group by key1, value1;
  select key1 from dest_g21 where value1 > 1 group by key1, value1;

  explain select key1 from dest_g21 where key1 > 1 group by key1, value1;
  select key1 from dest_g21 where key1 > 1 group by key1, value1;

  -- only value1 will be removed because there is aggregate call
  explain select count(key1) from dest_g21 group by key1, value1;
  select count(key1) from dest_g21 group by key1, value1;

  explain select count(key1) from dest_g21 where value1 > 1 group by key1, value1;
  select count(key1) from dest_g21 where value1 > 1 group by key1, value1;

  -- t1.key is unique even after join therefore group by = group by (t1.key)
  explain select t1.key1 from dest_g21 t1 join dest_g21 t2 on t1.key1 = t2.key1 where t2.value1 > 2 group by t1.key1, t1.value1;
  select t1.key1 from dest_g21 t1 join dest_g21 t2 on t1.key1 = t2.key1 where t2.value1 > 2 group by t1.key1, t1.value1;

  explain select count(t1.key1) from dest_g21 t1 join dest_g21 t2 on t1.key1 = t2.key1 where t2.value1 > 2 group by t1.key1, t1.value1;
  select count(t1.key1) from dest_g21 t1 join dest_g21 t2 on t1.key1 = t2.key1 where t2.value1 > 2 group by t1.key1, t1.value1;

  -- both aggregate and one of the key1 should be removed
  explain select key1 from (select key1, count(key1) from dest_g21 where value1 < 4.5 group by key1, value1) sub;
  select key1 from (select key1, count(key1) from dest_g21 where value1 < 4.5 group by key1, value1) sub;

  -- one of the aggregate will be removed and one of the key1 will be removed
  explain select key1, sm from (select key1, count(key1), sum(key1) as sm from dest_g21 where value1 < 4.5 group by key1, value1) sub;
  select key1, sm from (select key1, count(key1), sum(key1) as sm from dest_g21 where value1 < 4.5 group by key1, value1) sub;

  DROP table dest_g21;

CREATE TABLE tconst(i int NOT NULL disable rely, j INT NOT NULL disable norely, d_year string);
INSERT INTO tconst values(1, 1, '2001'), (2, null, '2002'), (3, 3, '2010');

-- explicit NOT NULL filter
explain select i, j from tconst where i is not null group by i,j, d_year;
select i, j from tconst where i is not null group by i,j, d_year;

-- filter on i should be removed
explain select i, j from tconst where i IS NOT NULL and j IS NOT NULL group by i,j, d_year;
select i, j from tconst where i IS NOT NULL and j IS NOT NULL group by i,j, d_year;

-- where will be removed since i is not null is always true
explain select i,j from tconst where i is not null OR j IS NOT NULL group by i, j, d_year;
select i,j from tconst where i is not null OR j IS NOT NULL group by i, j, d_year;

-- should not have implicit filter on join keys
explain select sum(t1.i) from tconst t1 join tconst t2 on t1.i=t2.j group by t1.i, t1.d_year;
select sum(t1.i) from tconst t1 join tconst t2 on t1.i=t2.j group by t1.i, t1.d_year;

-- both join keys have NOT NULL
explain select sum(t1.i) from tconst t1 join tconst t2 on t1.i=t2.i group by t1.i, t1.d_year;
select sum(t1.i) from tconst t1 join tconst t2 on t1.i=t2.i group by t1.i, t1.d_year;

DROP TABLE tconst;


-- UNIQUE + NOT NULL (same as primary key)
create table dest_g21 (key1 int NOT NULL disable rely, value1 double, UNIQUE(key1) disable rely);
explain select key1 from dest_g21 group by key1, value1;

-- UNIQUE with nullabiity
create table dest_g24 (key1 int , value1 double, UNIQUE(key1) disable rely);
explain select key1 from dest_g24 group by key1, value1;

DROP TABLE dest_g21;
DROP TABLE dest_g24;
