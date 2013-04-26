-- Join of a big table with 2 small tables on different keys should be performed as a single MR job
create table smallTbl1(key string, value string);
insert overwrite table smallTbl1 select * from src where key < 10;

create table smallTbl2(key string, value string);
insert overwrite table smallTbl2 select * from src where key < 10;

create table bigTbl(key string, value string);
insert overwrite table bigTbl
select * from
(
 select * from src
   union all
 select * from src
   union all
 select * from src
   union all
 select * from src
   union all
 select * from src
   union all
 select * from src
   union all
 select * from src
   union all
 select * from src
   union all
 select * from src
   union all
 select * from src
) subq;

set hive.auto.convert.join=true;

explain
select count(*) FROM
(select bigTbl.key as key, bigTbl.value as value1,
 bigTbl.value as value2 FROM bigTbl JOIN smallTbl1 
 on (bigTbl.key = smallTbl1.key)
) firstjoin
JOIN                                                                  
smallTbl2 on (firstjoin.value1 = smallTbl2.value);

select count(*) FROM
(select bigTbl.key as key, bigTbl.value as value1,
 bigTbl.value as value2 FROM bigTbl JOIN smallTbl1 
 on (bigTbl.key = smallTbl1.key)
) firstjoin
JOIN                                                                  
smallTbl2 on (firstjoin.value1 = smallTbl2.value);

set hive.auto.convert.join.noconditionaltask=true;
set hive.auto.convert.join.noconditionaltask.size=10000;

-- Now run a query with two-way join, which should be converted into a
-- map-join followed by groupby - two MR jobs overall 
explain
select count(*) FROM
(select bigTbl.key as key, bigTbl.value as value1,
 bigTbl.value as value2 FROM bigTbl JOIN smallTbl1 
 on (bigTbl.key = smallTbl1.key)
) firstjoin
JOIN                                                                  
smallTbl2 on (firstjoin.value1 = smallTbl2.value);

select count(*) FROM
(select bigTbl.key as key, bigTbl.value as value1,
 bigTbl.value as value2 FROM bigTbl JOIN smallTbl1 
 on (bigTbl.key = smallTbl1.key)
) firstjoin
JOIN                                                                  
smallTbl2 on (firstjoin.value1 = smallTbl2.value);

set hive.optimize.mapjoin.mapreduce=true;

-- Now run a query with two-way join, which should first be converted into a
-- map-join followed by groupby and then finally into a single MR job.

explain insert overwrite directory '${system:test.tmp.dir}/multiJoin1.output'
select count(*) FROM
(select bigTbl.key as key, bigTbl.value as value1,
 bigTbl.value as value2 FROM bigTbl JOIN smallTbl1 
 on (bigTbl.key = smallTbl1.key)
) firstjoin
JOIN                                                                  
smallTbl2 on (firstjoin.value1 = smallTbl2.value)
group by smallTbl2.key;

insert overwrite directory '${system:test.tmp.dir}/multiJoin1.output'
select count(*) FROM
(select bigTbl.key as key, bigTbl.value as value1,
 bigTbl.value as value2 FROM bigTbl JOIN smallTbl1 
 on (bigTbl.key = smallTbl1.key)
) firstjoin
JOIN                                                                  
smallTbl2 on (firstjoin.value1 = smallTbl2.value)
group by smallTbl2.key;
set hive.optimize.mapjoin.mapreduce=false;

create table smallTbl3(key string, value string);
insert overwrite table smallTbl3 select * from src where key < 10;

drop table bigTbl;

create table bigTbl(key1 string, key2 string, value string);
insert overwrite table bigTbl
select * from
(
 select key as key1, key as key2, value from src
   union all
 select key as key1, key as key2, value from src
   union all
 select key as key1, key as key2, value from src
   union all
 select key as key1, key as key2, value from src
   union all
 select key as key1, key as key2, value from src
   union all
 select key as key1, key as key2, value from src
   union all
 select key as key1, key as key2, value from src
   union all
 select key as key1, key as key2, value from src
   union all
 select key as key1, key as key2, value from src
   union all
 select key as key1, key as key2, value from src
) subq;

set hive.auto.convert.join.noconditionaltask=false;

explain
select count(*) FROM
 (
   SELECT firstjoin.key1 as key1, firstjoin.key2 as key2, smallTbl2.key as key3,
          firstjoin.value1 as value1, firstjoin.value2 as value2 FROM
    (SELECT bigTbl.key1 as key1, bigTbl.key2 as key2, 
            bigTbl.value as value1, bigTbl.value as value2 
     FROM bigTbl JOIN smallTbl1 
     on (bigTbl.key1 = smallTbl1.key)
    ) firstjoin
    JOIN                                                                  
    smallTbl2 on (firstjoin.value1 = smallTbl2.value)
 ) secondjoin
 JOIN smallTbl3 on (secondjoin.key2 = smallTbl3.key);

select count(*) FROM
 (
   SELECT firstjoin.key1 as key1, firstjoin.key2 as key2, smallTbl2.key as key3,
          firstjoin.value1 as value1, firstjoin.value2 as value2 FROM
    (SELECT bigTbl.key1 as key1, bigTbl.key2 as key2, 
            bigTbl.value as value1, bigTbl.value as value2 
     FROM bigTbl JOIN smallTbl1 
     on (bigTbl.key1 = smallTbl1.key)
    ) firstjoin
    JOIN                                                                  
    smallTbl2 on (firstjoin.value1 = smallTbl2.value)
 ) secondjoin
 JOIN smallTbl3 on (secondjoin.key2 = smallTbl3.key);

set hive.auto.convert.join.noconditionaltask=true;
set hive.auto.convert.join.noconditionaltask.size=10000;

-- join with 4 tables on different keys is also executed as a single MR job,
-- So, overall two jobs - one for multi-way join and one for count(*)
explain
select count(*) FROM
 (
   SELECT firstjoin.key1 as key1, firstjoin.key2 as key2, smallTbl2.key as key3,
          firstjoin.value1 as value1, firstjoin.value2 as value2 FROM
    (SELECT bigTbl.key1 as key1, bigTbl.key2 as key2, 
            bigTbl.value as value1, bigTbl.value as value2 
     FROM bigTbl JOIN smallTbl1 
     on (bigTbl.key1 = smallTbl1.key)
    ) firstjoin
    JOIN                                                                  
    smallTbl2 on (firstjoin.value1 = smallTbl2.value)
 ) secondjoin
 JOIN smallTbl3 on (secondjoin.key2 = smallTbl3.key);

select count(*) FROM
 (
   SELECT firstjoin.key1 as key1, firstjoin.key2 as key2, smallTbl2.key as key3,
          firstjoin.value1 as value1, firstjoin.value2 as value2 FROM
    (SELECT bigTbl.key1 as key1, bigTbl.key2 as key2, 
            bigTbl.value as value1, bigTbl.value as value2 
     FROM bigTbl JOIN smallTbl1 
     on (bigTbl.key1 = smallTbl1.key)
    ) firstjoin
    JOIN                                                                  
    smallTbl2 on (firstjoin.value1 = smallTbl2.value)
 ) secondjoin
 JOIN smallTbl3 on (secondjoin.key2 = smallTbl3.key);

set hive.optimize.mapjoin.mapreduce=true;
-- Now run the above query with M-MR optimization
-- This should be a single MR job end-to-end.
explain
select count(*) FROM
 (
   SELECT firstjoin.key1 as key1, firstjoin.key2 as key2, smallTbl2.key as key3,
          firstjoin.value1 as value1, firstjoin.value2 as value2 FROM
    (SELECT bigTbl.key1 as key1, bigTbl.key2 as key2, 
            bigTbl.value as value1, bigTbl.value as value2 
     FROM bigTbl JOIN smallTbl1 
     on (bigTbl.key1 = smallTbl1.key)
    ) firstjoin
    JOIN                                                                  
    smallTbl2 on (firstjoin.value1 = smallTbl2.value)
 ) secondjoin
 JOIN smallTbl3 on (secondjoin.key2 = smallTbl3.key);

select count(*) FROM
 (
   SELECT firstjoin.key1 as key1, firstjoin.key2 as key2, smallTbl2.key as key3,
          firstjoin.value1 as value1, firstjoin.value2 as value2 FROM
    (SELECT bigTbl.key1 as key1, bigTbl.key2 as key2, 
            bigTbl.value as value1, bigTbl.value as value2 
     FROM bigTbl JOIN smallTbl1 
     on (bigTbl.key1 = smallTbl1.key)
    ) firstjoin
    JOIN                                                                  
    smallTbl2 on (firstjoin.value1 = smallTbl2.value)
 ) secondjoin
 JOIN smallTbl3 on (secondjoin.key2 = smallTbl3.key);

set hive.optimize.mapjoin.mapreduce=false;
