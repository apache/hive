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

-- join with 4 tables on different keys is also executed as a single MR job
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
