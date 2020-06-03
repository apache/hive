--! qt:dataset:src1
--! qt:dataset:src
set hive.cbo.enable=true;
set hive.exec.check.crossproducts=false;
set hive.stats.fetch.column.stats=true;
set hive.auto.convert.join=false;
set hive.strict.checks.cartesian.product=false;
set hive.support.quoted.identifiers=standard;

-- SORT_QUERY_RESULTS

create database "db~!@@#$%^&*(),<>";
use "db~!@@#$%^&*(),<>";

create table "c/b/o_t1"(key string, value string, c_int int, c_float float, c_boolean boolean)  partitioned by (dt string) row format delimited fields terminated by ',' STORED AS TEXTFILE;
create table "//cbo_t2"(key string, value string, c_int int, c_float float, c_boolean boolean)  partitioned by (dt string) row format delimited fields terminated by ',' STORED AS TEXTFILE;
create table "cbo_/t3////"(key string, value string, c_int int, c_float float, c_boolean boolean)  row format delimited fields terminated by ',' STORED AS TEXTFILE;

load data local inpath '../../data/files/cbo_t1.txt' into table "c/b/o_t1" partition (dt='2014');
load data local inpath '../../data/files/cbo_t2.txt' into table "//cbo_t2" partition (dt='2014');
load data local inpath '../../data/files/cbo_t3.txt' into table "cbo_/t3////";

CREATE TABLE "p/a/r/t"(
    p_partkey INT,
    p_name STRING,
    p_mfgr STRING,
    p_brand STRING,
    p_type STRING,
    p_size INT,
    p_container STRING,
    p_retailprice DOUBLE,
    p_comment STRING
);

LOAD DATA LOCAL INPATH '../../data/files/tpch/tiny/part.tbl.bz2' overwrite into table "p/a/r/t";

CREATE TABLE "line/item" (L_ORDERKEY      INT,
                                L_PARTKEY       INT,
                                L_SUPPKEY       INT,
                                L_LINENUMBER    INT,
                                L_QUANTITY      DOUBLE,
                                L_EXTENDEDPRICE DOUBLE,
                                L_DISCOUNT      DOUBLE,
                                L_TAX           DOUBLE,
                                L_RETURNFLAG    STRING,
                                L_LINESTATUS    STRING,
                                l_shipdate      STRING,
                                L_COMMITDATE    STRING,
                                L_RECEIPTDATE   STRING,
                                L_SHIPINSTRUCT  STRING,
                                L_SHIPMODE      STRING,
                                L_COMMENT       STRING)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|';

LOAD DATA LOCAL INPATH '../../data/files/tpch/tiny/lineitem.tbl.bz2' OVERWRITE INTO TABLE "line/item";

create table "src/_/cbo" as select * from default.src;

analyze table "c/b/o_t1" compute statistics for columns key, value, c_int, c_float, c_boolean;

analyze table "//cbo_t2" compute statistics for columns key, value, c_int, c_float, c_boolean;

analyze table "cbo_/t3////" compute statistics for columns key, value, c_int, c_float, c_boolean;

analyze table "src/_/cbo" compute statistics for columns;

analyze table "p/a/r/t" compute statistics for columns;

analyze table "line/item" compute statistics for columns;

select key, (c_int+1)+2 as x, sum(c_int) from "c/b/o_t1" group by c_float, "c/b/o_t1".c_int, key;

set hive.cbo.enable=false;
set hive.exec.check.crossproducts=false;
set hive.stats.fetch.column.stats=true;
set hive.auto.convert.join=false;

-- 21. Test groupby is empty and there is no other cols in aggr
explain select unionsrc.key FROM (select 'max' as key, max(c_int) as value from "cbo_/t3////" s1

UNION  ALL

    select 'min' as key,  min(c_int) as value from "cbo_/t3////" s2

    UNION ALL

        select 'avg' as key,  avg(c_int) as value from "cbo_/t3////" s3) unionsrc order by unionsrc.key;

-- SORT_QUERY_RESULTS

-- 4. Test Select + Join + TS
explain select "c/b/o_t1".key from "c/b/o_t1" join "cbo_/t3////";

-- 5. Test Select + Join + FIL + TS
explain select "c/b/o_t1".c_int, "//cbo_t2".c_int from "c/b/o_t1" join "//cbo_t2" on "c/b/o_t1".key="//cbo_t2".key where ("c/b/o_t1".c_int + "//cbo_t2".c_int == 2) and ("c/b/o_t1".c_int > 0 or "//cbo_t2".c_float >= 0);


-- 7. Test Select + TS + Join + Fil + GB + GB Having + Limit
explain select key, (c_int+1)+2 as x, sum(c_int) from "c/b/o_t1" group by c_float, "c/b/o_t1".c_int, key order by x limit 1;

-- 12. SemiJoin
explain select "c/b/o_t1".c_int           from "c/b/o_t1" left semi join   "//cbo_t2" on "c/b/o_t1".key="//cbo_t2".key;


-- 1. Test Select + TS
explain select * from "c/b/o_t1" as "c/b/o_t1";

-- 2. Test Select + TS + FIL
explain select * from "c/b/o_t1" as "c/b/o_t1"  where "c/b/o_t1".c_int >= 0 and c_float+c_int >= 0 or c_float <= 100;

-- 3 Test Select + Select + TS + FIL
explain select * from (select * from "c/b/o_t1" where "c/b/o_t1".c_int >= 0) as "c/b/o_t1";

-- 13. null expr in select list
explain select null from "cbo_/t3////";

-- 14. unary operator
explain select key from "c/b/o_t1" where c_int = -6  or c_int = +6;

-- 15. query referencing only partition columns
explain select count("c/b/o_t1".dt) from "c/b/o_t1" join "//cbo_t2" on "c/b/o_t1".dt  = "//cbo_t2".dt  where "c/b/o_t1".dt = '2014' ;

-- 20. Test get stats with empty partition list
explain select "c/b/o_t1".value from "c/b/o_t1" join "//cbo_t2" on "c/b/o_t1".key = "//cbo_t2".key where "c/b/o_t1".dt = '10' and "c/b/o_t1".c_boolean = true;



-- 18. SubQueries Not Exists

-- distinct, corr

explain select *

from "src/_/cbo" b

where not exists

  (select distinct a.key

  from "src/_/cbo" a

  where b.value = a.value and a.value > 'val_2'

  )

;



-- no agg, corr, having

explain select *

from "src/_/cbo" b

group by key, value

having not exists

  (select a.key

  from "src/_/cbo" a

  where b.value = a.value  and a.key = b.key and a.value > 'val_12'

  )

;



-- 19. SubQueries Exists

-- view test

create view cv1_n0 as

select *

from "src/_/cbo" b

where exists

  (select a.key

  from "src/_/cbo" a

  where b.value = a.value  and a.key = b.key and a.value > 'val_9')

;



select * from cv1_n0

;



-- sq in from

explain select *

from (select *

      from "src/_/cbo" b

      where exists

          (select a.key

          from "src/_/cbo" a

          where b.value = a.value  and a.key = b.key and a.value > 'val_9')

     ) a

;



-- sq in from, having

explain select *

from (select b.key, count(*)

  from "src/_/cbo" b

  group by b.key

  having exists

    (select a.key

    from "src/_/cbo" a

    where a.key = b.key and a.value > 'val_9'

    )

) a

;



-- 17. SubQueries In

-- non agg, non corr

explain select *

from "src/_/cbo"

where "src/_/cbo".key in (select key from "src/_/cbo" s1 where s1.key > '9') order by key

;



-- agg, corr

-- add back once rank issue fixed for cbo



-- distinct, corr

explain select *

from "src/_/cbo" b

where b.key in

        (select distinct a.key

         from "src/_/cbo" a

         where b.value = a.value and a.key > '9'

        ) order by b.key

;



-- non agg, corr, with join in Parent Query

explain select p.p_partkey, li.l_suppkey

from (select distinct l_partkey as p_partkey from "line/item") p join "line/item" li on p.p_partkey = li.l_partkey

where li.l_linenumber = 1 and

 li.l_orderkey in (select l_orderkey from "line/item" where l_shipmode = 'AIR' and l_linenumber = li.l_linenumber)

 order by p.p_partkey

;



-- where and having

-- Plan is:

-- Stage 1: b semijoin sq1:"src/_/cbo" (subquery in where)

-- Stage 2: group by Stage 1 o/p

-- Stage 5: group by on sq2:"src/_/cbo" (subquery in having)

-- Stage 6: Stage 2 o/p semijoin Stage 5

explain select key, value, count(*)

from "src/_/cbo" b

where b.key in (select key from "src/_/cbo" where "src/_/cbo".key > '8')

group by key, value

having count(*) in (select count(*) from "src/_/cbo" s1 where s1.key > '9' group by s1.key ) order by key

;



-- non agg, non corr, windowing

explain select p_mfgr, p_name, avg(p_size)

from "p/a/r/t"

group by p_mfgr, p_name

having p_name in

  (select first_value(p_name) over(partition by p_mfgr order by p_size) from "p/a/r/t") order by p_mfgr

;



-- 16. SubQueries Not In

-- non agg, non corr

explain select *

from "src/_/cbo"

where "src/_/cbo".key not in

  ( select key  from "src/_/cbo" s1

    where s1.key > '2'

  ) order by key

;



-- non agg, corr

explain select p_mfgr, b.p_name, p_size

from "p/a/r/t" b

where b.p_name not in

  (select p_name

  from (select p_mfgr, p_name, p_size as r from "p/a/r/t") a

  where r < 10 and b.p_mfgr = a.p_mfgr

  ) order by p_mfgr,p_size

;



-- agg, non corr

explain select p_name, p_size

from

"p/a/r/t" where "p/a/r/t".p_size not in

  (select avg(p_size)

  from (select p_size from "p/a/r/t") a

  where p_size < 10

  ) order by p_name

;



-- agg, corr

explain select p_mfgr, p_name, p_size

from "p/a/r/t" b where b.p_size not in

  (select min(p_size)

  from (select p_mfgr, p_size from "p/a/r/t") a

  where p_size < 10 and b.p_mfgr = a.p_mfgr

  ) order by  p_name

;



-- non agg, non corr, Group By in Parent Query

explain select li.l_partkey, count(*)

from "line/item" li

where li.l_linenumber = 1 and

  li.l_orderkey not in (select l_orderkey from "line/item" where l_shipmode = 'AIR')

group by li.l_partkey order by li.l_partkey

;



-- add null check test from sq_notin.q once HIVE-7721 resolved.



-- non agg, corr, having

explain select b.p_mfgr, min(p_retailprice)

from "p/a/r/t" b

group by b.p_mfgr

having b.p_mfgr not in

  (select p_mfgr

  from (select p_mfgr, min(p_retailprice) l, max(p_retailprice) r, avg(p_retailprice) a from "p/a/r/t" group by p_mfgr) a

  where min(p_retailprice) = l and r - l > 600

  )

  order by b.p_mfgr

;



-- agg, non corr, having

explain select b.p_mfgr, min(p_retailprice)

from "p/a/r/t" b

group by b.p_mfgr

having b.p_mfgr not in

  (select p_mfgr

  from "p/a/r/t" a

  group by p_mfgr

  having max(p_retailprice) - min(p_retailprice) > 600

  )

  order by b.p_mfgr

;



-- SORT_QUERY_RESULTS



-- 8. Test UDF/UDAF
explain select f,a,e,b from (select count(*) as a, count(c_int) as b, sum(c_int) as c, avg(c_int) as d, max(c_int) as e, min(c_int) as f from "c/b/o_t1") "c/b/o_t1";



-- SORT_QUERY_RESULTS



-- 11. Union All
explain select * from (select * from "c/b/o_t1" order by key, c_boolean, value, dt)a union all select * from (select * from "//cbo_t2" order by key, c_boolean, value, dt)b;



-- 10. Test views

create view v1_n7 as select c_int, value, c_boolean, dt from "c/b/o_t1";

create view v2_n2 as select c_int, value from "//cbo_t2";



select value from v1_n7 where c_boolean=false;

select max(c_int) from v1_n7 group by (c_boolean);



select count(v1_n7.c_int)  from v1_n7 join "//cbo_t2" on v1_n7.c_int = "//cbo_t2".c_int;

select count(v1_n7.c_int)  from v1_n7 join v2_n2 on v1_n7.c_int = v2_n2.c_int;



select count(*) from v1_n7 a join v1_n7 b on a.value = b.value;



create view v3_n0 as select v1_n7.value val from v1_n7 join "c/b/o_t1" on v1_n7.c_boolean = "c/b/o_t1".c_boolean;



select count(val) from v3_n0 where val != '1';

with q1 as ( select key from "c/b/o_t1" where key = '1')

select count(*) from q1;



with q1 as ( select value from v1_n7 where c_boolean = false)

select count(value) from q1 ;



create view v4_n0 as

with q1 as ( select key,c_int from "c/b/o_t1"  where key = '1')

select * from q1

;



with q1 as ( select c_int from q2 where c_boolean = false),

q2 as ( select c_int,c_boolean from v1_n7  where value = '1')

select sum(c_int) from (select c_int from q1) a;



with q1 as ( select "c/b/o_t1".c_int c_int from q2 join "c/b/o_t1" where q2.c_int = "c/b/o_t1".c_int  and "c/b/o_t1".dt='2014'),

q2 as ( select c_int,c_boolean from v1_n7  where value = '1' or dt = '14')

select count(*) from q1 join q2 join v4_n0 on q1.c_int = q2.c_int and v4_n0.c_int = q2.c_int;





drop view v1_n7;

drop view v2_n2;

drop view v3_n0;

drop view v4_n0;

-- 9. Test Windowing Functions

-- SORT_QUERY_RESULTS

explain select count(c_int) over(partition by c_float order by key), sum(c_float) over(partition by c_float order by key), max(c_int) over(partition by c_float order by key), min(c_int) over(partition by c_float order by key), row_number() over(partition by c_float order by key) as rn, rank() over(partition by c_float order by key), dense_rank() over(partition by c_float order by key), round(percent_rank() over(partition by c_float order by key), 2), lead(c_int, 2, c_int) over(partition by c_float order by key), lag(c_float, 2, c_float) over(partition by c_float order by key) from "c/b/o_t1" order by rn;



insert into table "src/_/cbo" select * from default.src;

explain select * from "src/_/cbo" limit 1;

insert overwrite table "src/_/cbo" select * from default.src;

explain select * from "src/_/cbo" limit 1;

drop table "t//";
create table "t//" (col string);
insert into "t//" values(1);
insert into "t//" values(null);
analyze table "t//" compute statistics;
explain select * from "t//";

drop database "db~!@@#$%^&*(),<>" cascade;
