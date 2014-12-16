set hive.cbo.enable=true;
set hive.exec.check.crossproducts=false;

drop table if exists t1;
drop table if exists t2;
drop table if exists t3;

create table t1(key string, value string, c_int int, c_float float, c_boolean boolean)  partitioned by (dt string) row format delimited fields terminated by ',' STORED AS TEXTFILE;
create table t2(key string, value string, c_int int, c_float float, c_boolean boolean)  partitioned by (dt string) row format delimited fields terminated by ',' STORED AS TEXTFILE;
create table t3(key string, value string, c_int int, c_float float, c_boolean boolean)  row format delimited fields terminated by ',' STORED AS TEXTFILE;

load data local inpath '../../data/files/cbo_t1.txt' into table t1 partition (dt='2014');
load data local inpath '../../data/files/cbo_t2.txt' into table t2 partition (dt='2014');
load data local inpath '../../data/files/cbo_t3.txt' into table t3;

CREATE TABLE part( 
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

LOAD DATA LOCAL INPATH '../../data/files/part_tiny.txt' overwrite into table part;

DROP TABLE lineitem;
CREATE TABLE lineitem (L_ORDERKEY      INT,
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

LOAD DATA LOCAL INPATH '../../data/files/lineitem.txt' OVERWRITE INTO TABLE lineitem;

create table src_cbo as select * from src;


set hive.stats.dbclass=jdbc:derby;
analyze table t1 partition (dt) compute statistics;
analyze table t1 compute statistics for columns key, value, c_int, c_float, c_boolean;
analyze table t2 partition (dt) compute statistics;
analyze table t2 compute statistics for columns key, value, c_int, c_float, c_boolean;
analyze table t3 compute statistics;
analyze table t3 compute statistics for columns key, value, c_int, c_float, c_boolean;
analyze table src_cbo compute statistics;
analyze table src_cbo compute statistics for columns;
analyze table part compute statistics;
analyze table part compute statistics for columns;
analyze table lineitem compute statistics;
analyze table lineitem compute statistics for columns;

set hive.stats.fetch.column.stats=true;
set hive.auto.convert.join=false;

-- 1. Test Select + TS
select * from t1;
select * from t1 as t1;
select * from t1 as t2;

select t1.key as x, c_int as c_int, (((c_int+c_float)*10)+5) as y from t1; 

-- 2. Test Select + TS + FIL
select * from t1 where t1.c_int >= 0;
select * from t1 as t1  where t1.c_int >= 0 and c_float+c_int >= 0 or c_float <= 100;
select * from t1 as t2 where t2.c_int >= 0 and c_float+c_int >= 0 or c_float <= 100;

select t2.key as x, c_int as c_int, (((c_int+c_float)*10)+5) as y from t1 as t2  where t2.c_int >= 0 and c_float+c_int >= 0 or c_float <= 100;

-- 3 Test Select + Select + TS + FIL
select * from (select * from t1 where t1.c_int >= 0) as t1;
select * from (select * from t1 as t1  where t1.c_int >= 0 and c_float+c_int >= 0 or c_float <= 100) as t1;
select * from (select * from t1 as t2 where t2.c_int >= 0 and c_float+c_int >= 0 or c_float <= 100) as t1;
select * from (select t2.key as x, c_int as c_int, (((c_int+c_float)*10)+5) as y from t1 as t2  where t2.c_int >= 0 and c_float+c_int >= 0 or c_float <= 100) as t1;

select * from (select * from t1 where t1.c_int >= 0) as t1 where t1.c_int >= 0;
select * from (select * from t1 as t1  where t1.c_int >= 0 and c_float+c_int >= 0 or c_float <= 100) as t1  where t1.c_int >= 0 and c_float+c_int >= 0 or c_float <= 100;
select * from (select * from t1 as t2 where t2.c_int >= 0 and c_float+c_int >= 0 or c_float <= 100) as t2 where t2.c_int >= 0 and c_float+c_int >= 0 or c_float <= 100;
select * from (select t2.key as x, c_int as c_int, (((c_int+c_float)*10)+5) as y from t1 as t2  where t2.c_int >= 0 and c_float+c_int >= 0 or c_float <= 100) as t1 where t1.c_int >= 0 and y+c_int >= 0 or x <= 100;

select t1.c_int+c_float as x , c_int as c_int, (((c_int+c_float)*10)+5) as y from (select * from t1 where t1.c_int >= 0) as t1 where t1.c_int >= 0;
select t2.c_int+c_float as x , c_int as c_int, (((c_int+c_float)*10)+5) as y from (select * from t1 where t1.c_int >= 0) as t2 where t2.c_int >= 0;

-- 4. Test Select + Join + TS
select t1.c_int, t2.c_int from t1 join             t2 on t1.key=t2.key;
select t1.key from t1 join t3;
select t1.key from t1 join t3 where t1.key=t3.key and t1.key >= 1;
select t1.c_int, t2.c_int from t1 left outer join  t2 on t1.key=t2.key;
select t1.c_int, t2.c_int from t1 right outer join t2 on t1.key=t2.key;
select t1.c_int, t2.c_int from t1 full outer join  t2 on t1.key=t2.key;

select b, t1.c, t2.p, q, t3.c_int from (select key as a, c_int as b, t1.c_float as c from t1) t1 join (select t2.key as p, t2.c_int as q, c_float as r from t2) t2 on t1.a=p join t3 on t1.a=key;
select key, t1.c_int, t2.p, q from t1 join (select t2.key as p, t2.c_int as q, c_float as r from t2) t2 on t1.key=p join (select key as a, c_int as b, t3.c_float as c from t3)t3 on t1.key=a;
select a, t1.b, key, t2.c_int, t3.p from (select key as a, c_int as b, t1.c_float as c from t1) t1 join t2  on t1.a=key join (select key as p, c_int as q, t3.c_float as r from t3)t3 on t1.a=t3.p;
select b, t1.c, t2.c_int, t3.c_int from (select key as a, c_int as b, t1.c_float as c from t1) t1 join t2 on t1.a=t2.key join t3 on t1.a=t3.key;
select t3.c_int, b, t2.c_int, t1.c from (select key as a, c_int as b, t1.c_float as c from t1) t1 join t2 on t1.a=t2.key join t3 on t1.a=t3.key;

select b, t1.c, t2.p, q, t3.c_int from (select key as a, c_int as b, t1.c_float as c from t1) t1 left outer join (select t2.key as p, t2.c_int as q, c_float as r from t2) t2 on t1.a=p join t3 on t1.a=key;
select key, t1.c_int, t2.p, q from t1 join (select t2.key as p, t2.c_int as q, c_float as r from t2) t2 on t1.key=p left outer join (select key as a, c_int as b, t3.c_float as c from t3)t3 on t1.key=a;

select b, t1.c, t2.p, q, t3.c_int from (select key as a, c_int as b, t1.c_float as c from t1) t1 right outer join (select t2.key as p, t2.c_int as q, c_float as r from t2) t2 on t1.a=p join t3 on t1.a=key;
select key, t1.c_int, t2.p, q from t1 join (select t2.key as p, t2.c_int as q, c_float as r from t2) t2 on t1.key=p right outer join (select key as a, c_int as b, t3.c_float as c from t3)t3 on t1.key=a;

select b, t1.c, t2.p, q, t3.c_int from (select key as a, c_int as b, t1.c_float as c from t1) t1 full outer join (select t2.key as p, t2.c_int as q, c_float as r from t2) t2 on t1.a=p join t3 on t1.a=key;
select key, t1.c_int, t2.p, q from t1 join (select t2.key as p, t2.c_int as q, c_float as r from t2) t2 on t1.key=p full outer join (select key as a, c_int as b, t3.c_float as c from t3)t3 on t1.key=a;

-- 5. Test Select + Join + FIL + TS
select t1.c_int, t2.c_int from t1 join t2 on t1.key=t2.key where (t1.c_int + t2.c_int == 2) and (t1.c_int > 0 or t2.c_float >= 0);
select t1.c_int, t2.c_int from t1 left outer join  t2 on t1.key=t2.key where (t1.c_int + t2.c_int == 2) and (t1.c_int > 0 or t2.c_float >= 0);
select t1.c_int, t2.c_int from t1 right outer join t2 on t1.key=t2.key where (t1.c_int + t2.c_int == 2) and (t1.c_int > 0 or t2.c_float >= 0);
select t1.c_int, t2.c_int from t1 full outer join  t2 on t1.key=t2.key where (t1.c_int + t2.c_int == 2) and (t1.c_int > 0 or t2.c_float >= 0);

select b, t1.c, t2.p, q, t3.c_int from (select key as a, c_int as b, t1.c_float as c from t1  where (t1.c_int + 1 == 2) and (t1.c_int > 0 or t1.c_float >= 0)) t1 join (select t2.key as p, t2.c_int as q, c_float as r from t2  where (t2.c_int + 1 == 2) and (t2.c_int > 0 or t2.c_float >= 0)) t2 on t1.a=p join t3 on t1.a=key where (b + t2.q == 2) and (b > 0 or t2.q >= 0);

select q, b, t2.p, t1.c, t3.c_int from (select key as a, c_int as b, t1.c_float as c from t1  where (t1.c_int + 1 == 2) and (t1.c_int > 0 or t1.c_float >= 0)) t1 left outer join (select t2.key as p, t2.c_int as q, c_float as r from t2  where (t2.c_int + 1 == 2) and (t2.c_int > 0 or t2.c_float >= 0)) t2 on t1.a=p join t3 on t1.a=key where (b + t2.q == 2) and (b > 0 or c_int >= 0);



select q, b, t2.p, t1.c, t3.c_int from (select key as a, c_int as b, t1.c_float as c from t1  where (t1.c_int + 1 == 2) and (t1.c_int > 0 or t1.c_float >= 0)) t1 right outer join (select t2.key as p, t2.c_int as q, c_float as r from t2  where (t2.c_int + 1 == 2) and (t2.c_int > 0 or t2.c_float >= 0)) t2 on t1.a=p join t3 on t1.a=key where (b + t2.q == 2) and (b > 0 or c_int >= 0);

select q, b, t2.p, t1.c, t3.c_int from (select key as a, c_int as b, t1.c_float as c from t1  where (t1.c_int + 1 == 2) and (t1.c_int > 0 or t1.c_float >= 0)) t1 full outer join (select t2.key as p, t2.c_int as q, c_float as r from t2  where (t2.c_int + 1 == 2) and (t2.c_int > 0 or t2.c_float >= 0)) t2 on t1.a=p join t3 on t1.a=key where (b + t2.q == 2) and (b > 0 or c_int >= 0);

select * from (select q, b, t2.p, t1.c, t3.c_int from (select key as a, c_int as b, t1.c_float as c from t1  where (t1.c_int + 1 == 2) and (t1.c_int > 0 or t1.c_float >= 0)) t1 full outer join (select t2.key as p, t2.c_int as q, c_float as r from t2  where (t2.c_int + 1 == 2) and (t2.c_int > 0 or t2.c_float >= 0)) t2 on t1.a=p join t3 on t1.a=key where (b + t2.q == 2) and (b > 0 or c_int >= 0)) R where  (q + 1 = 2) and (R.b > 0 or c_int >= 0);


select * from (select q, b, t2.p, t1.c, t3.c_int from (select key as a, c_int as b, t1.c_float as c from t1  where (t1.c_int + 1 == 2) and (t1.c_int > 0 or t1.c_float >= 0)) t1 left outer join (select t2.key as p, t2.c_int as q, c_float as r from t2  where (t2.c_int + 1 == 2) and (t2.c_int > 0 or t2.c_float >= 0)) t2 on t1.a=p left outer join t3 on t1.a=key where (b + t2.q == 2) and (b > 0 or c_int >= 0)) R where  (q + 1 = 2) and (R.b > 0 or c_int >= 0);

select * from (select q, b, t2.p, t1.c, t3.c_int from (select key as a, c_int as b, t1.c_float as c from t1  where (t1.c_int + 1 == 2) and (t1.c_int > 0 or t1.c_float >= 0)) t1 left outer join (select t2.key as p, t2.c_int as q, c_float as r from t2  where (t2.c_int + 1 == 2) and (t2.c_int > 0 or t2.c_float >= 0)) t2 on t1.a=p right outer join t3 on t1.a=key where (b + t2.q == 2) and (b > 0 or c_int >= 0)) R where  (q + 1 = 2) and (R.b > 0 or c_int >= 0);

select * from (select q, b, t2.p, t1.c, t3.c_int from (select key as a, c_int as b, t1.c_float as c from t1  where (t1.c_int + 1 == 2) and (t1.c_int > 0 or t1.c_float >= 0)) t1 left outer join (select t2.key as p, t2.c_int as q, c_float as r from t2  where (t2.c_int + 1 == 2) and (t2.c_int > 0 or t2.c_float >= 0)) t2 on t1.a=p full outer join t3 on t1.a=key where (b + t2.q == 2) and (b > 0 or c_int >= 0)) R where  (q + 1 = 2) and (R.b > 0 or c_int >= 0);



select * from (select q, b, t2.p, t1.c, t3.c_int from (select key as a, c_int as b, t1.c_float as c from t1  where (t1.c_int + 1 == 2) and (t1.c_int > 0 or t1.c_float >= 0)) t1 right outer join (select t2.key as p, t2.c_int as q, c_float as r from t2  where (t2.c_int + 1 == 2) and (t2.c_int > 0 or t2.c_float >= 0)) t2 on t1.a=p right outer join t3 on t1.a=key where (b + t2.q == 2) and (b > 0 or c_int >= 0)) R where  (q + 1 = 2) and (R.b > 0 or c_int >= 0);

select * from (select q, b, t2.p, t1.c, t3.c_int from (select key as a, c_int as b, t1.c_float as c from t1  where (t1.c_int + 1 == 2) and (t1.c_int > 0 or t1.c_float >= 0)) t1 right outer join (select t2.key as p, t2.c_int as q, c_float as r from t2  where (t2.c_int + 1 == 2) and (t2.c_int > 0 or t2.c_float >= 0)) t2 on t1.a=p left outer join t3 on t1.a=key where (b + t2.q == 2) and (b > 0 or c_int >= 0)) R where  (q + 1 = 2) and (R.b > 0 or c_int >= 0);

select * from (select q, b, t2.p, t1.c, t3.c_int from (select key as a, c_int as b, t1.c_float as c from t1  where (t1.c_int + 1 == 2) and (t1.c_int > 0 or t1.c_float >= 0)) t1 right outer join (select t2.key as p, t2.c_int as q, c_float as r from t2  where (t2.c_int + 1 == 2) and (t2.c_int > 0 or t2.c_float >= 0)) t2 on t1.a=p full outer join t3 on t1.a=key where (b + t2.q == 2) and (b > 0 or c_int >= 0)) R where  (q + 1 = 2) and (R.b > 0 or c_int >= 0);



select * from (select q, b, t2.p, t1.c, t3.c_int from (select key as a, c_int as b, t1.c_float as c from t1  where (t1.c_int + 1 == 2) and (t1.c_int > 0 or t1.c_float >= 0)) t1 full outer join (select t2.key as p, t2.c_int as q, c_float as r from t2  where (t2.c_int + 1 == 2) and (t2.c_int > 0 or t2.c_float >= 0)) t2 on t1.a=p full outer join t3 on t1.a=key where (b + t2.q == 2) and (b > 0 or c_int >= 0)) R where  (q + 1 = 2) and (R.b > 0 or c_int >= 0);

select * from (select q, b, t2.p, t1.c, t3.c_int from (select key as a, c_int as b, t1.c_float as c from t1  where (t1.c_int + 1 == 2) and (t1.c_int > 0 or t1.c_float >= 0)) t1 full outer join (select t2.key as p, t2.c_int as q, c_float as r from t2  where (t2.c_int + 1 == 2) and (t2.c_int > 0 or t2.c_float >= 0)) t2 on t1.a=p left outer join t3 on t1.a=key where (b + t2.q == 2) and (b > 0 or c_int >= 0)) R where  (q + 1 = 2) and (R.b > 0 or c_int >= 0);

select * from (select q, b, t2.p, t1.c, t3.c_int from (select key as a, c_int as b, t1.c_float as c from t1  where (t1.c_int + 1 == 2) and (t1.c_int > 0 or t1.c_float >= 0)) t1 full outer join (select t2.key as p, t2.c_int as q, c_float as r from t2  where (t2.c_int + 1 == 2) and (t2.c_int > 0 or t2.c_float >= 0)) t2 on t1.a=p right outer join t3 on t1.a=key where (b + t2.q == 2) and (b > 0 or c_int >= 0)) R where  (q + 1 = 2) and (R.b > 0 or c_int >= 0);


-- 6. Test Select + TS + Join + Fil + GB + GB Having
select * from t1 group by c_int;
select key, (c_int+1)+2 as x, sum(c_int) from t1 group by c_float, t1.c_int, key;
select * from (select key, (c_int+c_float+1+2) as x, sum(c_int) as y from t1 group by c_float, t1.c_int, key) R group by y, x;

select * from (select key as a, c_int+1 as b, sum(c_int) as c from t1 where (t1.c_int + 1 >= 0) and (t1.c_int > 0 or t1.c_float >= 0) group by c_float, t1.c_int, key order by a) t1 join (select key as p, c_int+1 as q, sum(c_int) as r from t2 where (t2.c_int + 1 >= 0) and (t2.c_int > 0 or t2.c_float >= 0)  group by c_float, t2.c_int, key order by q/10 desc, r asc) t2 on t1.a=p join t3 on t1.a=key where (b + t2.q >= 0) and (b > 0 or c_int >= 0) group by t3.c_int, c order by t3.c_int+c desc, c;

select * from (select key as a, c_int+1 as b, sum(c_int) as c from t1 where (t1.c_int + 1 >= 0) and (t1.c_int > 0 or t1.c_float >= 0)  group by c_float, t1.c_int, key having t1.c_float > 0 and (c_int >=1 or c_float >= 1) and (c_int + c_float) >= 0 order by b % c asc, b desc) t1 left outer join (select key as p, c_int+1 as q, sum(c_int) as r from t2 where (t2.c_int + 1 >= 0) and (t2.c_int > 0 or t2.c_float >= 0)  group by c_float, t2.c_int, key  having t2.c_float > 0 and (c_int >=1 or c_float >= 1) and (c_int + c_float) >= 0) t2 on t1.a=p left outer join t3 on t1.a=key where (b + t2.q >= 0) and (b > 0 or c_int >= 0) group by t3.c_int, c  having t3.c_int > 0 and (c_int >=1 or c >= 1) and (c_int + c) >= 0  order by t3.c_int % c asc, t3.c_int desc;

select * from (select key as a, c_int+1 as b, sum(c_int) as c from t1 where (t1.c_int + 1 >= 0) and (t1.c_int > 0 or t1.c_float >= 0)  group by c_float, t1.c_int, key having t1.c_float > 0 and (c_int >=1 or c_float >= 1) and (c_int + c_float) >= 0 order by b+c, a desc) t1 right outer join (select key as p, c_int+1 as q, sum(c_int) as r from t2 where (t2.c_int + 1 >= 0) and (t2.c_int > 0 or t2.c_float >= 0)  group by c_float, t2.c_int, key having t2.c_float > 0 and (c_int >=1 or c_float >= 1) and (c_int + c_float) >= 0) t2 on t1.a=p right outer join t3 on t1.a=key where (b + t2.q >= 2) and (b > 0 or c_int >= 0) group by t3.c_int, c;

select * from (select key as a, c_int+1 as b, sum(c_int) as c from t1 where (t1.c_int + 1 >= 0) and (t1.c_int > 0 or t1.c_float >= 0)  group by c_float, t1.c_int, key having t1.c_float > 0 and (c_int >=1 or c_float >= 1) and (c_int + c_float) >= 0 order by c+a desc) t1 full outer join (select key as p, c_int+1 as q, sum(c_int) as r from t2 where (t2.c_int + 1 >= 0) and (t2.c_int > 0 or t2.c_float >= 0)  group by c_float, t2.c_int, key having t2.c_float > 0 and (c_int >=1 or c_float >= 1) and (c_int + c_float) >= 0 order by p+q desc, r asc) t2 on t1.a=p full outer join t3 on t1.a=key where (b + t2.q >= 0) and (b > 0 or c_int >= 0) group by t3.c_int, c having t3.c_int > 0 and (c_int >=1 or c >= 1) and (c_int + c) >= 0 order by t3.c_int;

select * from (select key as a, c_int+1 as b, sum(c_int) as c from t1 where (t1.c_int + 1 >= 0) and (t1.c_int > 0 or t1.c_float >= 0)  group by c_float, t1.c_int, key having t1.c_float > 0 and (c_int >=1 or c_float >= 1) and (c_int + c_float) >= 0) t1 join (select key as p, c_int+1 as q, sum(c_int) as r from t2 where (t2.c_int + 1 >= 0) and (t2.c_int > 0 or t2.c_float >= 0)  group by c_float, t2.c_int, key having t2.c_float > 0 and (c_int >=1 or c_float >= 1) and (c_int + c_float) >= 0) t2 on t1.a=p join t3 on t1.a=key where (b + t2.q >= 0) and (b > 0 or c_int >= 0) group by t3.c_int, c;

-- 7. Test Select + TS + Join + Fil + GB + GB Having + Limit
select * from t1 group by c_int limit 1;
select key, (c_int+1)+2 as x, sum(c_int) from t1 group by c_float, t1.c_int, key order by x limit 1;
select * from (select key, (c_int+c_float+1+2) as x, sum(c_int) as y from t1 group by c_float, t1.c_int, key) R group by y, x order by x,y limit 1;
select key from(select key from (select key from t1 limit 5)t2  limit 5)t3  limit 5;
select key, c_int from(select key, c_int from (select key, c_int from t1 order by c_int limit 5)t1  order by c_int limit 5)t2  order by c_int limit 5;

select * from (select key as a, c_int+1 as b, sum(c_int) as c from t1 where (t1.c_int + 1 >= 0) and (t1.c_int > 0 or t1.c_float >= 0) group by c_float, t1.c_int, key order by a limit 5) t1 join (select key as p, c_int+1 as q, sum(c_int) as r from t2 where (t2.c_int + 1 >= 0) and (t2.c_int > 0 or t2.c_float >= 0)  group by c_float, t2.c_int, key order by q/10 desc, r asc limit 5) t2 on t1.a=p join t3 on t1.a=key where (b + t2.q >= 0) and (b > 0 or c_int >= 0) group by t3.c_int, c order by t3.c_int+c desc, c limit 5;

select * from (select key as a, c_int+1 as b, sum(c_int) as c from t1 where (t1.c_int + 1 >= 0) and (t1.c_int > 0 or t1.c_float >= 0)  group by c_float, t1.c_int, key having t1.c_float > 0 and (c_int >=1 or c_float >= 1) and (c_int + c_float) >= 0 order by b % c asc, b desc limit 5) t1 left outer join (select key as p, c_int+1 as q, sum(c_int) as r from t2 where (t2.c_int + 1 >= 0) and (t2.c_int > 0 or t2.c_float >= 0)  group by c_float, t2.c_int, key  having t2.c_float > 0 and (c_int >=1 or c_float >= 1) and (c_int + c_float) >= 0 limit 5) t2 on t1.a=p left outer join t3 on t1.a=key where (b + t2.q >= 0) and (b > 0 or c_int >= 0) group by t3.c_int, c  having t3.c_int > 0 and (c_int >=1 or c >= 1) and (c_int + c) >= 0  order by t3.c_int % c asc, t3.c_int desc limit 5;

-- 8. Test UDF/UDAF
select count(*), count(c_int), sum(c_int), avg(c_int), max(c_int), min(c_int) from t1;
select count(*), count(c_int), sum(c_int), avg(c_int), max(c_int), min(c_int), case c_int when 0  then 1 when 1 then 2 else 3 end, sum(case c_int when 0  then 1 when 1 then 2 else 3 end) from t1 group by c_int;
select * from (select count(*) as a, count(distinct c_int) as b, sum(c_int) as c, avg(c_int) as d, max(c_int) as e, min(c_int) as f from t1) t1;
select * from (select count(*) as a, count(distinct c_int) as b, sum(c_int) as c, avg(c_int) as d, max(c_int) as e, min(c_int) as f, case c_int when 0  then 1 when 1 then 2 else 3 end as g, sum(case c_int when 0  then 1 when 1 then 2 else 3 end) as h from t1 group by c_int) t1;
select f,a,e,b from (select count(*) as a, count(c_int) as b, sum(c_int) as c, avg(c_int) as d, max(c_int) as e, min(c_int) as f from t1) t1;
select f,a,e,b from (select count(*) as a, count(distinct c_int) as b, sum(distinct c_int) as c, avg(distinct c_int) as d, max(distinct c_int) as e, min(distinct c_int) as f from t1) t1;
select count(c_int) as a, avg(c_float), key from t1 group by key;
select count(distinct c_int) as a, avg(c_float) from t1 group by c_float;
select count(distinct c_int) as a, avg(c_float) from t1 group by c_int;
select count(distinct c_int) as a, avg(c_float) from t1 group by c_float, c_int;

-- 9. Test Windowing Functions
select count(c_int) over() from t1;
select count(c_int) over(), sum(c_float) over(), max(c_int) over(), min(c_int) over(), row_number() over(), rank() over(), dense_rank() over(), percent_rank() over(), lead(c_int, 2, c_int) over(), lag(c_float, 2, c_float) over() from t1;
select * from (select count(c_int) over(), sum(c_float) over(), max(c_int) over(), min(c_int) over(), row_number() over(), rank() over(), dense_rank() over(), percent_rank() over(), lead(c_int, 2, c_int) over(), lag(c_float, 2, c_float) over() from t1) t1;
select x from (select count(c_int) over() as x, sum(c_float) over() from t1) t1;
select 1+sum(c_int) over() from t1;
select sum(c_int)+sum(sum(c_int)) over() from t1;
select * from (select max(c_int) over (partition by key order by value Rows UNBOUNDED PRECEDING), min(c_int) over (partition by key order by value rows current row), count(c_int) over(partition by key order by value ROWS 1 PRECEDING), avg(value) over (partition by key order by value Rows between unbounded preceding and unbounded following), sum(value) over (partition by key order by value rows between unbounded preceding and current row), avg(c_float) over (partition by key order by value Rows between 1 preceding and unbounded following), sum(c_float) over (partition by key order by value rows between 1 preceding and current row), max(c_float) over (partition by key order by value rows between 1 preceding and unbounded following), min(c_float) over (partition by key order by value rows between 1 preceding and 1 following) from t1) t1;
select i, a, h, b, c, d, e, f, g, a as x, a +1 as y from (select max(c_int) over (partition by key order by value range UNBOUNDED PRECEDING) a, min(c_int) over (partition by key order by value range current row) b, count(c_int) over(partition by key order by value range 1 PRECEDING) c, avg(value) over (partition by key order by value range between unbounded preceding and unbounded following) d, sum(value) over (partition by key order by value range between unbounded preceding and current row) e, avg(c_float) over (partition by key order by value range between 1 preceding and unbounded following) f, sum(c_float) over (partition by key order by value range between 1 preceding and current row) g, max(c_float) over (partition by key order by value range between 1 preceding and unbounded following) h, min(c_float) over (partition by key order by value range between 1 preceding and 1 following) i from t1) t1;

-- 10. Test views
create view v1 as select c_int, value, c_boolean, dt from t1;
create view v2 as select c_int, value from t2;

select value from v1 where c_boolean=false;
select max(c_int) from v1 group by (c_boolean);

select count(v1.c_int)  from v1 join t2 on v1.c_int = t2.c_int;
select count(v1.c_int)  from v1 join v2 on v1.c_int = v2.c_int;

select count(*) from v1 a join v1 b on a.value = b.value;

create view v3 as select v1.value val from v1 join t1 on v1.c_boolean = t1.c_boolean;

select count(val) from v3 where val != '1';
with q1 as ( select key from t1 where key = '1')
select count(*) from q1;

with q1 as ( select value from v1 where c_boolean = false)
select count(value) from q1 ;

create view v4 as
with q1 as ( select key,c_int from t1  where key = '1')
select * from q1
;

with q1 as ( select c_int from q2 where c_boolean = false),
q2 as ( select c_int,c_boolean from v1  where value = '1')
select sum(c_int) from (select c_int from q1) a;

with q1 as ( select t1.c_int c_int from q2 join t1 where q2.c_int = t1.c_int  and t1.dt='2014'),
q2 as ( select c_int,c_boolean from v1  where value = '1' or dt = '14')
select count(*) from q1 join q2 join v4 on q1.c_int = q2.c_int and v4.c_int = q2.c_int;


drop view v1;
drop view v2;
drop view v3;
drop view v4;

-- 11. Union All
select * from (
select * from t1 order by key, c_boolean, value, c_int, c_float, dt
union all
select * from t2 order by key, c_boolean, value, c_int, c_float, dt) a
order by key, c_boolean, value, c_int, c_float, dt;

select key from (select key, c_int from (select * from t1 union all select * from t2 where t2.key >=0)r1 union all select key, c_int from t3)r2 where key >=0 order by key;
select r2.key from (select key, c_int from (select key, c_int from t1 union all select key, c_int from t3 )r1 union all select key, c_int from t3)r2 join   (select key, c_int from (select * from t1 union all select * from t2 where t2.key >=0)r1 union all select key, c_int from t3)r3 on r2.key=r3.key where r3.key >=0 order by r2.key;

-- 12. SemiJoin
select t1.c_int           from t1 left semi join   t2 on t1.key=t2.key;
select t1.c_int           from t1 left semi join   t2 on t1.key=t2.key where (t1.c_int + 1 == 2) and (t1.c_int > 0 or t1.c_float >= 0);
select * from (select c, b, a from (select key as a, c_int as b, t1.c_float as c from t1  where (t1.c_int + 1 == 2) and (t1.c_int > 0 or t1.c_float >= 0)) t1 left semi join (select t2.key as p, t2.c_int as q, c_float as r from t2  where (t2.c_int + 1 == 2) and (t2.c_int > 0 or t2.c_float >= 0)) t2 on t1.a=p left semi join t3 on t1.a=key where (b + 1 == 2) and (b > 0 or c >= 0)) R where  (b + 1 = 2) and (R.b > 0 or c >= 0);
select * from (select t3.c_int, t1.c, b from (select key as a, c_int as b, t1.c_float as c from t1  where (t1.c_int + 1 = 2) and (t1.c_int > 0 or t1.c_float >= 0)) t1 left semi join (select t2.key as p, t2.c_int as q, c_float as r from t2  where (t2.c_int + 1 == 2) and (t2.c_int > 0 or t2.c_float >= 0)) t2 on t1.a=p left outer join t3 on t1.a=key where (b + t3.c_int  == 2) and (b > 0 or c_int >= 0)) R where  (R.c_int + 1 = 2) and (R.b > 0 or c_int >= 0);
select * from (select c_int, b, t1.c from (select key as a, c_int as b, t1.c_float as c from t1  where (t1.c_int + 1 == 2) and (t1.c_int > 0 or t1.c_float >= 0)) t1 left semi join (select t2.key as p, t2.c_int as q, c_float as r from t2  where (t2.c_int + 1 == 2) and (t2.c_int > 0 or t2.c_float >= 0)) t2 on t1.a=p right outer join t3 on t1.a=key where (b + 1 == 2) and (b > 0 or c_int >= 0)) R where  (c + 1 = 2) and (R.b > 0 or c_int >= 0);
select * from (select c_int, b, t1.c from (select key as a, c_int as b, t1.c_float as c from t1  where (t1.c_int + 1 == 2) and (t1.c_int > 0 or t1.c_float >= 0)) t1 left semi join (select t2.key as p, t2.c_int as q, c_float as r from t2  where (t2.c_int + 1 == 2) and (t2.c_int > 0 or t2.c_float >= 0)) t2 on t1.a=p full outer join t3 on t1.a=key where (b + 1 == 2) and (b > 0 or c_int >= 0)) R where  (c + 1 = 2) and (R.b > 0 or c_int >= 0);
select * from (select key as a, c_int+1 as b, sum(c_int) as c from t1 where (t1.c_int + 1 >= 0) and (t1.c_int > 0 or t1.c_float >= 0)  group by c_float, t1.c_int, key having t1.c_float > 0 and (c_int >=1 or c_float >= 1) and (c_int + c_float) >= 0 order by a+b desc, c asc) t1 left semi join (select key as p, c_int+1 as q, sum(c_int) as r from t2 where (t2.c_int + 1 >= 0) and (t2.c_int > 0 or t2.c_float >= 0)  group by c_float, t2.c_int, key having t2.c_float > 0 and (c_int >=1 or c_float >= 1) and (c_int + c_float) >= 0 order by q+r/10 desc, p) t2 on t1.a=p left semi join t3 on t1.a=key where (b + 1  >= 0) and (b > 0 or a >= 0) group by a, c  having a > 0 and (a >=1 or c >= 1) and (a + c) >= 0 order by c, a;
select * from (select key as a, c_int+1 as b, sum(c_int) as c from t1 where (t1.c_int + 1 >= 0) and (t1.c_int > 0 or t1.c_float >= 0)  group by c_float, t1.c_int, key having t1.c_float > 0 and (c_int >=1 or c_float >= 1) and (c_int + c_float) >= 0 order by a+b desc, c asc limit 5) t1 left semi join (select key as p, c_int+1 as q, sum(c_int) as r from t2 where (t2.c_int + 1 >= 0) and (t2.c_int > 0 or t2.c_float >= 0)  group by c_float, t2.c_int, key having t2.c_float > 0 and (c_int >=1 or c_float >= 1) and (c_int + c_float) >= 0 order by q+r/10 desc, p limit 5) t2 on t1.a=p left semi join t3 on t1.a=key where (b + 1  >= 0) and (b > 0 or a >= 0) group by a, c  having a > 0 and (a >=1 or c >= 1) and (a + c) >= 0 order by c, a;

-- 13. null expr in select list
select null from t3;

-- 14. unary operator
select key from t1 where c_int = -6  or c_int = +6;

-- 15. query referencing only partition columns
select count(t1.dt) from t1 join t2 on t1.dt  = t2.dt  where t1.dt = '2014' ;

-- 16. SubQueries Not In
-- non agg, non corr
select * 
from src_cbo 
where src_cbo.key not in  
  ( select key  from src_cbo s1 
    where s1.key > '2'
  ) order by key
;

-- non agg, corr
select p_mfgr, b.p_name, p_size 
from part b 
where b.p_name not in 
  (select p_name 
  from (select p_mfgr, p_name, p_size as r from part) a 
  where r < 10 and b.p_mfgr = a.p_mfgr 
  )
;

-- agg, non corr
select p_name, p_size 
from 
part where part.p_size not in 
  (select avg(p_size) 
  from (select p_size from part) a 
  where p_size < 10
  ) order by p_name
;

-- agg, corr
select p_mfgr, p_name, p_size 
from part b where b.p_size not in 
  (select min(p_size) 
  from (select p_mfgr, p_size from part) a 
  where p_size < 10 and b.p_mfgr = a.p_mfgr
  ) order by  p_name
;

-- non agg, non corr, Group By in Parent Query
select li.l_partkey, count(*) 
from lineitem li 
where li.l_linenumber = 1 and 
  li.l_orderkey not in (select l_orderkey from lineitem where l_shipmode = 'AIR') 
group by li.l_partkey
;

-- add null check test from sq_notin.q once HIVE-7721 resolved.

-- non agg, corr, having
select b.p_mfgr, min(p_retailprice) 
from part b 
group by b.p_mfgr
having b.p_mfgr not in 
  (select p_mfgr 
  from (select p_mfgr, min(p_retailprice) l, max(p_retailprice) r, avg(p_retailprice) a from part group by p_mfgr) a 
  where min(p_retailprice) = l and r - l > 600
  )
  order by b.p_mfgr
;

-- agg, non corr, having
select b.p_mfgr, min(p_retailprice) 
from part b 
group by b.p_mfgr
having b.p_mfgr not in 
  (select p_mfgr 
  from part a
  group by p_mfgr
  having max(p_retailprice) - min(p_retailprice) > 600
  )
  order by b.p_mfgr  
;

-- 17. SubQueries In
-- non agg, non corr
select * 
from src_cbo 
where src_cbo.key in (select key from src_cbo s1 where s1.key > '9')
;

-- agg, corr
-- add back once rank issue fixed for cbo

-- distinct, corr
select * 
from src_cbo b 
where b.key in
        (select distinct a.key 
         from src_cbo a 
         where b.value = a.value and a.key > '9'
        )
;

-- non agg, corr, with join in Parent Query
select p.p_partkey, li.l_suppkey 
from (select distinct l_partkey as p_partkey from lineitem) p join lineitem li on p.p_partkey = li.l_partkey 
where li.l_linenumber = 1 and
 li.l_orderkey in (select l_orderkey from lineitem where l_shipmode = 'AIR' and l_linenumber = li.l_linenumber)
;

-- where and having
-- Plan is:
-- Stage 1: b semijoin sq1:src_cbo (subquery in where)
-- Stage 2: group by Stage 1 o/p
-- Stage 5: group by on sq2:src_cbo (subquery in having)
-- Stage 6: Stage 2 o/p semijoin Stage 5
select key, value, count(*) 
from src_cbo b
where b.key in (select key from src_cbo where src_cbo.key > '8')
group by key, value
having count(*) in (select count(*) from src_cbo s1 where s1.key > '9' group by s1.key )
;

-- non agg, non corr, windowing
select p_mfgr, p_name, avg(p_size) 
from part 
group by p_mfgr, p_name
having p_name in 
  (select first_value(p_name) over(partition by p_mfgr order by p_size) from part)
;

-- 18. SubQueries Not Exists
-- distinct, corr
select * 
from src_cbo b 
where not exists 
  (select distinct a.key 
  from src_cbo a 
  where b.value = a.value and a.value > 'val_2'
  )
;

-- no agg, corr, having
select * 
from src_cbo b 
group by key, value
having not exists 
  (select a.key 
  from src_cbo a 
  where b.value = a.value  and a.key = b.key and a.value > 'val_12'
  )
;

-- 19. SubQueries Exists
-- view test
create view cv1 as 
select * 
from src_cbo b 
where exists
  (select a.key 
  from src_cbo a 
  where b.value = a.value  and a.key = b.key and a.value > 'val_9')
;

select * from cv1
;

-- sq in from
select * 
from (select * 
      from src_cbo b 
      where exists 
          (select a.key 
          from src_cbo a 
          where b.value = a.value  and a.key = b.key and a.value > 'val_9')
     ) a
;

-- sq in from, having
select *
from (select b.key, count(*) 
  from src_cbo b 
  group by b.key
  having exists 
    (select a.key 
    from src_cbo a 
    where a.key = b.key and a.value > 'val_9'
    )
) a
;

-- 20. Test get stats with empty partition list
select t1.value from t1 join t2 on t1.key = t2.key where t1.dt = '10' and t1.c_boolean = true;

-- 21. Test groupby is empty and there is no other cols in aggr
select unionsrc.key FROM (select 'tst1' as key, count(1) as value from src) unionsrc;

select unionsrc.key, unionsrc.value FROM (select 'tst1' as key, count(1) as value from src) unionsrc;

select unionsrc.key FROM (select 'max' as key, max(c_int) as value from t3 s1
	UNION  ALL
    	select 'min' as key,  min(c_int) as value from t3 s2
    UNION ALL
        select 'avg' as key,  avg(c_int) as value from t3 s3) unionsrc order by unionsrc.key;
        
select unionsrc.key, unionsrc.value FROM (select 'max' as key, max(c_int) as value from t3 s1
	UNION  ALL
    	select 'min' as key,  min(c_int) as value from t3 s2
    UNION ALL
        select 'avg' as key,  avg(c_int) as value from t3 s3) unionsrc order by unionsrc.key;

select unionsrc.key, count(1) FROM (select 'max' as key, max(c_int) as value from t3 s1
    UNION  ALL
        select 'min' as key,  min(c_int) as value from t3 s2
    UNION ALL
        select 'avg' as key,  avg(c_int) as value from t3 s3) unionsrc group by unionsrc.key order by unionsrc.key;

-- Windowing
select *, rank() over(partition by key order by value) as rr from src1;

select *, rank() over(partition by key order by value) from src1;
