--! qt:dataset:srcpart
--! qt:dataset:src_cbo
--! qt:dataset:src1
--! qt:dataset:src
--! qt:dataset:part
--! qt:dataset:lineitem
--! qt:dataset:cbo_t3
--! qt:dataset:cbo_t2
--! qt:dataset:cbo_t1
set hive.vectorized.execution.enabled=false;
set hive.strict.checks.bucketing=false;

set hive.mapred.mode=nonstrict;
set hive.explain.user=true;

explain create table src_orc_merge_test_part_n1(key int, value string) partitioned by (ds string, ts string) stored as orc;
create table src_orc_merge_test_part_n1(key int, value string) partitioned by (ds string, ts string) stored as orc;

alter table src_orc_merge_test_part_n1 add partition (ds='2012-01-03', ts='2012-01-03+14:46:31');
desc extended src_orc_merge_test_part_n1 partition (ds='2012-01-03', ts='2012-01-03+14:46:31');

explain insert overwrite table src_orc_merge_test_part_n1 partition(ds='2012-01-03', ts='2012-01-03+14:46:31') select * from src;
insert overwrite table src_orc_merge_test_part_n1 partition(ds='2012-01-03', ts='2012-01-03+14:46:31') select * from src;
explain insert into table src_orc_merge_test_part_n1 partition(ds='2012-01-03', ts='2012-01-03+14:46:31') select * from src limit 100;

explain select count(1) from src_orc_merge_test_part_n1 where ds='2012-01-03' and ts='2012-01-03+14:46:31';
explain select sum(hash(key)), sum(hash(value)) from src_orc_merge_test_part_n1 where ds='2012-01-03' and ts='2012-01-03+14:46:31';

alter table src_orc_merge_test_part_n1 partition (ds='2012-01-03', ts='2012-01-03+14:46:31') concatenate;


explain select count(1) from src_orc_merge_test_part_n1 where ds='2012-01-03' and ts='2012-01-03+14:46:31';
explain select sum(hash(key)), sum(hash(value)) from src_orc_merge_test_part_n1 where ds='2012-01-03' and ts='2012-01-03+14:46:31';

drop table src_orc_merge_test_part_n1;

set hive.auto.convert.join=true;

explain select sum(hash(a.k1,a.v1,a.k2, a.v2))
from (
select src1.key as k1, src1.value as v1, 
       src2.key as k2, src2.value as v2 FROM 
  (select * FROM src WHERE src.key < 10) src1 
    JOIN 
  (select * FROM src WHERE src.key < 10) src2
  SORT BY k1, v1, k2, v2
) a;

set hive.cbo.enable=true;
set hive.exec.check.crossproducts=false;

set hive.stats.fetch.column.stats=true;
set hive.auto.convert.join=false;

explain select key, (c_int+1)+2 as x, sum(c_int) from cbo_t1 group by c_float, cbo_t1.c_int, key;
explain select x, y, count(*) from (select key, (c_int+c_float+1+2) as x, sum(c_int) as y from cbo_t1 group by c_float, cbo_t1.c_int, key) R group by y, x;

explain select cbo_t3.c_int, c, count(*) from (select key as a, c_int+1 as b, sum(c_int) as c from cbo_t1 where (cbo_t1.c_int + 1 >= 0) and (cbo_t1.c_int > 0 or cbo_t1.c_float >= 0) group by c_float, cbo_t1.c_int, key order by a) cbo_t1 join (select key as p, c_int+1 as q, sum(c_int) as r from cbo_t2 where (cbo_t2.c_int + 1 >= 0) and (cbo_t2.c_int > 0 or cbo_t2.c_float >= 0)  group by c_float, cbo_t2.c_int, key order by q/10 desc, r asc) cbo_t2 on cbo_t1.a=p join cbo_t3 on cbo_t1.a=key where (b + cbo_t2.q >= 0) and (b > 0 or c_int >= 0) group by cbo_t3.c_int, c order by cbo_t3.c_int+c desc, c;

explain select cbo_t3.c_int, c, count(*) from (select key as a, c_int+1 as b, sum(c_int) as c from cbo_t1 where (cbo_t1.c_int + 1 >= 0) and (cbo_t1.c_int > 0 or cbo_t1.c_float >= 0)  group by c_float, cbo_t1.c_int, key having cbo_t1.c_float > 0 and (c_int >=1 or c_float >= 1) and (c_int + c_float) >= 0 order by b % c asc, b desc) cbo_t1 left outer join (select key as p, c_int+1 as q, sum(c_int) as r from cbo_t2 where (cbo_t2.c_int + 1 >= 0) and (cbo_t2.c_int > 0 or cbo_t2.c_float >= 0)  group by c_float, cbo_t2.c_int, key  having cbo_t2.c_float > 0 and (c_int >=1 or c_float >= 1) and (c_int + c_float) >= 0) cbo_t2 on cbo_t1.a=p left outer join cbo_t3 on cbo_t1.a=key where (b + cbo_t2.q >= 0) and (b > 0 or c_int >= 0) group by cbo_t3.c_int, c  having cbo_t3.c_int > 0 and (c_int >=1 or c >= 1) and (c_int + c) >= 0  order by cbo_t3.c_int % c asc, cbo_t3.c_int desc;

explain select cbo_t3.c_int, c, count(*) from (select key as a, c_int+1 as b, sum(c_int) as c from cbo_t1 where (cbo_t1.c_int + 1 >= 0) and (cbo_t1.c_int > 0 or cbo_t1.c_float >= 0)  group by c_float, cbo_t1.c_int, key having cbo_t1.c_float > 0 and (c_int >=1 or c_float >= 1) and (c_int + c_float) >= 0 order by b+c, a desc) cbo_t1 right outer join (select key as p, c_int+1 as q, sum(c_int) as r from cbo_t2 where (cbo_t2.c_int + 1 >= 0) and (cbo_t2.c_int > 0 or cbo_t2.c_float >= 0)  group by c_float, cbo_t2.c_int, key having cbo_t2.c_float > 0 and (c_int >=1 or c_float >= 1) and (c_int + c_float) >= 0) cbo_t2 on cbo_t1.a=p right outer join cbo_t3 on cbo_t1.a=key where (b + cbo_t2.q >= 2) and (b > 0 or c_int >= 0) group by cbo_t3.c_int, c;

explain select cbo_t3.c_int, c, count(*) from (select key as a, c_int+1 as b, sum(c_int) as c from cbo_t1 where (cbo_t1.c_int + 1 >= 0) and (cbo_t1.c_int > 0 or cbo_t1.c_float >= 0)  group by c_float, cbo_t1.c_int, key having cbo_t1.c_float > 0 and (c_int >=1 or c_float >= 1) and (c_int + c_float) >= 0 order by c+a desc) cbo_t1 full outer join (select key as p, c_int+1 as q, sum(c_int) as r from cbo_t2 where (cbo_t2.c_int + 1 >= 0) and (cbo_t2.c_int > 0 or cbo_t2.c_float >= 0)  group by c_float, cbo_t2.c_int, key having cbo_t2.c_float > 0 and (c_int >=1 or c_float >= 1) and (c_int + c_float) >= 0 order by p+q desc, r asc) cbo_t2 on cbo_t1.a=p full outer join cbo_t3 on cbo_t1.a=key where (b + cbo_t2.q >= 0) and (b > 0 or c_int >= 0) group by cbo_t3.c_int, c having cbo_t3.c_int > 0 and (c_int >=1 or c >= 1) and (c_int + c) >= 0 order by cbo_t3.c_int;

explain select cbo_t3.c_int, c, count(*) from (select key as a, c_int+1 as b, sum(c_int) as c from cbo_t1 where (cbo_t1.c_int + 1 >= 0) and (cbo_t1.c_int > 0 or cbo_t1.c_float >= 0)  group by c_float, cbo_t1.c_int, key having cbo_t1.c_float > 0 and (c_int >=1 or c_float >= 1) and (c_int + c_float) >= 0) cbo_t1 join (select key as p, c_int+1 as q, sum(c_int) as r from cbo_t2 where (cbo_t2.c_int + 1 >= 0) and (cbo_t2.c_int > 0 or cbo_t2.c_float >= 0)  group by c_float, cbo_t2.c_int, key having cbo_t2.c_float > 0 and (c_int >=1 or c_float >= 1) and (c_int + c_float) >= 0) cbo_t2 on cbo_t1.a=p join cbo_t3 on cbo_t1.a=key where (b + cbo_t2.q >= 0) and (b > 0 or c_int >= 0) group by cbo_t3.c_int, c;

explain select unionsrc.key FROM (select 'tst1' as key, count(1) as value from src) unionsrc;

explain select unionsrc.key FROM (select 'max' as key, max(c_int) as value from cbo_t3 s1
	UNION  ALL
    	select 'min' as key,  min(c_int) as value from cbo_t3 s2
    UNION ALL
        select 'avg' as key,  avg(c_int) as value from cbo_t3 s3) unionsrc order by unionsrc.key;
        
explain select unionsrc.key, count(1) FROM (select 'max' as key, max(c_int) as value from cbo_t3 s1
    UNION  ALL
        select 'min' as key,  min(c_int) as value from cbo_t3 s2
    UNION ALL
        select 'avg' as key,  avg(c_int) as value from cbo_t3 s3) unionsrc group by unionsrc.key order by unionsrc.key;

explain select cbo_t1.key from cbo_t1 join cbo_t3 where cbo_t1.key=cbo_t3.key and cbo_t1.key >= 1;
explain select cbo_t1.c_int, cbo_t2.c_int from cbo_t1 left outer join  cbo_t2 on cbo_t1.key=cbo_t2.key;
explain select cbo_t1.c_int, cbo_t2.c_int from cbo_t1 full outer join  cbo_t2 on cbo_t1.key=cbo_t2.key;

explain select b, cbo_t1.c, cbo_t2.p, q, cbo_t3.c_int from (select key as a, c_int as b, cbo_t1.c_float as c from cbo_t1) cbo_t1 join (select cbo_t2.key as p, cbo_t2.c_int as q, c_float as r from cbo_t2) cbo_t2 on cbo_t1.a=p join cbo_t3 on cbo_t1.a=key;
explain select key, cbo_t1.c_int, cbo_t2.p, q from cbo_t1 join (select cbo_t2.key as p, cbo_t2.c_int as q, c_float as r from cbo_t2) cbo_t2 on cbo_t1.key=p join (select key as a, c_int as b, cbo_t3.c_float as c from cbo_t3)cbo_t3 on cbo_t1.key=a;

explain select * from (select q, b, cbo_t2.p, cbo_t1.c, cbo_t3.c_int from (select key as a, c_int as b, cbo_t1.c_float as c from cbo_t1  where (cbo_t1.c_int + 1 == 2) and (cbo_t1.c_int > 0 or cbo_t1.c_float >= 0)) cbo_t1 full outer join (select cbo_t2.key as p, cbo_t2.c_int as q, c_float as r from cbo_t2  where (cbo_t2.c_int + 1 == 2) and (cbo_t2.c_int > 0 or cbo_t2.c_float >= 0)) cbo_t2 on cbo_t1.a=p join cbo_t3 on cbo_t1.a=key where (b + cbo_t2.q == 2) and (b > 0 or c_int >= 0)) R where  (q + 1 = 2) and (R.b > 0 or c_int >= 0);

explain select * from (select q, b, cbo_t2.p, cbo_t1.c, cbo_t3.c_int from (select key as a, c_int as b, cbo_t1.c_float as c from cbo_t1  where (cbo_t1.c_int + 1 == 2) and (cbo_t1.c_int > 0 or cbo_t1.c_float >= 0)) cbo_t1 right outer join (select cbo_t2.key as p, cbo_t2.c_int as q, c_float as r from cbo_t2  where (cbo_t2.c_int + 1 == 2) and (cbo_t2.c_int > 0 or cbo_t2.c_float >= 0)) cbo_t2 on cbo_t1.a=p right outer join cbo_t3 on cbo_t1.a=key where (b + cbo_t2.q == 2) and (b > 0 or c_int >= 0)) R where  (q + 1 = 2) and (R.b > 0 or c_int >= 0);

explain select key, (c_int+1)+2 as x, sum(c_int) from cbo_t1 group by c_float, cbo_t1.c_int, key order by x limit 1;
explain select x, y, count(*) from (select key, (c_int+c_float+1+2) as x, sum(c_int) as y from cbo_t1 group by c_float, cbo_t1.c_int, key) R group by y, x order by x,y limit 1;
explain select key from(select key from (select key from cbo_t1 limit 5)cbo_t2  limit 5)cbo_t3  limit 5;
explain select key, c_int from(select key, c_int from (select key, c_int from cbo_t1 order by c_int limit 5)cbo_t1  order by c_int limit 5)cbo_t2  order by c_int limit 5;

explain select cbo_t3.c_int, c, count(*) from (select key as a, c_int+1 as b, sum(c_int) as c from cbo_t1 where (cbo_t1.c_int + 1 >= 0) and (cbo_t1.c_int > 0 or cbo_t1.c_float >= 0) group by c_float, cbo_t1.c_int, key order by a limit 5) cbo_t1 join (select key as p, c_int+1 as q, sum(c_int) as r from cbo_t2 where (cbo_t2.c_int + 1 >= 0) and (cbo_t2.c_int > 0 or cbo_t2.c_float >= 0)  group by c_float, cbo_t2.c_int, key order by q/10 desc, r asc limit 5) cbo_t2 on cbo_t1.a=p join cbo_t3 on cbo_t1.a=key where (b + cbo_t2.q >= 0) and (b > 0 or c_int >= 0) group by cbo_t3.c_int, c order by cbo_t3.c_int+c desc, c limit 5;

explain select cbo_t1.c_int           from cbo_t1 left semi join   cbo_t2 on cbo_t1.key=cbo_t2.key where (cbo_t1.c_int + 1 == 2) and (cbo_t1.c_int > 0 or cbo_t1.c_float >= 0);
explain select * from (select c, b, a from (select key as a, c_int as b, cbo_t1.c_float as c from cbo_t1  where (cbo_t1.c_int + 1 == 2) and (cbo_t1.c_int > 0 or cbo_t1.c_float >= 0)) cbo_t1 left semi join (select cbo_t2.key as p, cbo_t2.c_int as q, c_float as r from cbo_t2  where (cbo_t2.c_int + 1 == 2) and (cbo_t2.c_int > 0 or cbo_t2.c_float >= 0)) cbo_t2 on cbo_t1.a=p left semi join cbo_t3 on cbo_t1.a=key where (b + 1 == 2) and (b > 0 or c >= 0)) R where  (b + 1 = 2) and (R.b > 0 or c >= 0);
explain select a, c, count(*) from (select key as a, c_int+1 as b, sum(c_int) as c from cbo_t1 where (cbo_t1.c_int + 1 >= 0) and (cbo_t1.c_int > 0 or cbo_t1.c_float >= 0)  group by c_float, cbo_t1.c_int, key having cbo_t1.c_float > 0 and (c_int >=1 or c_float >= 1) and (c_int + c_float) >= 0 order by a+b desc, c asc) cbo_t1 left semi join (select key as p, c_int+1 as q, sum(c_int) as r from cbo_t2 where (cbo_t2.c_int + 1 >= 0) and (cbo_t2.c_int > 0 or cbo_t2.c_float >= 0)  group by c_float, cbo_t2.c_int, key having cbo_t2.c_float > 0 and (c_int >=1 or c_float >= 1) and (c_int + c_float) >= 0 order by q+r/10 desc, p) cbo_t2 on cbo_t1.a=p left semi join cbo_t3 on cbo_t1.a=key where (b + 1  >= 0) and (b > 0 or a >= 0) group by a, c  having a > 0 and (a >=1 or c >= 1) and (a + c) >= 0 order by c, a;

explain select cbo_t1.key as x, c_int as c_int, (((c_int+c_float)*10)+5) as y from cbo_t1; 

explain select null from cbo_t1;

explain select key from cbo_t1 where c_int = -6  or c_int = +6;

explain select count(cbo_t1.dt) from cbo_t1 join cbo_t2 on cbo_t1.dt  = cbo_t2.dt  where cbo_t1.dt = '2014' ;

explain select * 
from src_cbo b 
where not exists 
  (select distinct a.key 
  from src_cbo a 
  where b.value = a.value and a.value > 'val_2'
  )
;

explain select * 
from src_cbo b 
group by key, value
having not exists 
  (select a.key 
  from src_cbo a 
  where b.value = a.value  and a.key = b.key and a.value > 'val_12'
  )
;

create view cv1_n5 as 
select * 
from src_cbo b 
where exists
  (select a.key 
  from src_cbo a 
  where b.value = a.value  and a.key = b.key and a.value > 'val_9')
;

explain select * from cv1_n5;

explain select * 
from (select * 
      from src_cbo b 
      where exists 
          (select a.key 
          from src_cbo a 
          where b.value = a.value  and a.key = b.key and a.value > 'val_9')
     ) a
;


explain select * 
from src_cbo 
where src_cbo.key in (select key from src_cbo s1 where s1.key > '9')
;


explain select p.p_partkey, li.l_suppkey 
from (select distinct l_partkey as p_partkey from lineitem) p join lineitem li on p.p_partkey = li.l_partkey 
where li.l_linenumber = 1 and
 li.l_orderkey in (select l_orderkey from lineitem where l_shipmode = 'AIR' and l_linenumber = li.l_linenumber)
;

explain select key, value, count(*) 
from src_cbo b
where b.key in (select key from src_cbo where src_cbo.key > '8')
group by key, value
having count(*) in (select count(*) from src_cbo s1 where s1.key > '9' group by s1.key )
;

explain select p_mfgr, p_name, avg(p_size) 
from part 
group by p_mfgr, p_name
having p_name in 
  (select first_value(p_name) over(partition by p_mfgr order by p_size) from part)
;

explain select * 
from src_cbo 
where src_cbo.key not in  
  ( select key  from src_cbo s1 
    where s1.key > '2'
  ) order by key
;

explain select p_mfgr, b.p_name, p_size 
from part b 
where b.p_name not in 
  (select p_name 
  from (select p_mfgr, p_name, p_size as r from part) a 
  where r < 10 and b.p_mfgr = a.p_mfgr 
  )
;

explain select p_name, p_size 
from 
part where part.p_size not in 
  (select avg(p_size) 
  from (select p_size from part) a 
  where p_size < 10
  ) order by p_name
;

explain select b.p_mfgr, min(p_retailprice) 
from part b 
group by b.p_mfgr
having b.p_mfgr not in 
  (select p_mfgr 
  from (select p_mfgr, min(p_retailprice) l, max(p_retailprice) r, avg(p_retailprice) a from part group by p_mfgr) a 
  where min(p_retailprice) = l and r - l > 600
  )
  order by b.p_mfgr
;

explain select count(c_int) over(), sum(c_float) over(), max(c_int) over(), min(c_int) over(), row_number() over(), rank() over(), dense_rank() over(), percent_rank() over(), lead(c_int, 2, c_int) over(), lag(c_float, 2, c_float) over() from cbo_t1;
explain select * from (select count(c_int) over(), sum(c_float) over(), max(c_int) over(), min(c_int) over(), row_number() over(), rank() over(), dense_rank() over(), percent_rank() over(), lead(c_int, 2, c_int) over(), lag(c_float, 2, c_float) over() from cbo_t1) cbo_t1;
explain select i, a, h, b, c, d, e, f, g, a as x, a +1 as y from (select max(c_int) over (partition by key order by value range UNBOUNDED PRECEDING) a, min(c_int) over (partition by key order by value range current row) b, count(c_int) over(partition by key order by value range 1 PRECEDING) c, avg(value) over (partition by key order by value range between unbounded preceding and unbounded following) d, sum(value) over (partition by key order by value range between unbounded preceding and current row) e, avg(c_float) over (partition by key order by value range between 1 preceding and unbounded following) f, sum(c_float) over (partition by key order by value range between 1 preceding and current row) g, max(c_float) over (partition by key order by value range between 1 preceding and unbounded following) h, min(c_float) over (partition by key order by value range between 1 preceding and 1 following) i from cbo_t1) cbo_t1;
explain select *, rank() over(partition by key order by value) as rr from src1;


set hive.auto.convert.join=false;
set hive.optimize.correlation=false;
explain
select SUM(HASH(tmp.key)), SUM(HASH(tmp.cnt))
FROM (select x.key AS key, count(1) AS cnt
      FROM src1 x JOIN src y ON (x.key = y.key)
      GROUP BY x.key) tmp;

set hive.optimize.correlation=true;
explain
select SUM(HASH(tmp.key)), SUM(HASH(tmp.cnt))
FROM (select x.key AS key, count(1) AS cnt
      FROM src1 x JOIN src y ON (x.key = y.key)
      GROUP BY x.key) tmp;

set hive.auto.convert.join=true;
set hive.optimize.correlation=true;
explain
select SUM(HASH(tmp.key)), SUM(HASH(tmp.cnt))
FROM (select x.key AS key, count(1) AS cnt
      FROM src1 x JOIN src y ON (x.key = y.key)
      GROUP BY x.key) tmp;

set hive.auto.convert.join=false;
set hive.optimize.correlation=false;
explain
select SUM(HASH(tmp.key)), SUM(HASH(tmp.cnt))
FROM (select x.key AS key, count(1) AS cnt
      FROM src1 x LEFT SEMI JOIN src y ON (x.key = y.key)
      GROUP BY x.key) tmp;

explain create table abcd_n1 (a int, b int, c int, d int);
create table abcd_n1 (a int, b int, c int, d int);
LOAD DATA LOCAL INPATH '../../data/files/in4.txt' INTO TABLE abcd_n1;

set hive.map.aggr=true;
explain select a, count(distinct b), count(distinct c), sum(d) from abcd_n1 group by a;

set hive.map.aggr=false;
explain select a, count(distinct b), count(distinct c), sum(d) from abcd_n1 group by a;

explain create table src_rc_merge_test_n0(key int, value string) stored as rcfile;
create table src_rc_merge_test_n0(key int, value string) stored as rcfile;

load data local inpath '../../data/files/smbbucket_1.rc' into table src_rc_merge_test_n0;

set hive.exec.compress.output = true;

explain create table tgt_rc_merge_test_n0(key int, value string) stored as rcfile;
create table tgt_rc_merge_test_n0(key int, value string) stored as rcfile;
insert into table tgt_rc_merge_test_n0 select * from src_rc_merge_test_n0;

show table extended like `tgt_rc_merge_test_n0`;

explain select count(1) from tgt_rc_merge_test_n0;
explain select sum(hash(key)), sum(hash(value)) from tgt_rc_merge_test_n0;

alter table tgt_rc_merge_test_n0 concatenate;

show table extended like `tgt_rc_merge_test_n0`;

explain select count(1) from tgt_rc_merge_test_n0;
explain select sum(hash(key)), sum(hash(value)) from tgt_rc_merge_test_n0;

drop table src_rc_merge_test_n0;
drop table tgt_rc_merge_test_n0;

explain select src.key from src cross join src src2;


explain create table nzhang_Tmp_n1(a int, b string);
create table nzhang_Tmp_n1(a int, b string);

explain create table nzhang_CTAS1_n1 as select key k, value from src sort by k, value limit 10;
create table nzhang_CTAS1_n1 as select key k, value from src sort by k, value limit 10;


explain create table nzhang_ctas3_n1 row format serde "org.apache.hadoop.hive.serde2.columnar.ColumnarSerDe" stored as RCFile as select key/2 half_key, concat(value, "_con") conb  from src sort by half_key, conb limit 10;

create table nzhang_ctas3_n1 row format serde "org.apache.hadoop.hive.serde2.columnar.ColumnarSerDe" stored as RCFile as select key/2 half_key, concat(value, "_con") conb  from src sort by half_key, conb limit 10;

explain create table if not exists nzhang_ctas3_n1 as select key, value from src sort by key, value limit 2;

create table if not exists nzhang_ctas3_n1 as select key, value from src sort by key, value limit 2;

set hive.support.concurrency=true;
set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;


explain create temporary table acid_dtt(a int, b varchar(128)) clustered by (a) into 2 buckets stored as orc TBLPROPERTIES ('transactional'='true');
create temporary table acid_dtt(a int, b varchar(128)) clustered by (a) into 2 buckets stored as orc TBLPROPERTIES ('transactional'='true');

set hive.map.aggr=false;
set hive.groupby.skewindata=true;


explain
select src1.key as k1, src1.value as v1, 
       src2.key as k2, src2.value as v2 FROM 
  (select * FROM src WHERE src.key < 10) src1 
    JOIN 
  (select * FROM src WHERE src.key < 10) src2
  SORT BY k1, v1, k2, v2;


CREATE TABLE myinput1_n7(key int, value int);
LOAD DATA LOCAL INPATH '../../data/files/in8.txt' INTO TABLE myinput1_n7;

explain select * from myinput1_n7 a join myinput1_n7 b on a.key<=>b.value;

explain select * from myinput1_n7 a join myinput1_n7 b on a.key<=>b.value join myinput1_n7 c on a.key=c.key;

explain select * from myinput1_n7 a join myinput1_n7 b on a.key<=>b.value join myinput1_n7 c on a.key<=>c.key;

explain select * from myinput1_n7 a join myinput1_n7 b on a.key<=>b.value AND a.value=b.key join myinput1_n7 c on a.key<=>c.key AND a.value=c.value;

explain select * from myinput1_n7 a join myinput1_n7 b on a.key<=>b.value AND a.value<=>b.key join myinput1_n7 c on a.key<=>c.key AND a.value<=>c.value;

explain select * FROM myinput1_n7 a LEFT OUTER JOIN myinput1_n7 b ON a.key<=>b.value;
explain select * FROM myinput1_n7 a RIGHT OUTER JOIN myinput1_n7 b ON a.key<=>b.value;
explain select * FROM myinput1_n7 a FULL OUTER JOIN myinput1_n7 b ON a.key<=>b.value;

explain select /*+ MAPJOIN(b) */ * FROM myinput1_n7 a JOIN myinput1_n7 b ON a.key<=>b.value;

CREATE TABLE smb_input_n0(key int, value int);
LOAD DATA LOCAL INPATH '../../data/files/in4.txt' into table smb_input_n0;
LOAD DATA LOCAL INPATH '../../data/files/in5.txt' into table smb_input_n0;


;

CREATE TABLE smb_input1_n2(key int, value int) CLUSTERED BY (key) SORTED BY (key) INTO 2 BUCKETS;
CREATE TABLE smb_input2_n2(key int, value int) CLUSTERED BY (value) SORTED BY (value) INTO 2 BUCKETS;

from smb_input_n0
insert overwrite table smb_input1_n2 select *
insert overwrite table smb_input2_n2 select *;

SET hive.optimize.bucketmapjoin = true;
SET hive.optimize.bucketmapjoin.sortedmerge = true;
SET hive.input.format = org.apache.hadoop.hive.ql.io.BucketizedHiveInputFormat;

analyze table smb_input1_n2 compute statistics;

explain select /*+ MAPJOIN(a) */ * FROM smb_input1_n2 a JOIN smb_input1_n2 b ON a.key <=> b.key;
explain select /*+ MAPJOIN(a) */ * FROM smb_input1_n2 a JOIN smb_input1_n2 b ON a.key <=> b.key AND a.value <=> b.value;
explain select /*+ MAPJOIN(a) */ * FROM smb_input1_n2 a RIGHT OUTER JOIN smb_input1_n2 b ON a.key <=> b.key;
explain select /*+ MAPJOIN(b) */ * FROM smb_input1_n2 a JOIN smb_input1_n2 b ON a.key <=> b.key;
explain select /*+ MAPJOIN(b) */ * FROM smb_input1_n2 a LEFT OUTER JOIN smb_input1_n2 b ON a.key <=> b.key;

drop table sales_n0;
drop table things_n0;

set hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;

CREATE TABLE sales_n0 (name STRING, id INT)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t';

CREATE TABLE things_n0 (id INT, name STRING) partitioned by (ds string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t';

load data local inpath '../../data/files/sales.txt' INTO TABLE sales_n0;
load data local inpath '../../data/files/things.txt' INTO TABLE things_n0 partition(ds='2011-10-23');
load data local inpath '../../data/files/things2.txt' INTO TABLE things_n0 partition(ds='2011-10-24');

explain select name,id FROM sales_n0 LEFT SEMI JOIN things_n0 ON (sales_n0.id = things_n0.id);

drop table sales_n0;
drop table things_n0;

set hive.auto.convert.join=true;
set hive.auto.convert.join.noconditionaltask=true;
set hive.auto.convert.join.noconditionaltask.size=10000;
set hive.stats.fetch.column.stats=false;
 
set hive.mapjoin.optimized.hashtable=false;

explain select srcpart.key from srcpart join src on (srcpart.value=src.value) join src1 on (srcpart.key=src1.key) where srcpart.value > 'val_450';

set hive.mapjoin.optimized.hashtable=true;

explain select srcpart.key from srcpart join src on (srcpart.value=src.value) join src1 on (srcpart.key=src1.key) where srcpart.value > 'val_450';
set hive.stats.fetch.column.stats=true;
explain
select p_mfgr, p_name, p_size,
rank() over (partition by p_mfgr order by p_name) as r,
dense_rank() over (partition by p_mfgr order by p_name) as dr,
sum(p_retailprice) over (partition by p_mfgr order by p_name rows between unbounded preceding and current row) as s1
from noop(on part 
  partition by p_mfgr
  order by p_name
  );

explain
select p_mfgr, p_name,
p_size, p_size - lag(p_size,1,p_size) over (partition by p_mfgr order by p_name) as deltaSz
from noop (on (select p1.* from part p1 join part p2 on p1.p_partkey = p2.p_partkey) j
distribute by j.p_mfgr
sort by j.p_name)
;    

explain
select p_mfgr, p_name, p_size,
rank() over (partition by p_mfgr order by p_name) as r,
dense_rank() over (partition by p_mfgr order by p_name) as dr,
sum(p_retailprice) over (partition by p_mfgr order by p_name rows between unbounded preceding and current row) as s1
from noop(on part 
  partition by p_mfgr
  order by p_name
  ) abc;

explain
select p_mfgr, p_name, p_size, 
rank() over (partition by p_mfgr order by p_name) as r, 
dense_rank() over (partition by p_mfgr order by p_name) as dr, 
p_size, p_size - lag(p_size,1,p_size) over (partition by p_mfgr order by p_name) as deltaSz 
from noop(on part 
          partition by p_mfgr 
          order by p_name 
          ) 
;

explain
select p_mfgr, p_name, p_size, 
rank() over (partition by p_mfgr order by p_name) as r, 
dense_rank() over (partition by p_mfgr order by p_name) as dr, 
p_size, p_size - lag(p_size,1,p_size) over (partition by p_mfgr order by p_name) as deltaSz 
from noop(on part 
          partition by p_mfgr 
          order by p_name 
          ) 
group by p_mfgr, p_name, p_size  
;

explain
select abc.* 
from noop(on part 
partition by p_mfgr 
order by p_name 
) abc join part p1 on abc.p_partkey = p1.p_partkey;


explain
select p_mfgr, p_name, p_size, 
rank() over (partition by p_mfgr order by p_name, p_size desc) as r
from noopwithmap(on part
partition by p_mfgr
order by p_name, p_size desc);

explain 
select p_mfgr, p_name, p_size,
rank() over (partition by p_mfgr order by p_name) as r,
dense_rank() over (partition by p_mfgr order by p_name) as dr,
sum(p_retailprice) over (partition by p_mfgr order by p_name rows between unbounded preceding and current row) as s1
from noopwithmap(on part 
  partition by p_mfgr
  order by p_name);

explain
select p_mfgr, p_name, p_size,
rank() over (partition by p_mfgr order by p_name) as r,
dense_rank() over (partition by p_mfgr order by p_name) as dr,
sum(p_retailprice) over (partition by p_mfgr order by p_name rows between unbounded preceding and current row) as s1
from noop(on part
partition by p_mfgr
order by p_name)
;
  
explain
select p_mfgr, p_name, p_size, 
rank() over (partition by p_mfgr order by p_name) as r, 
dense_rank() over (partition by p_mfgr order by p_name) as dr, 
sum(p_retailprice) over (partition by p_mfgr order by p_name rows between unbounded preceding and current row)  as s1
from noop(on noopwithmap(on noop(on part 
partition by p_mfgr 
order by p_mfgr DESC, p_name
)));

explain
select p_mfgr, p_name, 
sub1.cd, sub1.s1 
from (select p_mfgr, p_name, 
count(p_size) over (partition by p_mfgr order by p_name) as cd, 
p_retailprice, 
sum(p_retailprice) over w1  as s1
from noop(on part 
partition by p_mfgr 
order by p_name) 
window w1 as (partition by p_mfgr order by p_name rows between 2 preceding and 2 following) 
) sub1 ;


explain
select abc.p_mfgr, abc.p_name, 
rank() over (distribute by abc.p_mfgr sort by abc.p_name) as r, 
dense_rank() over (distribute by abc.p_mfgr sort by abc.p_name) as dr, 
count(abc.p_name) over (distribute by abc.p_mfgr sort by abc.p_name) as cd, 
abc.p_retailprice, sum(abc.p_retailprice) over (distribute by abc.p_mfgr sort by abc.p_name rows between unbounded preceding and current row) as s1, 
abc.p_size, abc.p_size - lag(abc.p_size,1,abc.p_size) over (distribute by abc.p_mfgr sort by abc.p_name) as deltaSz 
from noop(on part 
partition by p_mfgr 
order by p_name 
) abc join part p1 on abc.p_partkey = p1.p_partkey 
;

  
explain create view IF NOT EXISTS mfgr_price_view_n3 as 
select p_mfgr, p_brand, 
sum(p_retailprice) as s 
from part 
group by p_mfgr, p_brand;

CREATE TABLE part_4_n1( 
p_mfgr STRING, 
p_name STRING, 
p_size INT, 
r INT, 
dr INT, 
s DOUBLE);

CREATE TABLE part_5_n1( 
p_mfgr STRING, 
p_name STRING, 
p_size INT, 
s2 INT, 
r INT, 
dr INT, 
cud DOUBLE, 
fv1 INT);

explain
from noop(on part 
partition by p_mfgr 
order by p_name) 
INSERT OVERWRITE TABLE part_4_n1 select p_mfgr, p_name, p_size, 
rank() over (distribute by p_mfgr sort by p_name) as r, 
dense_rank() over (distribute by p_mfgr sort by p_name) as dr, 
sum(p_retailprice) over (distribute by p_mfgr sort by p_name rows between unbounded preceding and current row)  as s  
INSERT OVERWRITE TABLE part_5_n1 select  p_mfgr,p_name, p_size,  
round(sum(p_size) over (distribute by p_mfgr sort by p_size range between 5 preceding and current row),1) as s2,
rank() over (distribute by p_mfgr sort by p_mfgr, p_name) as r, 
dense_rank() over (distribute by p_mfgr sort by p_mfgr, p_name) as dr, 
cume_dist() over (distribute by p_mfgr sort by p_mfgr, p_name) as cud, 
first_value(p_size, true) over w1  as fv1
window w1 as (distribute by p_mfgr sort by p_mfgr, p_name rows between 2 preceding and 2 following);


explain
select p_mfgr, p_name,  
rank() over (partition by p_mfgr,p_name) as r, 
dense_rank() over (partition by p_mfgr,p_name) as dr, 
p_size, sum(p_size) over (partition by p_mfgr,p_name rows between unbounded preceding and current row)  as s1
from noop(on 
        noopwithmap(on 
          noop(on 
              noop(on part 
              partition by p_mfgr 
              order by p_mfgr) 
            ) 
          partition by p_mfgr,p_name 
          order by p_mfgr,p_name) 
        partition by p_mfgr,p_name  
        order by p_mfgr,p_name) ;

explain
select p_mfgr, p_name,  
rank() over (partition by p_mfgr order by p_name) as r, 
dense_rank() over (partition by p_mfgr order by p_name) as dr, 
p_size, sum(p_size) over (partition by p_mfgr order by p_name rows between unbounded preceding and current row)  as s1
from noop(on 
        noop(on 
          noop(on 
              noop(on part 
              partition by p_mfgr 
              order by p_mfgr) 
            ) 
          partition by p_mfgr,p_name 
          order by p_mfgr,p_name) 
        partition by p_mfgr  
        order by p_mfgr ) ;

explain
select p_mfgr, p_name,  
rank() over (partition by p_mfgr order by p_name) as r, 
dense_rank() over (partition by p_mfgr order by p_name) as dr, 
p_size, sum(p_size) over (partition by p_mfgr order by p_name) as s1 
from noop(on 
        noop(on 
          noop(on 
              noop(on part 
              partition by p_mfgr,p_name 
              order by p_mfgr,p_name) 
            ) 
          partition by p_mfgr 
          order by p_mfgr)); 

explain select distinct src.* from src;

explain select explode(array('a', 'b'));

set hive.optimize.skewjoin = true;
set hive.skewjoin.key = 2;

CREATE TABLE T1_n119(key STRING, val STRING) STORED AS TEXTFILE;
CREATE TABLE T2_n70(key STRING, val STRING) STORED AS TEXTFILE;
CREATE TABLE T3_n26(key STRING, val STRING) STORED AS TEXTFILE;
CREATE TABLE T4_n15(key STRING, val STRING) STORED AS TEXTFILE;
CREATE TABLE dest_j1_n16(key INT, value STRING) STORED AS TEXTFILE;

LOAD DATA LOCAL INPATH '../../data/files/T1.txt' INTO TABLE T1_n119;
LOAD DATA LOCAL INPATH '../../data/files/T2.txt' INTO TABLE T2_n70;
LOAD DATA LOCAL INPATH '../../data/files/T3.txt' INTO TABLE T3_n26;
LOAD DATA LOCAL INPATH '../../data/files/T1.txt' INTO TABLE T4_n15;


explain
FROM src src1 JOIN src src2 ON (src1.key = src2.key)
INSERT OVERWRITE TABLE dest_j1_n16 select src1.key, src2.value;

FROM src src1 JOIN src src2 ON (src1.key = src2.key)
INSERT OVERWRITE TABLE dest_j1_n16 select src1.key, src2.value;



explain
select /*+ STREAMTABLE(a) */ *
FROM T1_n119 a JOIN T2_n70 b ON a.key = b.key
          JOIN T3_n26 c ON b.key = c.key
          JOIN T4_n15 d ON c.key = d.key;

explain
select /*+ STREAMTABLE(a,c) */ *
FROM T1_n119 a JOIN T2_n70 b ON a.key = b.key
          JOIN T3_n26 c ON b.key = c.key
          JOIN T4_n15 d ON c.key = d.key;

explain FROM T1_n119 a JOIN src c ON c.key+1=a.key select /*+ STREAMTABLE(a) */ sum(hash(a.key)), sum(hash(a.val)), sum(hash(c.key));
FROM T1_n119 a JOIN src c ON c.key+1=a.key select /*+ STREAMTABLE(a) */ sum(hash(a.key)), sum(hash(a.val)), sum(hash(c.key));

explain 
select * FROM 
(select src.* FROM src) x
JOIN 
(select src.* FROM src) Y
ON (x.key = Y.key);


explain select /*+ mapjoin(k)*/ sum(hash(k.key)), sum(hash(v.val)) from T1_n119 k join T1_n119 v on k.key=v.val;

explain select sum(hash(k.key)), sum(hash(v.val)) from T1_n119 k join T1_n119 v on k.key=v.key;

explain select count(1) from  T1_n119 a join T1_n119 b on a.key = b.key;

explain FROM T1_n119 a LEFT OUTER JOIN T2_n70 c ON c.key+1=a.key select sum(hash(a.key)), sum(hash(a.val)), sum(hash(c.key));

explain FROM T1_n119 a RIGHT OUTER JOIN T2_n70 c ON c.key+1=a.key select /*+ STREAMTABLE(a) */ sum(hash(a.key)), sum(hash(a.val)), sum(hash(c.key));

explain FROM T1_n119 a FULL OUTER JOIN T2_n70 c ON c.key+1=a.key select /*+ STREAMTABLE(a) */ sum(hash(a.key)), sum(hash(a.val)), sum(hash(c.key));

explain select /*+ mapjoin(v)*/ sum(hash(k.key)), sum(hash(v.val)) from T1_n119 k left outer join T1_n119 v on k.key+1=v.key;
