set hive.mapred.mode=nonstrict;
set hive.cbo.enable=true;
set hive.cbo.returnpath.hiveop=true;
set hive.exec.check.crossproducts=false;

set hive.stats.fetch.column.stats=true;
set hive.auto.convert.join=false;

-- SORT_QUERY_RESULTS
-- 4. Test Select + Join + TS
select cbo_t1.c_int, cbo_t2.c_int from cbo_t1 join             cbo_t2 on cbo_t1.key=cbo_t2.key;
select cbo_t1.key from cbo_t1 join cbo_t3;
select cbo_t1.key from cbo_t1 join cbo_t3 where cbo_t1.key=cbo_t3.key and cbo_t1.key >= 1;
select cbo_t1.c_int, cbo_t2.c_int from cbo_t1 left outer join  cbo_t2 on cbo_t1.key=cbo_t2.key;
select cbo_t1.c_int, cbo_t2.c_int from cbo_t1 right outer join cbo_t2 on cbo_t1.key=cbo_t2.key;
select cbo_t1.c_int, cbo_t2.c_int from cbo_t1 full outer join  cbo_t2 on cbo_t1.key=cbo_t2.key;

select b, cbo_t1.c, cbo_t2.p, q, cbo_t3.c_int from (select key as a, c_int as b, cbo_t1.c_float as c from cbo_t1) cbo_t1 join (select cbo_t2.key as p, cbo_t2.c_int as q, c_float as r from cbo_t2) cbo_t2 on cbo_t1.a=p join cbo_t3 on cbo_t1.a=key;
select key, cbo_t1.c_int, cbo_t2.p, q from cbo_t1 join (select cbo_t2.key as p, cbo_t2.c_int as q, c_float as r from cbo_t2) cbo_t2 on cbo_t1.key=p join (select key as a, c_int as b, cbo_t3.c_float as c from cbo_t3)cbo_t3 on cbo_t1.key=a;
select a, cbo_t1.b, key, cbo_t2.c_int, cbo_t3.p from (select key as a, c_int as b, cbo_t1.c_float as c from cbo_t1) cbo_t1 join cbo_t2  on cbo_t1.a=key join (select key as p, c_int as q, cbo_t3.c_float as r from cbo_t3)cbo_t3 on cbo_t1.a=cbo_t3.p;
select b, cbo_t1.c, cbo_t2.c_int, cbo_t3.c_int from (select key as a, c_int as b, cbo_t1.c_float as c from cbo_t1) cbo_t1 join cbo_t2 on cbo_t1.a=cbo_t2.key join cbo_t3 on cbo_t1.a=cbo_t3.key;
select cbo_t3.c_int, b, cbo_t2.c_int, cbo_t1.c from (select key as a, c_int as b, cbo_t1.c_float as c from cbo_t1) cbo_t1 join cbo_t2 on cbo_t1.a=cbo_t2.key join cbo_t3 on cbo_t1.a=cbo_t3.key;

select b, cbo_t1.c, cbo_t2.p, q, cbo_t3.c_int from (select key as a, c_int as b, cbo_t1.c_float as c from cbo_t1) cbo_t1 left outer join (select cbo_t2.key as p, cbo_t2.c_int as q, c_float as r from cbo_t2) cbo_t2 on cbo_t1.a=p join cbo_t3 on cbo_t1.a=key;
select key, cbo_t1.c_int, cbo_t2.p, q from cbo_t1 join (select cbo_t2.key as p, cbo_t2.c_int as q, c_float as r from cbo_t2) cbo_t2 on cbo_t1.key=p left outer join (select key as a, c_int as b, cbo_t3.c_float as c from cbo_t3)cbo_t3 on cbo_t1.key=a;

select b, cbo_t1.c, cbo_t2.p, q, cbo_t3.c_int from (select key as a, c_int as b, cbo_t1.c_float as c from cbo_t1) cbo_t1 right outer join (select cbo_t2.key as p, cbo_t2.c_int as q, c_float as r from cbo_t2) cbo_t2 on cbo_t1.a=p join cbo_t3 on cbo_t1.a=key;
select key, cbo_t1.c_int, cbo_t2.p, q from cbo_t1 join (select cbo_t2.key as p, cbo_t2.c_int as q, c_float as r from cbo_t2) cbo_t2 on cbo_t1.key=p right outer join (select key as a, c_int as b, cbo_t3.c_float as c from cbo_t3)cbo_t3 on cbo_t1.key=a;

select b, cbo_t1.c, cbo_t2.p, q, cbo_t3.c_int from (select key as a, c_int as b, cbo_t1.c_float as c from cbo_t1) cbo_t1 full outer join (select cbo_t2.key as p, cbo_t2.c_int as q, c_float as r from cbo_t2) cbo_t2 on cbo_t1.a=p join cbo_t3 on cbo_t1.a=key;
select key, cbo_t1.c_int, cbo_t2.p, q from cbo_t1 join (select cbo_t2.key as p, cbo_t2.c_int as q, c_float as r from cbo_t2) cbo_t2 on cbo_t1.key=p full outer join (select key as a, c_int as b, cbo_t3.c_float as c from cbo_t3)cbo_t3 on cbo_t1.key=a;

-- 5. Test Select + Join + FIL + TS
select cbo_t1.c_int, cbo_t2.c_int from cbo_t1 join cbo_t2 on cbo_t1.key=cbo_t2.key where (cbo_t1.c_int + cbo_t2.c_int == 2) and (cbo_t1.c_int > 0 or cbo_t2.c_float >= 0);
select cbo_t1.c_int, cbo_t2.c_int from cbo_t1 left outer join  cbo_t2 on cbo_t1.key=cbo_t2.key where (cbo_t1.c_int + cbo_t2.c_int == 2) and (cbo_t1.c_int > 0 or cbo_t2.c_float >= 0);
select cbo_t1.c_int, cbo_t2.c_int from cbo_t1 right outer join cbo_t2 on cbo_t1.key=cbo_t2.key where (cbo_t1.c_int + cbo_t2.c_int == 2) and (cbo_t1.c_int > 0 or cbo_t2.c_float >= 0);
select cbo_t1.c_int, cbo_t2.c_int from cbo_t1 full outer join  cbo_t2 on cbo_t1.key=cbo_t2.key where (cbo_t1.c_int + cbo_t2.c_int == 2) and (cbo_t1.c_int > 0 or cbo_t2.c_float >= 0);

select b, cbo_t1.c, cbo_t2.p, q, cbo_t3.c_int from (select key as a, c_int as b, cbo_t1.c_float as c from cbo_t1  where (cbo_t1.c_int + 1 == 2) and (cbo_t1.c_int > 0 or cbo_t1.c_float >= 0)) cbo_t1 join (select cbo_t2.key as p, cbo_t2.c_int as q, c_float as r from cbo_t2  where (cbo_t2.c_int + 1 == 2) and (cbo_t2.c_int > 0 or cbo_t2.c_float >= 0)) cbo_t2 on cbo_t1.a=p join cbo_t3 on cbo_t1.a=key where (b + cbo_t2.q == 2) and (b > 0 or cbo_t2.q >= 0);

select q, b, cbo_t2.p, cbo_t1.c, cbo_t3.c_int from (select key as a, c_int as b, cbo_t1.c_float as c from cbo_t1  where (cbo_t1.c_int + 1 == 2) and (cbo_t1.c_int > 0 or cbo_t1.c_float >= 0)) cbo_t1 left outer join (select cbo_t2.key as p, cbo_t2.c_int as q, c_float as r from cbo_t2  where (cbo_t2.c_int + 1 == 2) and (cbo_t2.c_int > 0 or cbo_t2.c_float >= 0)) cbo_t2 on cbo_t1.a=p join cbo_t3 on cbo_t1.a=key where (b + cbo_t2.q == 2) and (b > 0 or c_int >= 0);

select q, b, cbo_t2.p, cbo_t1.c, cbo_t3.c_int from (select key as a, c_int as b, cbo_t1.c_float as c from cbo_t1  where (cbo_t1.c_int + 1 == 2) and (cbo_t1.c_int > 0 or cbo_t1.c_float >= 0)) cbo_t1 right outer join (select cbo_t2.key as p, cbo_t2.c_int as q, c_float as r from cbo_t2  where (cbo_t2.c_int + 1 == 2) and (cbo_t2.c_int > 0 or cbo_t2.c_float >= 0)) cbo_t2 on cbo_t1.a=p join cbo_t3 on cbo_t1.a=key where (b + cbo_t2.q == 2) and (b > 0 or c_int >= 0);

select q, b, cbo_t2.p, cbo_t1.c, cbo_t3.c_int from (select key as a, c_int as b, cbo_t1.c_float as c from cbo_t1  where (cbo_t1.c_int + 1 == 2) and (cbo_t1.c_int > 0 or cbo_t1.c_float >= 0)) cbo_t1 full outer join (select cbo_t2.key as p, cbo_t2.c_int as q, c_float as r from cbo_t2  where (cbo_t2.c_int + 1 == 2) and (cbo_t2.c_int > 0 or cbo_t2.c_float >= 0)) cbo_t2 on cbo_t1.a=p join cbo_t3 on cbo_t1.a=key where (b + cbo_t2.q == 2) and (b > 0 or c_int >= 0);

select * from (select q, b, cbo_t2.p, cbo_t1.c, cbo_t3.c_int from (select key as a, c_int as b, cbo_t1.c_float as c from cbo_t1  where (cbo_t1.c_int + 1 == 2) and (cbo_t1.c_int > 0 or cbo_t1.c_float >= 0)) cbo_t1 full outer join (select cbo_t2.key as p, cbo_t2.c_int as q, c_float as r from cbo_t2  where (cbo_t2.c_int + 1 == 2) and (cbo_t2.c_int > 0 or cbo_t2.c_float >= 0)) cbo_t2 on cbo_t1.a=p join cbo_t3 on cbo_t1.a=key where (b + cbo_t2.q == 2) and (b > 0 or c_int >= 0)) R where  (q + 1 = 2) and (R.b > 0 or c_int >= 0);

select * from (select q, b, cbo_t2.p, cbo_t1.c, cbo_t3.c_int from (select key as a, c_int as b, cbo_t1.c_float as c from cbo_t1  where (cbo_t1.c_int + 1 == 2) and (cbo_t1.c_int > 0 or cbo_t1.c_float >= 0)) cbo_t1 left outer join (select cbo_t2.key as p, cbo_t2.c_int as q, c_float as r from cbo_t2  where (cbo_t2.c_int + 1 == 2) and (cbo_t2.c_int > 0 or cbo_t2.c_float >= 0)) cbo_t2 on cbo_t1.a=p left outer join cbo_t3 on cbo_t1.a=key where (b + cbo_t2.q == 2) and (b > 0 or c_int >= 0)) R where  (q + 1 = 2) and (R.b > 0 or c_int >= 0);

select * from (select q, b, cbo_t2.p, cbo_t1.c, cbo_t3.c_int from (select key as a, c_int as b, cbo_t1.c_float as c from cbo_t1  where (cbo_t1.c_int + 1 == 2) and (cbo_t1.c_int > 0 or cbo_t1.c_float >= 0)) cbo_t1 left outer join (select cbo_t2.key as p, cbo_t2.c_int as q, c_float as r from cbo_t2  where (cbo_t2.c_int + 1 == 2) and (cbo_t2.c_int > 0 or cbo_t2.c_float >= 0)) cbo_t2 on cbo_t1.a=p right outer join cbo_t3 on cbo_t1.a=key where (b + cbo_t2.q == 2) and (b > 0 or c_int >= 0)) R where  (q + 1 = 2) and (R.b > 0 or c_int >= 0);

select * from (select q, b, cbo_t2.p, cbo_t1.c, cbo_t3.c_int from (select key as a, c_int as b, cbo_t1.c_float as c from cbo_t1  where (cbo_t1.c_int + 1 == 2) and (cbo_t1.c_int > 0 or cbo_t1.c_float >= 0)) cbo_t1 left outer join (select cbo_t2.key as p, cbo_t2.c_int as q, c_float as r from cbo_t2  where (cbo_t2.c_int + 1 == 2) and (cbo_t2.c_int > 0 or cbo_t2.c_float >= 0)) cbo_t2 on cbo_t1.a=p full outer join cbo_t3 on cbo_t1.a=key where (b + cbo_t2.q == 2) and (b > 0 or c_int >= 0)) R where  (q + 1 = 2) and (R.b > 0 or c_int >= 0);

select * from (select q, b, cbo_t2.p, cbo_t1.c, cbo_t3.c_int from (select key as a, c_int as b, cbo_t1.c_float as c from cbo_t1  where (cbo_t1.c_int + 1 == 2) and (cbo_t1.c_int > 0 or cbo_t1.c_float >= 0)) cbo_t1 right outer join (select cbo_t2.key as p, cbo_t2.c_int as q, c_float as r from cbo_t2  where (cbo_t2.c_int + 1 == 2) and (cbo_t2.c_int > 0 or cbo_t2.c_float >= 0)) cbo_t2 on cbo_t1.a=p right outer join cbo_t3 on cbo_t1.a=key where (b + cbo_t2.q == 2) and (b > 0 or c_int >= 0)) R where  (q + 1 = 2) and (R.b > 0 or c_int >= 0);

select * from (select q, b, cbo_t2.p, cbo_t1.c, cbo_t3.c_int from (select key as a, c_int as b, cbo_t1.c_float as c from cbo_t1  where (cbo_t1.c_int + 1 == 2) and (cbo_t1.c_int > 0 or cbo_t1.c_float >= 0)) cbo_t1 right outer join (select cbo_t2.key as p, cbo_t2.c_int as q, c_float as r from cbo_t2  where (cbo_t2.c_int + 1 == 2) and (cbo_t2.c_int > 0 or cbo_t2.c_float >= 0)) cbo_t2 on cbo_t1.a=p left outer join cbo_t3 on cbo_t1.a=key where (b + cbo_t2.q == 2) and (b > 0 or c_int >= 0)) R where  (q + 1 = 2) and (R.b > 0 or c_int >= 0);

select * from (select q, b, cbo_t2.p, cbo_t1.c, cbo_t3.c_int from (select key as a, c_int as b, cbo_t1.c_float as c from cbo_t1  where (cbo_t1.c_int + 1 == 2) and (cbo_t1.c_int > 0 or cbo_t1.c_float >= 0)) cbo_t1 right outer join (select cbo_t2.key as p, cbo_t2.c_int as q, c_float as r from cbo_t2  where (cbo_t2.c_int + 1 == 2) and (cbo_t2.c_int > 0 or cbo_t2.c_float >= 0)) cbo_t2 on cbo_t1.a=p full outer join cbo_t3 on cbo_t1.a=key where (b + cbo_t2.q == 2) and (b > 0 or c_int >= 0)) R where  (q + 1 = 2) and (R.b > 0 or c_int >= 0);

select * from (select q, b, cbo_t2.p, cbo_t1.c, cbo_t3.c_int from (select key as a, c_int as b, cbo_t1.c_float as c from cbo_t1  where (cbo_t1.c_int + 1 == 2) and (cbo_t1.c_int > 0 or cbo_t1.c_float >= 0)) cbo_t1 full outer join (select cbo_t2.key as p, cbo_t2.c_int as q, c_float as r from cbo_t2  where (cbo_t2.c_int + 1 == 2) and (cbo_t2.c_int > 0 or cbo_t2.c_float >= 0)) cbo_t2 on cbo_t1.a=p full outer join cbo_t3 on cbo_t1.a=key where (b + cbo_t2.q == 2) and (b > 0 or c_int >= 0)) R where  (q + 1 = 2) and (R.b > 0 or c_int >= 0);

select * from (select q, b, cbo_t2.p, cbo_t1.c, cbo_t3.c_int from (select key as a, c_int as b, cbo_t1.c_float as c from cbo_t1  where (cbo_t1.c_int + 1 == 2) and (cbo_t1.c_int > 0 or cbo_t1.c_float >= 0)) cbo_t1 full outer join (select cbo_t2.key as p, cbo_t2.c_int as q, c_float as r from cbo_t2  where (cbo_t2.c_int + 1 == 2) and (cbo_t2.c_int > 0 or cbo_t2.c_float >= 0)) cbo_t2 on cbo_t1.a=p left outer join cbo_t3 on cbo_t1.a=key where (b + cbo_t2.q == 2) and (b > 0 or c_int >= 0)) R where  (q + 1 = 2) and (R.b > 0 or c_int >= 0);

select * from (select q, b, cbo_t2.p, cbo_t1.c, cbo_t3.c_int from (select key as a, c_int as b, cbo_t1.c_float as c from cbo_t1  where (cbo_t1.c_int + 1 == 2) and (cbo_t1.c_int > 0 or cbo_t1.c_float >= 0)) cbo_t1 full outer join (select cbo_t2.key as p, cbo_t2.c_int as q, c_float as r from cbo_t2  where (cbo_t2.c_int + 1 == 2) and (cbo_t2.c_int > 0 or cbo_t2.c_float >= 0)) cbo_t2 on cbo_t1.a=p right outer join cbo_t3 on cbo_t1.a=key where (b + cbo_t2.q == 2) and (b > 0 or c_int >= 0)) R where  (q + 1 = 2) and (R.b > 0 or c_int >= 0);

