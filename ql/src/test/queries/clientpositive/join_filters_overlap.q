--! qt:dataset:src
set hive.mapred.mode=nonstrict;
-- SORT_QUERY_RESULTS
-- HIVE-3411 Filter predicates on outer join overlapped on single alias is not handled properly

create table a_n4 as SELECT 100 as key, a_n4.value as value FROM src LATERAL VIEW explode(array(40, 50, 60)) a_n4 as value limit 3;

-- overlap on a_n4
explain extended select * from a_n4 left outer join a_n4 b on (a_n4.key=b.key AND a_n4.value=50 AND b.value=50) left outer join a_n4 c on (a_n4.key=c.key AND a_n4.value=60 AND c.value=60);
select * from a_n4 left outer join a_n4 b on (a_n4.key=b.key AND a_n4.value=50 AND b.value=50) left outer join a_n4 c on (a_n4.key=c.key AND a_n4.value=60 AND c.value=60);
select /*+ MAPJOIN(b,c)*/ * from a_n4 left outer join a_n4 b on (a_n4.key=b.key AND a_n4.value=50 AND b.value=50) left outer join a_n4 c on (a_n4.key=c.key AND a_n4.value=60 AND c.value=60);

-- overlap on b
explain extended select * from a_n4 right outer join a_n4 b on (a_n4.key=b.key AND a_n4.value=50 AND b.value=50) left outer join a_n4 c on (b.key=c.key AND b.value=60 AND c.value=60);
select * from a_n4 right outer join a_n4 b on (a_n4.key=b.key AND a_n4.value=50 AND b.value=50) left outer join a_n4 c on (b.key=c.key AND b.value=60 AND c.value=60);
select /*+ MAPJOIN(a_n4,c)*/ * from a_n4 right outer join a_n4 b on (a_n4.key=b.key AND a_n4.value=50 AND b.value=50) left outer join a_n4 c on (b.key=c.key AND b.value=60 AND c.value=60);

-- overlap on b with two filters for each
explain extended select * from a_n4 right outer join a_n4 b on (a_n4.key=b.key AND a_n4.value=50 AND b.value=50 AND b.value>10) left outer join a_n4 c on (b.key=c.key AND b.value=60 AND b.value>20 AND c.value=60);
select * from a_n4 right outer join a_n4 b on (a_n4.key=b.key AND a_n4.value=50 AND b.value=50 AND b.value>10) left outer join a_n4 c on (b.key=c.key AND b.value=60 AND b.value>20 AND c.value=60);
select /*+ MAPJOIN(a_n4,c)*/ * from a_n4 right outer join a_n4 b on (a_n4.key=b.key AND a_n4.value=50 AND b.value=50 AND b.value>10) left outer join a_n4 c on (b.key=c.key AND b.value=60 AND b.value>20 AND c.value=60);

-- overlap on a_n4, b
explain extended select * from a_n4 full outer join a_n4 b on (a_n4.key=b.key AND a_n4.value=50 AND b.value=50) left outer join a_n4 c on (b.key=c.key AND b.value=60 AND c.value=60) left outer join a_n4 d on (a_n4.key=d.key AND a_n4.value=40 AND d.value=40);
select * from a_n4 full outer join a_n4 b on (a_n4.key=b.key AND a_n4.value=50 AND b.value=50) left outer join a_n4 c on (b.key=c.key AND b.value=60 AND c.value=60) left outer join a_n4 d on (a_n4.key=d.key AND a_n4.value=40 AND d.value=40);

-- triple overlap on a_n4
explain extended select * from a_n4 left outer join a_n4 b on (a_n4.key=b.key AND a_n4.value=50 AND b.value=50) left outer join a_n4 c on (a_n4.key=c.key AND a_n4.value=60 AND c.value=60) left outer join a_n4 d on (a_n4.key=d.key AND a_n4.value=40 AND d.value=40);
select * from a_n4 left outer join a_n4 b on (a_n4.key=b.key AND a_n4.value=50 AND b.value=50) left outer join a_n4 c on (a_n4.key=c.key AND a_n4.value=60 AND c.value=60) left outer join a_n4 d on (a_n4.key=d.key AND a_n4.value=40 AND d.value=40);
select /*+ MAPJOIN(b,c, d)*/ * from a_n4 left outer join a_n4 b on (a_n4.key=b.key AND a_n4.value=50 AND b.value=50) left outer join a_n4 c on (a_n4.key=c.key AND a_n4.value=60 AND c.value=60) left outer join a_n4 d on (a_n4.key=d.key AND a_n4.value=40 AND d.value=40);
