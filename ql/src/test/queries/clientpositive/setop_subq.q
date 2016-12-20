set hive.mapred.mode=nonstrict;

explain select key from ((select key from src) union (select key from src))subq;

explain select key from ((select key from src) intersect (select key from src))subq;

explain select key from ((select key from src) intersect select key from src)subq;

explain select key from (select key from src intersect (select key from src))subq;

explain
select a.key, b.value from ( (select key from src)a join (select value from src)b on a.key=b.value);
