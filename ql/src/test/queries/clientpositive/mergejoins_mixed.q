set hive.mapred.mode=nonstrict;
-- HIVE-3464

create table a_n5 (key string, value string);

-- (a_n5-b-c-d)
explain
select * from a_n5 join a_n5 b on (a_n5.key=b.key) left outer join a_n5 c on (b.key=c.key) left outer join a_n5 d on (a_n5.key=d.key);

explain
select * from a_n5 join a_n5 b on (a_n5.key=b.key) left outer join a_n5 c on (b.key=c.key) right outer join a_n5 d on (a_n5.key=d.key);

explain
select * from a_n5 join a_n5 b on (a_n5.key=b.key) right outer join a_n5 c on (b.key=c.key) left outer join a_n5 d on (a_n5.key=d.key);

explain
select * from a_n5 join a_n5 b on (a_n5.key=b.key) right outer join a_n5 c on (b.key=c.key) right outer join a_n5 d on (a_n5.key=d.key);

-- ((a_n5-b-d)-c) (reordered)
explain
select * from a_n5 join a_n5 b on (a_n5.key=b.key) left outer join a_n5 c on (b.value=c.key) left outer join a_n5 d on (a_n5.key=d.key);

explain
select * from a_n5 join a_n5 b on (a_n5.key=b.key) right outer join a_n5 c on (b.value=c.key) right outer join a_n5 d on (a_n5.key=d.key);

explain
select * from a_n5 join a_n5 b on (a_n5.key=b.key) full outer join a_n5 c on (b.value=c.key) full outer join a_n5 d on (a_n5.key=d.key);

-- (((a_n5-b)-c)-d)
explain
select * from a_n5 join a_n5 b on (a_n5.key=b.key) left outer join a_n5 c on (b.value=c.key) right outer join a_n5 d on (a_n5.key=d.key);

explain
select * from a_n5 join a_n5 b on (a_n5.key=b.key) left outer join a_n5 c on (b.value=c.key) full outer join a_n5 d on (a_n5.key=d.key);

explain
select * from a_n5 join a_n5 b on (a_n5.key=b.key) right outer join a_n5 c on (b.value=c.key) left outer join a_n5 d on (a_n5.key=d.key);

explain
select * from a_n5 join a_n5 b on (a_n5.key=b.key) right outer join a_n5 c on (b.value=c.key) full outer join a_n5 d on (a_n5.key=d.key);

-- ((a_n5-b)-c-d)
explain
select * from a_n5 join a_n5 b on (a_n5.key=b.key) left outer join a_n5 c on (b.value=c.key) left outer join a_n5 d on (c.key=d.key);
