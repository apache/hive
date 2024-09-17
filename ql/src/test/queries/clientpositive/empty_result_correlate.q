create table t1 (id int, val varchar(10));
create table t2 (id int, val varchar(10));

EXPLAIN CBO SELECT id FROM t1 WHERE NULL IN (SELECT NULL FROM t2 where t1.id = t2.id);
