set hive.mapred.mode=nonstrict;
set hive.exec.post.hooks=org.apache.hadoop.hive.ql.hooks.LineageLogger;

drop table if exists src2;
create table src2 as select key key2, value value2 from src1;

select * from src1 where key is not null and value is not null limit 3;
select * from src1 where key > 10 and value > 'val' order by key limit 5;

drop table if exists dest1;
create table dest1 as select * from src1;
insert into table dest1 select * from src2;

select key k, dest1.value from dest1;
select key from src1 union select key2 from src2 order by key;
select key k from src1 union select key2 from src2 order by k;

select key, count(1) a from dest1 group by key;
select key k, count(*) from dest1 group by key;
select key k, count(value) from dest1 group by key;
select value, max(length(key)) from dest1 group by value;
select value, max(length(key)) from dest1 group by value order by value limit 5;

select key, length(value) from dest1;
select length(value) + 3 from dest1;
select 5 from dest1;
select 3 * 5 from dest1;

drop table if exists dest2;
create table dest2 as select * from src1 JOIN src2 ON src1.key = src2.key2;
insert overwrite table dest2 select * from src1 JOIN src2 ON src1.key = src2.key2;
insert into table dest2 select * from src1 JOIN src2 ON src1.key = src2.key2;
insert into table dest2
  select * from src1 JOIN src2 ON length(src1.value) = length(src2.value2) + 1;

select * from src1 where length(key) > 2;
select * from src1 where length(key) > 2 and value > 'a';

drop table if exists dest3;
create table dest3 as
  select * from src1 JOIN src2 ON src1.key = src2.key2 WHERE length(key) > 1;
insert overwrite table dest2
  select * from src1 JOIN src2 ON src1.key = src2.key2 WHERE length(key) > 3;

drop table if exists dest_l1;
CREATE TABLE dest_l1(key INT, value STRING) STORED AS TEXTFILE;

INSERT OVERWRITE TABLE dest_l1
SELECT j.*
FROM (SELECT t1.key, p1.value
      FROM src1 t1
      LEFT OUTER JOIN src p1
      ON (t1.key = p1.key)
      UNION ALL
      SELECT t2.key, p2.value
      FROM src1 t2
      LEFT OUTER JOIN src p2
      ON (t2.key = p2.key)) j;

drop table if exists emp;
drop table if exists dept;
drop table if exists project;
drop table if exists tgt;
create table emp(emp_id int, name string, mgr_id int, dept_id int);
create table dept(dept_id int, dept_name string);
create table project(project_id int, project_name string);
create table tgt(dept_name string, name string,
  emp_id int, mgr_id int, proj_id int, proj_name string);

INSERT INTO TABLE tgt
SELECT emd.dept_name, emd.name, emd.emp_id, emd.mgr_id, p.project_id, p.project_name
FROM (
  SELECT d.dept_name, em.name, em.emp_id, em.mgr_id, em.dept_id
  FROM (
    SELECT e.name, e.dept_id, e.emp_id emp_id, m.emp_id mgr_id
    FROM emp e JOIN emp m ON e.emp_id = m.emp_id
    ) em
  JOIN dept d ON d.dept_id = em.dept_id
  ) emd JOIN project p ON emd.dept_id = p.project_id;

drop table if exists dest_l2;
create table dest_l2 (id int, c1 tinyint, c2 int, c3 bigint) stored as textfile;
insert into dest_l2 values(0, 1, 100, 10000);

select * from (
  select c1 + c2 x from dest_l2
  union all
  select sum(c3) y from (select c3 from dest_l2) v1) v2 order by x;

drop table if exists dest_l3;
create table dest_l3 (id int, c1 string, c2 string, c3 int) stored as textfile;
insert into dest_l3 values(0, "s1", "s2", 15);

select sum(a.c1) over (partition by a.c1 order by a.id)
from dest_l2 a
where a.c2 != 10
group by a.c1, a.c2, a.id
having count(a.c2) > 0;

select sum(a.c1), count(b.c1), b.c2, b.c3
from dest_l2 a join dest_l3 b on (a.id = b.id)
where a.c2 != 10 and b.c3 > 0
group by a.c1, a.c2, a.id, b.c1, b.c2, b.c3
having count(a.c2) > 0
order by b.c3 limit 5;

drop table if exists t;
create table t as
select distinct a.c2, a.c3 from dest_l2 a
inner join dest_l3 b on (a.id = b.id)
where a.id > 0 and b.c3 = 15;

SELECT substr(src1.key,1,1), count(DISTINCT substr(src1.value,5)),
concat(substr(src1.key,1,1),sum(substr(src1.value,5)))
from src1
GROUP BY substr(src1.key,1,1);

drop table if exists relations;
create table relations (identity char(32), type string,
  ep1_src_type string, ep1_type string, ep2_src_type string, ep2_type string,
  ep1_ids array<string>, ep2_ids array<string>);

drop table if exists rels_exploded;
create table rels_exploded (identity char(32), type string,
  ep1_src_type string, ep1_type string, ep2_src_type string, ep2_type string,
  ep1_id char(32), ep2_id char(32));

select identity, ep1_id from relations
  lateral view explode(ep1_ids) nav_rel as ep1_id;

insert into rels_exploded select identity, type,
  ep1_src_type, ep1_type, ep2_src_type, ep2_type, ep1_id, ep2_id
from relations lateral view explode(ep1_ids) rel1 as ep1_id
  lateral view explode (ep2_ids) rel2 as ep2_id;

