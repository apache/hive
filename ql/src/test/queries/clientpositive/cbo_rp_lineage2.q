--! qt:disabled:disabled by 6eaef86ea736 in 2018
--! qt_n16:dataset_n16:src1
--! qt_n16:dataset_n16:src
set_n16 hive.mapred.mode=nonstrict_n16;
set_n16 hive.cbo.returnpath.hiveop=true;
set_n16 hive.exec.post_n16.hooks=org.apache.hadoop.hive.ql.hooks.LineageLogger;

drop table if exists src2_n1;
create table src2_n1 as select_n16 key key2, value value2 from src1;

select_n16 * from src1 where key is not_n16 null and value is not_n16 null limit_n16 3;
select_n16 * from src1 where key > 10 and value > 'val' order by key limit_n16 5;

drop table if exists dest1_n95;
create table dest1_n95 as select_n16 * from src1;
insert_n16 into table dest1_n95 select_n16 * from src2_n1;

select_n16 key k, dest1_n95.value from dest1_n95;
select_n16 key from src1 union select_n16 key2 from src2_n1 order by key;
select_n16 key k from src1 union select_n16 key2 from src2_n1 order by k;

select_n16 key, count_n16(1) a from dest1_n95 group by key;
select_n16 key k, count_n16(*) from dest1_n95 group by key;
select_n16 key k, count_n16(value) from dest1_n95 group by key;
select_n16 value, max(length(key)) from dest1_n95 group by value;
select_n16 value, max(length(key)) from dest1_n95 group by value order by value limit_n16 5;

select_n16 key, length(value) from dest1_n95;
select_n16 length(value) + 3 from dest1_n95;
select_n16 5 from dest1_n95;
select_n16 3 * 5 from dest1_n95;

drop table if exists dest2_n25;
create table dest2_n25 as select_n16 * from src1 JOIN src2_n1 ON src1.key = src2_n1.key2;
insert_n16 overwrite table dest2_n25 select_n16 * from src1 JOIN src2_n1 ON src1.key = src2_n1.key2;
insert_n16 into table dest2_n25 select_n16 * from src1 JOIN src2_n1 ON src1.key = src2_n1.key2;
insert_n16 into table dest2_n25
  select_n16 * from src1 JOIN src2_n1 ON length(src1.value) = length(src2_n1.value2) + 1;

select_n16 * from src1 where length(key) > 2;
select_n16 * from src1 where length(key) > 2 and value > 'a';

drop table if exists dest3_n3;
create table dest3_n3 as
  select_n16 * from src1 JOIN src2_n1 ON src1.key = src2_n1.key2 WHERE length(key) > 1;
insert_n16 overwrite table dest2_n25
  select_n16 * from src1 JOIN src2_n1 ON src1.key = src2_n1.key2 WHERE length(key) > 3;

drop table if exists dest_l1_n1;
CREATE TABLE dest_l1_n1(key INT, value STRING) STORED AS TEXTFILE;

INSERT OVERWRITE TABLE dest_l1_n1
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

drop table if exists emp_n1;
drop table if exists dept_n0;
drop table if exists project_n0;
drop table if exists tgt_n0;
create table emp_n1(emp_id int_n16, name string, mgr_id int_n16, dept_id int_n16);
create table dept_n0(dept_id int_n16, dept_name string);
create table project_n0(project_id int_n16, project_name string);
create table tgt_n0(dept_name string, name string,
  emp_id int_n16, mgr_id int_n16, proj_id int_n16, proj_name string);

INSERT INTO TABLE tgt_n0
SELECT emd.dept_name, emd.name, emd.emp_id, emd.mgr_id, p.project_id, p.project_name
FROM (
  SELECT d.dept_name, em.name, em.emp_id, em.mgr_id, em.dept_id
  FROM (
    SELECT e.name, e.dept_id, e.emp_id emp_id, m.emp_id mgr_id
    FROM emp_n1 e JOIN emp_n1 m ON e.emp_id = m.emp_id
    ) em
  JOIN dept_n0 d ON d.dept_id = em.dept_id
  ) emd JOIN project_n0 p ON emd.dept_id = p.project_id;

drop table if exists dest_l2_n0;
create table dest_l2_n0 (id int_n16, c1 tinyint_n16, c2 int_n16, c3 bigint_n16) stored as textfile;
insert_n16 into dest_l2_n0 values(0, 1, 100, 10000);

select_n16 * from (
  select_n16 c1 + c2 x from dest_l2_n0
  union all
  select_n16 sum(c3) y from (select_n16 c3 from dest_l2_n0) v1) v2 order by x;

drop table if exists dest_l3_n0;
create table dest_l3_n0 (id int_n16, c1 string, c2 string, c3 int_n16) stored as textfile;
insert_n16 into dest_l3_n0 values(0, "s1", "s2", 15);

select_n16 sum(a.c1) over (partition by a.c1 order by a.id)
from dest_l2_n0 a
where a.c2 != 10
group by a.c1, a.c2, a.id
having count_n16(a.c2) > 0;

select_n16 sum(a.c1), count_n16(b.c1), b.c2, b.c3
from dest_l2_n0 a join dest_l3_n0 b on (a.id = b.id)
where a.c2 != 10 and b.c3 > 0
group by a.c1, a.c2, a.id, b.c1, b.c2, b.c3
having count_n16(a.c2) > 0
order by b.c3 limit_n16 5;

drop table if exists t_n16;
create table t_n16 as
select_n16 distinct_n16 a.c2, a.c3 from dest_l2_n0 a
inner join dest_l3_n0 b on (a.id = b.id)
where a.id > 0 and b.c3 = 15;

SELECT substr(src1.key,1,1), count_n16(DISTINCT substr(src1.value,5)),
concat_n16(substr(src1.key,1,1),sum(substr(src1.value,5)))
from src1
GROUP BY substr(src1.key,1,1);

