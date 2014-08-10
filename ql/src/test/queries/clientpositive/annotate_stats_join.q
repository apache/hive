set hive.stats.fetch.column.stats=true;
set hive.stats.ndv.error=0.0;

create table if not exists emp (
  lastname string,
  deptid int,
  locid int
) row format delimited fields terminated by '|' stored as textfile;

create table if not exists dept (
  deptid int,
  deptname string
) row format delimited fields terminated by '|' stored as textfile;

create table if not exists loc (
  state string,
  locid int,
  zip bigint,
  year int
) row format delimited fields terminated by '|' stored as textfile;

LOAD DATA LOCAL INPATH '../../data/files/emp.txt' OVERWRITE INTO TABLE emp;
LOAD DATA LOCAL INPATH '../../data/files/dept.txt' OVERWRITE INTO TABLE dept;
LOAD DATA LOCAL INPATH '../../data/files/loc.txt' OVERWRITE INTO TABLE loc;

analyze table emp compute statistics;
analyze table dept compute statistics;
analyze table loc compute statistics;
analyze table emp compute statistics for columns lastname,deptid,locid;
analyze table dept compute statistics for columns deptname,deptid;
analyze table loc compute statistics for columns state,locid,zip,year;

-- number of rows
-- emp  - 48
-- dept - 6
-- loc  - 8

-- count distincts for relevant columns (since count distinct values are approximate in some cases count distint values will be greater than number of rows)
-- emp.deptid - 3
-- emp.lastname - 6
-- emp.locid - 7
-- dept.deptid - 7
-- dept.deptname - 6
-- loc.locid - 7
-- loc.state - 6

-- 2 relations, 1 attribute
-- Expected output rows: (48*6)/max(3,7) = 41
explain select * from emp e join dept d on (e.deptid = d.deptid);

-- 2 relations, 2 attributes
-- Expected output rows: (48*6)/(max(3,7) * max(6,6)) = 6
explain select * from emp,dept where emp.deptid = dept.deptid and emp.lastname = dept.deptname;
explain select * from emp e join dept d on (e.deptid = d.deptid and e.lastname = d.deptname);

-- 2 relations, 3 attributes
-- Expected output rows: (48*6)/(max(3,7) * max(6,6) * max(6,6)) = 1
explain select * from emp,dept where emp.deptid = dept.deptid and emp.lastname = dept.deptname and dept.deptname = emp.lastname;

-- 3 relations, 1 attribute
-- Expected output rows: (48*6*48)/top2largest(3,7,3) = 658
explain select * from emp e join dept d on (e.deptid = d.deptid) join emp e1 on (e.deptid = e1.deptid);

-- Expected output rows: (48*6*8)/top2largest(3,7,7) = 47
explain select * from emp e join dept d  on (e.deptid = d.deptid) join loc l on (e.deptid = l.locid);

-- 3 relations and 2 attribute
-- Expected output rows: (48*6*8)/top2largest(3,7,7)*top2largest(6,6,6) = 1
explain select * from emp e join dept d on (e.deptid = d.deptid and e.lastname = d.deptname) join loc l on (e.deptid = l.locid and e.lastname = l.state);

