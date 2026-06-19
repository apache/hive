set hive.stats.fetch.column.stats=true;
set hive.stats.ndv.error=0.0;

create table if not exists emp_n2 (
  lastname string,
  deptid int,
  locid int
) row format delimited fields terminated by '|' stored as textfile;

create table if not exists dept_n1 (
  deptid int,
  deptname string
) row format delimited fields terminated by '|' stored as textfile;

create table if not exists loc (
  state string,
  locid int,
  zip bigint,
  year int
) row format delimited fields terminated by '|' stored as textfile;

LOAD DATA LOCAL INPATH '../../data/files/emp.txt' OVERWRITE INTO TABLE emp_n2;
LOAD DATA LOCAL INPATH '../../data/files/dept.txt' OVERWRITE INTO TABLE dept_n1;
LOAD DATA LOCAL INPATH '../../data/files/loc.txt' OVERWRITE INTO TABLE loc;

analyze table emp_n2 compute statistics;
analyze table dept_n1 compute statistics;
analyze table loc compute statistics;
analyze table emp_n2 compute statistics for columns lastname,deptid,locid;
analyze table dept_n1 compute statistics for columns deptname,deptid;
analyze table loc compute statistics for columns state,locid,zip,year;

-- number of rows
-- emp_n2  - 48
-- dept_n1 - 6
-- loc  - 8

-- count distincts for relevant columns (since count distinct values are approximate in some cases count distint values will be greater than number of rows)
-- emp_n2.deptid - 3
-- emp_n2.lastname - 6
-- emp_n2.locid - 7
-- dept_n1.deptid - 7
-- dept_n1.deptname - 6
-- loc.locid - 7
-- loc.state - 6

-- 2 relations, 1 attribute
-- Expected output rows: (48*6)/max(3,7) = 41
explain select * from emp_n2 e join dept_n1 d on (e.deptid = d.deptid);

-- 2 relations, 2 attributes
-- Expected output rows: (48*6)/(max(3,7) * max(6,6)) = 6
explain select * from emp_n2,dept_n1 where emp_n2.deptid = dept_n1.deptid and emp_n2.lastname = dept_n1.deptname;
explain select * from emp_n2 e join dept_n1 d on (e.deptid = d.deptid and e.lastname = d.deptname);

-- 2 relations, 3 attributes
-- Expected output rows: (48*6)/(max(3,7) * max(6,6) * max(6,6)) = 1
explain select * from emp_n2,dept_n1 where emp_n2.deptid = dept_n1.deptid and emp_n2.lastname = dept_n1.deptname and dept_n1.deptname = emp_n2.lastname;

-- 3 relations, 1 attribute
-- Expected output rows: (48*6*48)/top2largest(3,7,3) = 658
explain select * from emp_n2 e join dept_n1 d on (e.deptid = d.deptid) join emp_n2 e1 on (e.deptid = e1.deptid);

-- Expected output rows: (48*6*8)/top2largest(3,7,7) = 47
explain select * from emp_n2 e join dept_n1 d  on (e.deptid = d.deptid) join loc l on (e.deptid = l.locid);

-- 3 relations and 2 attribute
-- Expected output rows: (48*6*8)/top2largest(3,7,7)*top2largest(6,6,6) = 1
explain select * from emp_n2 e join dept_n1 d on (e.deptid = d.deptid and e.lastname = d.deptname) join loc l on (e.deptid = l.locid and e.lastname = l.state);

-- left outer join
explain select * from emp_n2 left outer join dept_n1 on emp_n2.deptid = dept_n1.deptid and emp_n2.lastname = dept_n1.deptname and dept_n1.deptname = emp_n2.lastname;

-- left semi join
explain select * from emp_n2 left semi join dept_n1 on emp_n2.deptid = dept_n1.deptid and emp_n2.lastname = dept_n1.deptname and dept_n1.deptname = emp_n2.lastname;

-- right outer join
explain select * from emp_n2 right outer join dept_n1 on emp_n2.deptid = dept_n1.deptid and emp_n2.lastname = dept_n1.deptname and dept_n1.deptname = emp_n2.lastname;

-- full outer join
explain select * from emp_n2 full outer join dept_n1 on emp_n2.deptid = dept_n1.deptid and emp_n2.lastname = dept_n1.deptname and dept_n1.deptname = emp_n2.lastname;
