create table tbl_1(i1 int, j1 int);
insert into tbl_1 values(1,2),(1,null), (null, 200), (45,68);
create table tbl_2(i2 int, j2 int);
insert into tbl_2 values(1,2),(1,null), (null, 200), (45,68);

-- simple join
explain cbo select * from tbl_1 left join tbl_2 on tbl_1.i1 = tbl_2.i2;
select * from tbl_1 left join tbl_2 on tbl_1.i1 = tbl_2.i2;

explain cbo select * from tbl_1 right join tbl_2 on tbl_1.i1 = tbl_2.i2;
select * from tbl_1 right join tbl_2 on tbl_1.i1 = tbl_2.i2;

explain cbo select * from tbl_1 full outer join tbl_2 on tbl_1.i1 = tbl_2.i2;
select * from tbl_1 full outer join tbl_2 on tbl_1.i1 = tbl_2.i2;

-- conjunction
explain cbo select * from tbl_1 left join tbl_2 on tbl_1.i1 = tbl_2.i2 AND tbl_1.j1=tbl_2.j2;
select * from tbl_1 left join tbl_2 on tbl_1.i1 = tbl_2.i2 AND tbl_1.j1=tbl_2.j2;

explain cbo select * from tbl_1 right join tbl_2 on tbl_1.i1 = tbl_2.i2 AND tbl_1.j1=tbl_2.j2;
select * from tbl_1 right join tbl_2 on tbl_1.i1 = tbl_2.i2 AND tbl_1.j1=tbl_2.j2;

-- equi + non-equi
explain cbo select * from tbl_1 left join tbl_2 on tbl_1.i1 = tbl_2.i2 AND tbl_1.j1>tbl_2.j2;
select * from tbl_1 left join tbl_2 on tbl_1.i1 = tbl_2.i2 AND tbl_1.j1>tbl_2.j2;

explain cbo select * from tbl_1 right join tbl_2 on tbl_1.i1 = tbl_2.i2 AND tbl_1.j1>tbl_2.j2;
select * from tbl_1 right join tbl_2 on tbl_1.i1 = tbl_2.i2 AND tbl_1.j1>tbl_2.j2;

explain cbo SELECT t0.col0, t0.col1
FROM
  (
    SELECT i1 as col0, j1 as col1 FROM tbl_1
  ) AS t0
  LEFT JOIN
  (
    SELECT i2 as col0, j2 as col1 FROM tbl_2
  ) AS t1
ON t0.col0 = t1.col0 AND t0.col1 = t1.col1;

SELECT t0.col0, t0.col1
FROM
  (
    SELECT i1 as col0, j1 as col1 FROM tbl_1
  ) AS t0
  LEFT JOIN
  (
    SELECT i2 as col0, j2 as col1 FROM tbl_2
  ) AS t1
ON t0.col0 = t1.col0 AND t0.col1 = t1.col1;

DROP TABLE tbl_1;
DROP TABLE tbl_2;
