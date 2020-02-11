set hive.mapred.mode=nonstrict;
drop table pokes_n0;
drop table pokes2_n0;
create table pokes_n0 (foo int, bar int, blah int);
create table pokes2_n0 (foo int, bar int, blah int);

-- Q1: predicate should not be pushed on the right side of a left outer join
explain
SELECT a.foo as foo1, b.foo as foo2, b.bar
FROM pokes_n0 a LEFT OUTER JOIN pokes2_n0 b
ON a.foo=b.foo
WHERE b.bar=3;

-- Q2: predicate should not be pushed on the right side of a left outer join
explain
SELECT * FROM
    (SELECT a.foo as foo1, b.foo as foo2, b.bar
    FROM pokes_n0 a LEFT OUTER JOIN pokes2_n0 b
    ON a.foo=b.foo) a
WHERE a.bar=3;

-- Q3: predicate should be pushed
explain
SELECT * FROM
    (SELECT a.foo as foo1, b.foo as foo2, a.bar
    FROM pokes_n0 a JOIN pokes2_n0 b
    ON a.foo=b.foo) a
WHERE a.bar=3;

-- Q4: here, the filter c.bar should be created under the first join but above the second
explain select c.foo, d.bar from (select c.foo, b.bar, c.blah from pokes_n0 c left outer join pokes_n0 b on c.foo=b.foo) c left outer join pokes_n0 d where d.foo=1 and c.bar=2;

drop table pokes_n0;
drop table pokes2_n0;
