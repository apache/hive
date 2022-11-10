

explain
SELECT * FROM (
   VALUES(1, '1'),
         (2, 'orange'),
         (5, 'yellow'),
         (10, 'green'),
         (11, 'blue'),
         (12, 'indigo'),
         (20, 'violet'))
   AS Colors
;

explain
SELECT * FROM (
   VALUES(1, '1'),
         (2, 'orange'),
         (5, 'yellow'),
         (10, 'green'),
         (11, 'blue'),
         (12, 'indigo'),
         (20, 'violet'))
   AS Colors
union all
  select 2,'2'
union all
  select 2,'2'
;

SELECT * FROM (
   VALUES(1, '1'),
         (2, 'orange'),
         (5, 'yellow'))
   as colors1
UNION ALL
SELECT * FROM (
   VALUES(10, 'green'),
         (11, 'blue'),
         (12, 'indigo'),
         (20, 'violet'))
   AS Colors
union all
  select 2,'2'
union all
  select 2,'2';


explain
  select 1, 1
union all
  select 2, 2
union all
  select 3, 3
;

  select 1, 1
union all
  select 2, 2
union all
  select 3, 3
;


create table t (a string, b string);
insert into t values(9,9);

explain cbo
  select cast(a as integer) from t
union all
  select cast(1 as integer)
;

explain cbo
  select cast(a as integer) from t
union all
  select cast(1 as integer)
union all
  select cast(2 as integer)
;



explain
  select cast(a as integer) from t
union all
  select cast(1 as integer)
union all
  select cast(2 as integer)
union all
  select cast(3 as integer)
;

  select cast(a as integer) from t
union all
  select cast(1 as integer)
union all
  select cast(2 as integer)
union all
  select cast(3 as integer)
;

  select cast(a as integer) from t;
