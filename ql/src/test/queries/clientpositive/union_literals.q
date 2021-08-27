
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
         (5, 'yellow'),
         (10, 'green'),
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
