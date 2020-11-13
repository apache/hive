--! qt:dataset:srcpart
--! qt:dataset:src
set hive.mapred.mode=nonstrict;
set hive.optimize.ppd=true;
set hive.ppd.remove.duplicatefilters=true;

-- SORT_QUERY_RESULTS

explain
select b.key,b.cc
from (
  select a.*
  from (
    select key, count(value) as cc
    from srcpart a
    where a.ds = '2008-04-08' and a.hr = '11'
    group by key
  )a
  distribute by a.key
  sort by a.key,a.cc desc) b
where b.cc>1;

select b.key,b.cc
from (
  select a.*
  from (
    select key, count(value) as cc
    from srcpart a
    where a.ds = '2008-04-08' and a.hr = '11'
    group by key
  )a
  distribute by a.key
  sort by a.key,a.cc desc) b
where b.cc>1;

EXPLAIN
SELECT user_id 
FROM (
  SELECT 
  CAST(key AS INT) AS user_id
  ,CASE WHEN (value LIKE 'aaa%' OR value LIKE 'vvv%')
  THEN 1
  ELSE 0 END AS tag_student
  FROM srcpart
) sub
WHERE sub.tag_student > 0;

EXPLAIN 
SELECT x.key, x.value as v1, y.key  FROM SRC x JOIN SRC y ON (x.key = y.key)  where x.key = 20 CLUSTER BY v1;

set hive.optimize.ppd=true;
set hive.ppd.remove.duplicatefilters=false;

explain
select b.key,b.cc
from (
  select a.*
  from (
    select key, count(value) as cc
    from srcpart a
    where a.ds = '2008-04-08' and a.hr = '11'
    group by key
  )a
  distribute by a.key
  sort by a.key,a.cc desc) b
where b.cc>1;

select b.key,b.cc
from (
  select a.*
  from (
    select key, count(value) as cc
    from srcpart a
    where a.ds = '2008-04-08' and a.hr = '11'
    group by key
  )a
  distribute by a.key
  sort by a.key,a.cc desc) b
where b.cc>1;


set hive.explain.user=false;
set hive.cbo.enable=false;
set hive.optimize.ppd=true;
set hive.ppd.remove.duplicatefilters=true;

DROP TABLE arc;
CREATE table arc(`dt_from` string, `dt_to` string);

DROP TABLE loc1;
CREATE table loc1(`dt_from` string, `dt_to` string);

-- INSERT INTO arc VALUES('2020', '2020');
-- INSERT INTO loc1 VALUES('2020', '2020');

DROP VIEW view;
CREATE
 VIEW view AS
     SELECT
        '9999' as DT_FROM,
        uuid() as DT_TO
     FROM
       loc1
 UNION ALL
     SELECT
        dt_from as DT_FROM,
        uuid() as DT_TO
     FROM
       arc;

EXPLAIN
    SELECT
      dt_from, dt_to
    FROM
      view
    WHERE
      '2020'  between dt_from and dt_to;
