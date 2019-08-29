set hive.cli.print.header=true;
set hive.mapred.mode=nonstrict;
set hive.explain.user=false;
set hive.fetch.task.conversion=none;
set hive.vectorized.execution.enabled=true;

create table timestamps_txt (tsval timestamp) STORED AS TEXTFILE;

LOAD DATA LOCAL INPATH '../../data/files/timestamps.txt' OVERWRITE INTO TABLE timestamps_txt;

create table timestamps (cdate date, ctimestamp1 timestamp,  stimestamp1 string,  ctimestamp2 timestamp) stored as orc;
insert overwrite table timestamps
  select cast(tsval as date), tsval, cast(tsval as string), tsval - '1 2:3:4' day to second from timestamps_txt;
  
INSERT INTO TABLE timestamps VALUES (NULL,NULL,NULL,NULL);

SET hive.vectorized.if.expr.mode=adaptor;

EXPLAIN VECTORIZATION DETAIL
SELECT
   ctimestamp1,
   ctimestamp2,
   CASE
     WHEN ctimestamp2 <= date '1800-12-31' THEN "1800s or Earlier"
     WHEN ctimestamp2 < date '1900-01-01' THEN "1900s"
     WHEN ctimestamp2 between cast('2006-01-01 00:00:00.000' as timestamp) and cast('2010-12-31 23:59:59.999999999' as timestamp) THEN "Late 2000s"
     WHEN ctimestamp2 <= timestamp '2015-12-31 23:59:59.999999999' THEN "Early 2010s"
     ELSE "Unknown" END AS ctimestamp2_Description,
   CASE
     WHEN ctimestamp2 <= timestamp '2000-12-31 23:59:59.999999999' THEN "Old"
     WHEN ctimestamp2 < timestamp '2006-01-01 00:00:00.000' THEN "Early 2000s"
     WHEN ctimestamp2 between cast('2006-01-01 00:00:00.000' as timestamp) and cast('2010-12-31 23:59:59.999999999' as timestamp) THEN "Late 2000s"
     WHEN ctimestamp2 <= timestamp '2015-12-31 23:59:59.999999999' THEN "Early 2010s"
     ELSE NULL END AS ctimestamp2_Description_2,
   CASE
     WHEN ctimestamp2 <= timestamp '2000-12-31 23:59:59.999999999' THEN "Old"
     WHEN ctimestamp2 < timestamp '2006-01-01 00:00:00.000' THEN "Early 2000s"
     WHEN ctimestamp2 between timestamp '2006-01-01 00:00:00.000' and timestamp '2010-12-31 23:59:59.999999999' THEN "Late 2000s"
     WHEN ctimestamp2 <= timestamp '2015-12-31 23:59:59.999999999' THEN NULL
     ELSE NULL END AS ctimestamp2_Description_3,
   IF(timestamp '1974-10-04 17:21:03.989' > ctimestamp1, year(ctimestamp1), year(ctimestamp2)) AS field1,
   CASE WHEN stimestamp1 LIKE '%19%'
            THEN stimestamp1
        ELSE timestamp '2018-03-08 23:04:59' END AS Field_2,
   IF(ctimestamp1 = timestamp '2021-09-24 03:18:32.413655165' , NULL, minute(ctimestamp1)) AS Field_3,
   IF(ctimestamp2 >= timestamp '5344-10-04 18:40:08.165' and ctimestamp2 < timestamp '6631-11-13 16:31:29.702202248', minute(ctimestamp1), NULL) AS Field_4,
   IF(cast(ctimestamp1 as double) % 500 > 100, DATE_ADD(cdate, 1), DATE_ADD(cdate, 365)) AS Field_5
FROM timestamps
ORDER BY ctimestamp1, stimestamp1, ctimestamp2;
SELECT
   ctimestamp1,
   ctimestamp2,
   CASE
     WHEN ctimestamp2 <= date '1800-12-31' THEN "1800s or Earlier"
     WHEN ctimestamp2 < date '1900-01-01' THEN "1900s"
     WHEN ctimestamp2 between cast('2006-01-01 00:00:00.000' as timestamp) and cast('2010-12-31 23:59:59.999999999' as timestamp) THEN "Late 2000s"
     WHEN ctimestamp2 <= timestamp '2015-12-31 23:59:59.999999999' THEN "Early 2010s"
     ELSE "Unknown" END AS ctimestamp2_Description,
   CASE
     WHEN ctimestamp2 <= timestamp '2000-12-31 23:59:59.999999999' THEN "Old"
     WHEN ctimestamp2 < timestamp '2006-01-01 00:00:00.000' THEN "Early 2000s"
     WHEN ctimestamp2 between cast('2006-01-01 00:00:00.000' as timestamp) and cast('2010-12-31 23:59:59.999999999' as timestamp) THEN "Late 2000s"
     WHEN ctimestamp2 <= timestamp '2015-12-31 23:59:59.999999999' THEN "Early 2010s"
     ELSE NULL END AS ctimestamp2_Description_2,
   CASE
     WHEN ctimestamp2 <= timestamp '2000-12-31 23:59:59.999999999' THEN "Old"
     WHEN ctimestamp2 < timestamp '2006-01-01 00:00:00.000' THEN "Early 2000s"
     WHEN ctimestamp2 between timestamp '2006-01-01 00:00:00.000' and timestamp '2010-12-31 23:59:59.999999999' THEN "Late 2000s"
     WHEN ctimestamp2 <= timestamp '2015-12-31 23:59:59.999999999' THEN NULL
     ELSE NULL END AS ctimestamp2_Description_3,
   IF(timestamp '1974-10-04 17:21:03.989' > ctimestamp1, year(ctimestamp1), year(ctimestamp2)) AS field1,
   CASE WHEN stimestamp1 LIKE '%19%'
            THEN stimestamp1
        ELSE timestamp '2018-03-08 23:04:59' END AS Field_2,
   IF(ctimestamp1 = timestamp '2021-09-24 03:18:32.413655165' , NULL, minute(ctimestamp1)) AS Field_3,
   IF(ctimestamp2 >= timestamp '5344-10-04 18:40:08.165' and ctimestamp2 < timestamp '6631-11-13 16:31:29.702202248', minute(ctimestamp1), NULL) AS Field_4,
   IF(cast(ctimestamp1 as double) % 500 > 100, DATE_ADD(cdate, 1), DATE_ADD(cdate, 365)) AS Field_5
FROM timestamps
ORDER BY ctimestamp1, stimestamp1, ctimestamp2;

SET hive.vectorized.if.expr.mode=good;

EXPLAIN VECTORIZATION DETAIL
SELECT
   ctimestamp1,
   ctimestamp2,
   CASE
     WHEN ctimestamp2 <= date '1800-12-31' THEN "1800s or Earlier"
     WHEN ctimestamp2 < date '1900-01-01' THEN "1900s"
     WHEN ctimestamp2 between cast('2006-01-01 00:00:00.000' as timestamp) and cast('2010-12-31 23:59:59.999999999' as timestamp) THEN "Late 2000s"
     WHEN ctimestamp2 <= timestamp '2015-12-31 23:59:59.999999999' THEN "Early 2010s"
     ELSE "Unknown" END AS ctimestamp2_Description,
   CASE
     WHEN ctimestamp2 <= timestamp '2000-12-31 23:59:59.999999999' THEN "Old"
     WHEN ctimestamp2 < timestamp '2006-01-01 00:00:00.000' THEN "Early 2000s"
     WHEN ctimestamp2 between cast('2006-01-01 00:00:00.000' as timestamp) and cast('2010-12-31 23:59:59.999999999' as timestamp) THEN "Late 2000s"
     WHEN ctimestamp2 <= timestamp '2015-12-31 23:59:59.999999999' THEN "Early 2010s"
     ELSE NULL END AS ctimestamp2_Description_2,
   CASE
     WHEN ctimestamp2 <= timestamp '2000-12-31 23:59:59.999999999' THEN "Old"
     WHEN ctimestamp2 < timestamp '2006-01-01 00:00:00.000' THEN "Early 2000s"
     WHEN ctimestamp2 between timestamp '2006-01-01 00:00:00.000' and timestamp '2010-12-31 23:59:59.999999999' THEN "Late 2000s"
     WHEN ctimestamp2 <= timestamp '2015-12-31 23:59:59.999999999' THEN NULL
     ELSE NULL END AS ctimestamp2_Description_3,
   IF(timestamp '1974-10-04 17:21:03.989' > ctimestamp1, year(ctimestamp1), year(ctimestamp2)) AS field1,
   CASE WHEN stimestamp1 LIKE '%19%'
            THEN stimestamp1
        ELSE timestamp '2018-03-08 23:04:59' END AS Field_2,
   IF(ctimestamp1 = timestamp '2021-09-24 03:18:32.413655165' , NULL, minute(ctimestamp1)) AS Field_3,
   IF(ctimestamp2 >= timestamp '5344-10-04 18:40:08.165' and ctimestamp2 < timestamp '6631-11-13 16:31:29.702202248', minute(ctimestamp1), NULL) AS Field_4,
   IF(cast(ctimestamp1 as double) % 500 > 100, DATE_ADD(cdate, 1), DATE_ADD(cdate, 365)) AS Field_5
FROM timestamps
ORDER BY ctimestamp1, stimestamp1, ctimestamp2;
SELECT
   ctimestamp1,
   ctimestamp2,
   CASE
     WHEN ctimestamp2 <= date '1800-12-31' THEN "1800s or Earlier"
     WHEN ctimestamp2 < date '1900-01-01' THEN "1900s"
     WHEN ctimestamp2 between cast('2006-01-01 00:00:00.000' as timestamp) and cast('2010-12-31 23:59:59.999999999' as timestamp) THEN "Late 2000s"
     WHEN ctimestamp2 <= timestamp '2015-12-31 23:59:59.999999999' THEN "Early 2010s"
     ELSE "Unknown" END AS ctimestamp2_Description,
   CASE
     WHEN ctimestamp2 <= timestamp '2000-12-31 23:59:59.999999999' THEN "Old"
     WHEN ctimestamp2 < timestamp '2006-01-01 00:00:00.000' THEN "Early 2000s"
     WHEN ctimestamp2 between cast('2006-01-01 00:00:00.000' as timestamp) and cast('2010-12-31 23:59:59.999999999' as timestamp) THEN "Late 2000s"
     WHEN ctimestamp2 <= timestamp '2015-12-31 23:59:59.999999999' THEN "Early 2010s"
     ELSE NULL END AS ctimestamp2_Description_2,
   CASE
     WHEN ctimestamp2 <= timestamp '2000-12-31 23:59:59.999999999' THEN "Old"
     WHEN ctimestamp2 < timestamp '2006-01-01 00:00:00.000' THEN "Early 2000s"
     WHEN ctimestamp2 between timestamp '2006-01-01 00:00:00.000' and timestamp '2010-12-31 23:59:59.999999999' THEN "Late 2000s"
     WHEN ctimestamp2 <= timestamp '2015-12-31 23:59:59.999999999' THEN NULL
     ELSE NULL END AS ctimestamp2_Description_3,
   IF(timestamp '1974-10-04 17:21:03.989' > ctimestamp1, year(ctimestamp1), year(ctimestamp2)) AS field1,
   CASE WHEN stimestamp1 LIKE '%19%'
            THEN stimestamp1
        ELSE timestamp '2018-03-08 23:04:59' END AS Field_2,
   IF(ctimestamp1 = timestamp '2021-09-24 03:18:32.413655165' , NULL, minute(ctimestamp1)) AS Field_3,
   IF(ctimestamp2 >= timestamp '5344-10-04 18:40:08.165' and ctimestamp2 < timestamp '6631-11-13 16:31:29.702202248', minute(ctimestamp1), NULL) AS Field_4,
   IF(cast(ctimestamp1 as double) % 500 > 100, DATE_ADD(cdate, 1), DATE_ADD(cdate, 365)) AS Field_5
FROM timestamps
ORDER BY ctimestamp1, stimestamp1, ctimestamp2;

SET hive.vectorized.if.expr.mode=better;

EXPLAIN VECTORIZATION DETAIL
SELECT
   ctimestamp1,
   ctimestamp2,
   CASE
     WHEN ctimestamp2 <= date '1800-12-31' THEN "1800s or Earlier"
     WHEN ctimestamp2 < date '1900-01-01' THEN "1900s"
     WHEN ctimestamp2 between cast('2006-01-01 00:00:00.000' as timestamp) and cast('2010-12-31 23:59:59.999999999' as timestamp) THEN "Late 2000s"
     WHEN ctimestamp2 <= timestamp '2015-12-31 23:59:59.999999999' THEN "Early 2010s"
     ELSE "Unknown" END AS ctimestamp2_Description,
   CASE
     WHEN ctimestamp2 <= timestamp '2000-12-31 23:59:59.999999999' THEN "Old"
     WHEN ctimestamp2 < timestamp '2006-01-01 00:00:00.000' THEN "Early 2000s"
     WHEN ctimestamp2 between cast('2006-01-01 00:00:00.000' as timestamp) and cast('2010-12-31 23:59:59.999999999' as timestamp) THEN "Late 2000s"
     WHEN ctimestamp2 <= timestamp '2015-12-31 23:59:59.999999999' THEN "Early 2010s"
     ELSE NULL END AS ctimestamp2_Description_2,
   CASE
     WHEN ctimestamp2 <= timestamp '2000-12-31 23:59:59.999999999' THEN "Old"
     WHEN ctimestamp2 < timestamp '2006-01-01 00:00:00.000' THEN "Early 2000s"
     WHEN ctimestamp2 between timestamp '2006-01-01 00:00:00.000' and timestamp '2010-12-31 23:59:59.999999999' THEN "Late 2000s"
     WHEN ctimestamp2 <= timestamp '2015-12-31 23:59:59.999999999' THEN NULL
     ELSE NULL END AS ctimestamp2_Description_3,
   IF(timestamp '1974-10-04 17:21:03.989' > ctimestamp1, year(ctimestamp1), year(ctimestamp2)) AS field1,
   CASE WHEN stimestamp1 LIKE '%19%'
            THEN stimestamp1
        ELSE timestamp '2018-03-08 23:04:59' END AS Field_2,
   IF(ctimestamp1 = timestamp '2021-09-24 03:18:32.413655165' , NULL, minute(ctimestamp1)) AS Field_3,
   IF(ctimestamp2 >= timestamp '5344-10-04 18:40:08.165' and ctimestamp2 < timestamp '6631-11-13 16:31:29.702202248', minute(ctimestamp1), NULL) AS Field_4,
   IF(cast(ctimestamp1 as double) % 500 > 100, DATE_ADD(cdate, 1), DATE_ADD(cdate, 365)) AS Field_5
FROM timestamps
ORDER BY ctimestamp1, stimestamp1, ctimestamp2;
SELECT
   ctimestamp1,
   ctimestamp2,
   CASE
     WHEN ctimestamp2 <= date '1800-12-31' THEN "1800s or Earlier"
     WHEN ctimestamp2 < date '1900-01-01' THEN "1900s"
     WHEN ctimestamp2 between cast('2006-01-01 00:00:00.000' as timestamp) and cast('2010-12-31 23:59:59.999999999' as timestamp) THEN "Late 2000s"
     WHEN ctimestamp2 <= timestamp '2015-12-31 23:59:59.999999999' THEN "Early 2010s"
     ELSE "Unknown" END AS ctimestamp2_Description,
   CASE
     WHEN ctimestamp2 <= timestamp '2000-12-31 23:59:59.999999999' THEN "Old"
     WHEN ctimestamp2 < timestamp '2006-01-01 00:00:00.000' THEN "Early 2000s"
     WHEN ctimestamp2 between cast('2006-01-01 00:00:00.000' as timestamp) and cast('2010-12-31 23:59:59.999999999' as timestamp) THEN "Late 2000s"
     WHEN ctimestamp2 <= timestamp '2015-12-31 23:59:59.999999999' THEN "Early 2010s"
     ELSE NULL END AS ctimestamp2_Description_2,
   CASE
     WHEN ctimestamp2 <= timestamp '2000-12-31 23:59:59.999999999' THEN "Old"
     WHEN ctimestamp2 < timestamp '2006-01-01 00:00:00.000' THEN "Early 2000s"
     WHEN ctimestamp2 between timestamp '2006-01-01 00:00:00.000' and timestamp '2010-12-31 23:59:59.999999999' THEN "Late 2000s"
     WHEN ctimestamp2 <= timestamp '2015-12-31 23:59:59.999999999' THEN NULL
     ELSE NULL END AS ctimestamp2_Description_3,
   IF(timestamp '1974-10-04 17:21:03.989' > ctimestamp1, year(ctimestamp1), year(ctimestamp2)) AS field1,
   CASE WHEN stimestamp1 LIKE '%19%'
            THEN stimestamp1
        ELSE timestamp '2018-03-08 23:04:59' END AS Field_2,
   IF(ctimestamp1 = timestamp '2021-09-24 03:18:32.413655165' , NULL, minute(ctimestamp1)) AS Field_3,
   IF(ctimestamp2 >= timestamp '5344-10-04 18:40:08.165' and ctimestamp2 < timestamp '6631-11-13 16:31:29.702202248', minute(ctimestamp1), NULL) AS Field_4,
   IF(cast(ctimestamp1 as double) % 500 > 100, DATE_ADD(cdate, 1), DATE_ADD(cdate, 365)) AS Field_5
FROM timestamps
ORDER BY ctimestamp1, stimestamp1, ctimestamp2;


create temporary table foo(q548284 int);
insert into foo values(1),(2),(3),(4),(5),(6);

set hive.cbo.enable=true;
explain vectorization detail select q548284, CASE WHEN ((q548284 = 1)) THEN (0.2)
    WHEN ((q548284 = 2)) THEN (0.4) WHEN ((q548284 = 3)) THEN (0.6) WHEN ((q548284 = 4))
    THEN (0.8) WHEN ((q548284 = 5)) THEN (1) ELSE (null) END from foo order by q548284 limit 1;
select q548284, CASE WHEN ((q548284 = 1)) THEN (0.2)
    WHEN ((q548284 = 2)) THEN (0.4) WHEN ((q548284 = 3)) THEN (0.6) WHEN ((q548284 = 4))
    THEN (0.8) WHEN ((q548284 = 5)) THEN (1) ELSE (null) END from foo order by q548284 limit 1;

explain vectorization detail select q548284, CASE WHEN  ((q548284 = 4)) THEN (0.8)
    WHEN ((q548284 = 5)) THEN (1) ELSE (8) END from foo order by q548284 limit 1;
select q548284, CASE WHEN  ((q548284 = 4)) THEN (0.8) WHEN ((q548284 = 5)) THEN (1) ELSE (8) END
    from foo order by q548284 limit 1;

set hive.cbo.enable=false;
explain vectorization detail select q548284, CASE WHEN ((q548284 = 1)) THEN (0.2)
    WHEN ((q548284 = 2)) THEN (0.4) WHEN ((q548284 = 3)) THEN (0.6) WHEN ((q548284 = 4))
    THEN (0.8) WHEN ((q548284 = 5)) THEN (1) ELSE (null) END from foo order by q548284 limit 1;
select q548284, CASE WHEN ((q548284 = 1)) THEN (0.2)
    WHEN ((q548284 = 2)) THEN (0.4) WHEN ((q548284 = 3)) THEN (0.6) WHEN ((q548284 = 4))
    THEN (0.8) WHEN ((q548284 = 5)) THEN (1) ELSE (null) END from foo order by q548284 limit 1;

explain vectorization detail select q548284, CASE WHEN  ((q548284 = 4)) THEN (0.8)
    WHEN ((q548284 = 5)) THEN (1) ELSE (8) END from foo order by q548284 limit 1;
select q548284, CASE WHEN  ((q548284 = 4)) THEN (0.8) WHEN ((q548284 = 5)) THEN (1) ELSE (8) END
    from foo order by q548284 limit 1;
