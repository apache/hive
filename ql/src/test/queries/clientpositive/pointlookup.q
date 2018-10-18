--! qt:dataset:src

set hive.optimize.point.lookup.min=31;

explain
SELECT key
FROM src
WHERE
   ((key = '0'  AND value = 'val_0') OR
    (key = '1'  AND value = 'val_1') OR
    (key = '2'  AND value = 'val_2') OR
    (key = '3'  AND value = 'val_3') OR
    (key = '4'  AND value = 'val_4') OR
    (key = '5'  AND value = 'val_5') OR
    (key = '6'  AND value = 'val_6') OR
    (key = '7'  AND value = 'val_7') OR
    (key = '8'  AND value = 'val_8') OR
    (key = '9'  AND value = 'val_9') OR
    (key = '10' AND value = 'val_10'))
;

create table orOutput as
SELECT key
FROM src
WHERE
   ((key = '0'  AND value = 'val_0') OR
    (key = '1'  AND value = 'val_1') OR
    (key = '2'  AND value = 'val_2') OR
    (key = '3'  AND value = 'val_3') OR
    (key = '4'  AND value = 'val_4') OR
    (key = '5'  AND value = 'val_5') OR
    (key = '6'  AND value = 'val_6') OR
    (key = '7'  AND value = 'val_7') OR
    (key = '8'  AND value = 'val_8') OR
    (key = '9'  AND value = 'val_9') OR
    (key = '10' AND value = 'val_10'))
;

set hive.optimize.point.lookup.min=3;
set hive.optimize.partition.columns.separate=false;
explain
SELECT key
FROM src
WHERE
   ((key = '0'  AND value = 'val_0') OR
    (key = '1'  AND value = 'val_1') OR
    (key = '2'  AND value = 'val_2') OR
    (key = '3'  AND value = 'val_3') OR
    (key = '4'  AND value = 'val_4') OR
    (key = '5'  AND value = 'val_5') OR
    (key = '6'  AND value = 'val_6') OR
    (key = '7'  AND value = 'val_7') OR
    (key = '8'  AND value = 'val_8') OR
    (key = '9'  AND value = 'val_9') OR
    (key = '10' AND value = 'val_10'))
;

create table inOutput as
SELECT key
FROM src
WHERE
   ((key = '0'  AND value = 'val_0') OR
    (key = '1'  AND value = 'val_1') OR
    (key = '2'  AND value = 'val_2') OR
    (key = '3'  AND value = 'val_3') OR
    (key = '4'  AND value = 'val_4') OR
    (key = '5'  AND value = 'val_5') OR
    (key = '6'  AND value = 'val_6') OR
    (key = '7'  AND value = 'val_7') OR
    (key = '8'  AND value = 'val_8') OR
    (key = '9'  AND value = 'val_9') OR
    (key = '10' AND value = 'val_10'))
;

set hive.optimize.partition.columns.separate=true;
explain
SELECT key
FROM src
WHERE
   ((key = '0'  AND value = 'val_0') OR
    (key = '1'  AND value = 'val_1') OR
    (key = '2'  AND value = 'val_2') OR
    (key = '3'  AND value = 'val_3') OR
    (key = '4'  AND value = 'val_4') OR
    (key = '5'  AND value = 'val_5') OR
    (key = '6'  AND value = 'val_6') OR
    (key = '7'  AND value = 'val_7') OR
    (key = '8'  AND value = 'val_8') OR
    (key = '9'  AND value = 'val_9') OR
    (key = '10' AND value = 'val_10'))
;

create table inOutputOpt as
SELECT key
FROM src
WHERE
   ((key = '0'  AND value = 'val_0') OR
    (key = '1'  AND value = 'val_1') OR
    (key = '2'  AND value = 'val_2') OR
    (key = '3'  AND value = 'val_3') OR
    (key = '4'  AND value = 'val_4') OR
    (key = '5'  AND value = 'val_5') OR
    (key = '6'  AND value = 'val_6') OR
    (key = '7'  AND value = 'val_7') OR
    (key = '8'  AND value = 'val_8') OR
    (key = '9'  AND value = 'val_9') OR
    (key = '10' AND value = 'val_10'))
;

-- Output from all these tables should be the same
select count(*) from orOutput;
select count(*) from inOutput;
select count(*) from inOutputOpt;

-- check that orOutput and inOutput matches using full outer join
select orOutput.key, inOutput.key
from orOutput full outer join inOutput on (orOutput.key = inOutput.key)
where orOutput.key = null
or inOutput.key = null;

-- check that ourOutput and inOutputOpt matches using full outer join
select orOutput.key, inOutputOpt.key
from orOutput full outer join inOutputOpt on (orOutput.key = inOutputOpt.key)
where orOutput.key = null
or inOutputOpt.key = null;

drop table orOutput;
drop table inOutput;
drop table inOutputOpt;
