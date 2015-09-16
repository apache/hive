set hive.optimize.point.lookup=false;

explain
SELECT key
FROM src
WHERE
   ((key = '0'
   AND value = '8') OR (key = '1'
   AND value = '5') OR (key = '2'
   AND value = '6') OR (key = '3'
   AND value = '8') OR (key = '4'
   AND value = '1') OR (key = '5'
   AND value = '6') OR (key = '6'
   AND value = '1') OR (key = '7'
   AND value = '1') OR (key = '8'
   AND value = '1') OR (key = '9'
   AND value = '1') OR (key = '10'
   AND value = '3'))
;
