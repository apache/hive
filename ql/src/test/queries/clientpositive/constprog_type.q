set hive.optimize.constant.propagation=true;

CREATE TABLE dest1(d date, t timestamp);

EXPLAIN
INSERT OVERWRITE TABLE dest1
SELECT cast('2013-11-17' as date), cast(cast('1.3041352164485E9' as double) as timestamp)
       FROM src tablesample (1 rows);

INSERT OVERWRITE TABLE dest1
SELECT cast('2013-11-17' as date), cast(cast('1.3041352164485E9' as double) as timestamp)
       FROM src tablesample (1 rows);

SELECT * FROM dest1;
