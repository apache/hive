DESCRIBE FUNCTION hour;
DESCRIBE FUNCTION minute;
DESCRIBE FUNCTION second;


DESCRIBE FUNCTION EXTENDED hour;
DESCRIBE FUNCTION EXTENDED minute;
DESCRIBE FUNCTION EXTENDED second;


EXPLAIN
SELECT hour('2009-08-07 13:14:15'), hour('13:14:15'), hour('2009-08-07'),
       minute('2009-08-07 13:14:15'), minute('13:14:15'), minute('2009-08-07'),
       second('2009-08-07 13:14:15'), second('13:14:15'), second('2009-08-07')
FROM src WHERE key = 86;

SELECT hour('2009-08-07 13:14:15'), hour('13:14:15'), hour('2009-08-07'),
       minute('2009-08-07 13:14:15'), minute('13:14:15'), minute('2009-08-07'),
       second('2009-08-07 13:14:15'), second('13:14:15'), second('2009-08-07')
FROM src WHERE key = 86;
