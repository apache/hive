DESCRIBE FUNCTION unix_timestamp;
DESCRIBE FUNCTION EXTENDED unix_timestamp;

SELECT
  '2009-03-20 11:30:01',
  unix_timestamp('2009-03-20 11:30:01')
FROM src LIMIT 1;

SELECT
  '2009-03-20',
  unix_timestamp('2009-03-20', 'yyyy-MM-dd')
FROM src LIMIT 1;

SELECT
  '2009 Mar 20 11:30:01 am',
  unix_timestamp('2009 Mar 20 11:30:01 am', 'yyyy MMM dd h:mm:ss a')
FROM src LIMIT 1;

SELECT
  'random_string',
  unix_timestamp('random_string')
FROM src LIMIT 1;


