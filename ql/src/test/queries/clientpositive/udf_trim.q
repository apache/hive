DESCRIBE FUNCTION trim;
DESCRIBE FUNCTION EXTENDED trim;

SET hive.vectorized.execution.enabled=false;

SELECT trim(null);

SELECT '"' || trim(null, null) || '"';

SET hive.vectorized.execution.enabled=true;

SELECT trim(null);

SELECT '"' || trim(null, null) || '"';

SELECT '"' || trim('   tech   ') || '"';

SELECT '"' || TRIM(' '  FROM  '   tech   ') || '"';

SELECT '"' || TRIM(LEADING '0' FROM '000123') || '"';

SELECT '"' || TRIM(TRAILING '1' FROM 'Tech1') || '"';

SELECT '"' || TRIM(BOTH '1' FROM '123Tech111') || '"';

SELECT '"' || trim('xyfacebookyyx', 'xy') || '"';
