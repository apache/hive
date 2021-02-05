DESCRIBE FUNCTION ltrim;
DESCRIBE FUNCTION EXTENDED ltrim;

SELECT '"' || ltrim('   tech   ') || '"';

SELECT '"' || ltrim('xyzzxyfacebook', 'zyx') || '"';
