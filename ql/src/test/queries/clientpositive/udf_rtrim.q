DESCRIBE FUNCTION rtrim;
DESCRIBE FUNCTION EXTENDED rtrim;

SELECT '"' || rtrim('   tech   ') || '"';

SELECT '"' || rtrim('facebookxyzzyx', 'xzy') || '"';
