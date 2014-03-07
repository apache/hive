DROP TABLE IF EXISTS bool_literal;

CREATE TABLE bool_literal(key int, value boolean);

LOAD DATA LOCAL INPATH '../../data/files/bool_literal.txt' INTO TABLE bool_literal;

SET hive.lazysimple.extended_boolean_literal=false;

SELECT * FROM bool_literal;

SET hive.lazysimple.extended_boolean_literal=true;

SELECT * FROM bool_literal;

DROP TABLE bool_literal;