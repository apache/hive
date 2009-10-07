DESCRIBE FUNCTION concat_ws;
DESCRIBE FUNCTION EXTENDED concat_ws;

CREATE TABLE dest1(c1 STRING, c2 STRING, c3 STRING);

FROM src INSERT OVERWRITE TABLE dest1 SELECT 'abc', 'xyz', '8675309'  WHERE src.key = 86;

EXPLAIN
SELECT concat_ws(dest1.c1, dest1.c2, dest1.c3),
       concat_ws(',', dest1.c1, dest1.c2, dest1.c3),
       concat_ws(NULL, dest1.c1, dest1.c2, dest1.c3),
       concat_ws('**', dest1.c1, NULL, dest1.c3) FROM dest1;

SELECT concat_ws(dest1.c1, dest1.c2, dest1.c3),
       concat_ws(',', dest1.c1, dest1.c2, dest1.c3),
       concat_ws(NULL, dest1.c1, dest1.c2, dest1.c3),
       concat_ws('**', dest1.c1, NULL, dest1.c3) FROM dest1;

