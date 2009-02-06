CREATE TABLE dest1(c1 STRING) STORED AS TEXTFILE;

FROM src INSERT OVERWRITE TABLE dest1 SELECT '  abc  ' WHERE src.key = 86;

EXPLAIN
SELECT LN(3.0), LN(0.0), LN(-1), LOG(3.0), LOG(0.0), LOG(-1), LOG2(3.0),
       LOG2(0.0), LOG2(-1), LOG10(3.0), LOG10(0.0), LOG10(-1), LOG(2, 3.0),
       LOG(2, 0.0), LOG(2, -1), LOG(0.5, 2), LOG(2, 0.5), EXP(2.0),
       POW(2,3), POWER(2,3), POWER(2,-3), POWER(0.5, -3), POWER(4, 0.5),
       POWER(-1, 0.5), POWER(-1, 2) FROM dest1;

SELECT LN(3.0), LN(0.0), LN(-1), LOG(3.0), LOG(0.0), LOG(-1), LOG2(3.0),
       LOG2(0.0), LOG2(-1), LOG10(3.0), LOG10(0.0), LOG10(-1), LOG(2, 3.0),
       LOG(2, 0.0), LOG(2, -1), LOG(0.5, 2), LOG(2, 0.5), EXP(2.0),
       POW(2,3), POWER(2,3), POWER(2,-3), POWER(0.5, -3), POWER(4, 0.5),
       POWER(-1, 0.5), POWER(-1, 2) FROM dest1;

