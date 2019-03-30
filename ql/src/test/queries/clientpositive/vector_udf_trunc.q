--! qt:dataset:alltypesorc
set hive.fetch.task.conversion=none;
set hive.vectorized.execution.enabled=true;

DESCRIBE FUNCTION trunc;
DESCRIBE FUNCTION EXTENDED trunc;

CREATE TABLE trunc_number(c DOUBLE) STORED AS ORC;
INSERT INTO TABLE trunc_number VALUES (12345.54321);
INSERT INTO TABLE trunc_number VALUES (12345);
INSERT INTO TABLE trunc_number VALUES (0.54321);
INSERT INTO TABLE trunc_number VALUES (NULL);

-- trunc date from timestamp
explain vectorization detail select trunc(ctimestamp1, 'MM') from alltypesorc;

select trunc(ctimestamp1, 'MM'), ctimestamp1 from alltypesorc order by ctimestamp1 LIMIT 10;
select trunc(ctimestamp1, 'Q'), ctimestamp1 from alltypesorc order by ctimestamp1 LIMIT 10;
select trunc(ctimestamp1, 'YEAR'), ctimestamp1 from alltypesorc order by ctimestamp1 LIMIT 10;

-- trunc date from string
explain vectorization detail select trunc(CAST(ctimestamp1 AS STRING), 'MM') from alltypesorc;

select trunc(CAST(ctimestamp1 AS STRING), 'MM'), ctimestamp1 from alltypesorc order by ctimestamp1 LIMIT 10;
select trunc(CAST(ctimestamp1 AS STRING), 'Q'), ctimestamp1 from alltypesorc order by ctimestamp1 LIMIT 10;
select trunc(CAST(ctimestamp1 AS STRING), 'YEAR'), ctimestamp1 from alltypesorc order by ctimestamp1 LIMIT 10;

-- trunc date from date
explain vectorization detail select trunc(CAST(ctimestamp1 AS DATE), 'MM') from alltypesorc;

select trunc(CAST(ctimestamp1 AS DATE), 'MM'), ctimestamp1 from alltypesorc order by ctimestamp1 LIMIT 10;
select trunc(CAST(ctimestamp1 AS DATE), 'Q'), ctimestamp1 from alltypesorc order by ctimestamp1 LIMIT 10;
select trunc(CAST(ctimestamp1 AS DATE), 'YEAR'), ctimestamp1 from alltypesorc order by ctimestamp1 LIMIT 10;

select trunc(ctimestamp1, 'MM'), ctimestamp1 from alltypesorc WHERE ctimestamp1 IS NULL LIMIT 10;

-- trunc double
explain vectorization detail
select c, trunc(c,0) from trunc_number order by c;
select c, 0, trunc(c,0) from trunc_number order by c;
select c, -1, trunc(c,-1) from trunc_number order by c;
select c, 1, trunc(c,1) from trunc_number order by c;

-- trunc float
explain vectorization detail
select c, trunc(CAST (c AS FLOAT), 0) from trunc_number order by c;
select c, 0, trunc(CAST (c AS FLOAT), 0) from trunc_number order by c;
select c, -1, trunc(CAST (c AS FLOAT), -1) from trunc_number order by c;
select c, 1, trunc(CAST (c AS FLOAT), 1) from trunc_number order by c;

-- trunc decimal
explain vectorization detail
select c, trunc(CAST (c AS DECIMAL(10,5)), 0) from trunc_number order by c;
select c, 0, trunc(CAST (c AS DECIMAL(10,5)), 0) from trunc_number order by c;
select c, -1, trunc(CAST (c AS DECIMAL(10,5)), -1) from trunc_number order by c;
select c, 1, trunc(CAST (c AS DECIMAL(10,5)), 1) from trunc_number order by c;

-- scale not defined -> 0 (float)
explain vectorization detail
select c, trunc(c) from trunc_number order by c;
select c, trunc(c) from trunc_number order by c;

-- scale not defined -> 0 (decimal)
explain vectorization detail
select c, trunc(CAST (c AS DECIMAL(10,5))) from trunc_number order by c;
select c, trunc(CAST (c AS DECIMAL(10,5))) from trunc_number order by c;



set hive.vectorized.execution.enabled=false;

-- trunc date from timestamp
select trunc(ctimestamp1, 'MM'), ctimestamp1 from alltypesorc order by ctimestamp1 LIMIT 10;
select trunc(ctimestamp1, 'Q'), ctimestamp1 from alltypesorc order by ctimestamp1 LIMIT 10;
select trunc(ctimestamp1, 'YEAR'), ctimestamp1 from alltypesorc order by ctimestamp1 LIMIT 10;

-- trunc date from string
select trunc(CAST(ctimestamp1 AS STRING), 'MM'), ctimestamp1 from alltypesorc order by ctimestamp1 LIMIT 10;
select trunc(CAST(ctimestamp1 AS STRING), 'Q'), ctimestamp1 from alltypesorc order by ctimestamp1 LIMIT 10;
select trunc(CAST(ctimestamp1 AS STRING), 'YEAR'), ctimestamp1 from alltypesorc order by ctimestamp1 LIMIT 10;

-- trunc date from date
select trunc(CAST(ctimestamp1 AS DATE), 'MM'), ctimestamp1 from alltypesorc order by ctimestamp1 LIMIT 10;
select trunc(CAST(ctimestamp1 AS DATE), 'Q'), ctimestamp1 from alltypesorc order by ctimestamp1 LIMIT 10;
select trunc(CAST(ctimestamp1 AS DATE), 'YEAR'), ctimestamp1 from alltypesorc order by ctimestamp1 LIMIT 10;

select trunc(ctimestamp1, 'MM'), ctimestamp1 from alltypesorc WHERE ctimestamp1 IS NULL LIMIT 10;

-- trunc double
select c, 0, trunc(c,0) from trunc_number order by c;
select c, -1, trunc(c,-1) from trunc_number order by c;
select c, 1, trunc(c,1) from trunc_number order by c;

-- trunc float
select c, 0, trunc(CAST (c AS FLOAT), 0) from trunc_number order by c;
select c, -1, trunc(CAST (c AS FLOAT), -1) from trunc_number order by c;
select c, 1, trunc(CAST (c AS FLOAT), 1) from trunc_number order by c;

-- trunc decimal
select c, 0, trunc(CAST (c AS DECIMAL(10,5)), 0) from trunc_number order by c;
select c, -1, trunc(CAST (c AS DECIMAL(10,5)), -1) from trunc_number order by c;
select c, 1, trunc(CAST (c AS DECIMAL(10,5)), 1) from trunc_number order by c;

-- scale not defined -> 0 (float)
select c, trunc(c) from trunc_number order by c;

-- scale not defined -> 0 (decimal)
select c, trunc(CAST (c AS DECIMAL(10,5))) from trunc_number order by c;

drop table trunc_number;
