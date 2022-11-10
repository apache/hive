CREATE TABLE event (username string, action string, amount int);

INSERT INTO event VALUES
('john', 'sell', 20),
('john', 'sell', 3),
('john', 'buy', 25),
('john', 'buy', 39),
('john', 'buy', null);

-- hive.default.nulls.last is true by default, it sets NULLS_FIRST for DESC
EXPLAIN AST
SELECT username, action, amount, row_number() OVER (PARTITION BY username, action ORDER BY action DESC, amount DESC)
FROM event;

EXPLAIN
SELECT username, action, amount, row_number() OVER (PARTITION BY username, action ORDER BY action DESC, amount DESC)
FROM event;

SELECT username, action, amount, row_number() OVER (PARTITION BY username, action ORDER BY action DESC, amount DESC)
FROM event;

-- we set hive.default.nulls.last=false, it sets NULLS_LAST for DESC
set hive.default.nulls.last=false;

EXPLAIN AST
SELECT username, action, amount, row_number() OVER (PARTITION BY username, action ORDER BY action DESC, amount DESC)
FROM event;

EXPLAIN
SELECT username, action, amount, row_number() OVER (PARTITION BY username, action ORDER BY action DESC, amount DESC)
FROM event;

SELECT username, action, amount, row_number() OVER (PARTITION BY username, action ORDER BY action DESC, amount DESC)
FROM event;

-- we set hive.default.nulls.last=false but we have explicit NULLS_LAST, we expect NULLS_LAST
set hive.default.nulls.last=false;

EXPLAIN AST
SELECT username, action, amount, row_number() OVER (PARTITION BY username, action ORDER BY action DESC, amount DESC NULLS LAST)
FROM event;

EXPLAIN
SELECT username, action, amount, row_number() OVER (PARTITION BY username, action ORDER BY action DESC, amount DESC NULLS LAST)
FROM event;

SELECT username, action, amount, row_number() OVER (PARTITION BY username, action ORDER BY action DESC, amount DESC NULLS LAST)
FROM event;

-- we have explicit NULLS_FIRST, we expect NULLS_FIRST
EXPLAIN AST
SELECT username, action, amount, row_number() OVER (PARTITION BY username, action ORDER BY action DESC, amount DESC NULLS FIRST)
FROM event;

EXPLAIN
SELECT username, action, amount, row_number() OVER (PARTITION BY username, action ORDER BY action DESC, amount DESC NULLS FIRST)
FROM event;

SELECT username, action, amount, row_number() OVER (PARTITION BY username, action ORDER BY action DESC, amount DESC NULLS FIRST)
FROM event;
