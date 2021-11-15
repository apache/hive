CREATE PACKAGE Counter AS
  count INT := 0;
  FUNCTION current() RETURNS INT;
  PROCEDURE inc(i INT);
END;

CREATE PACKAGE BODY Counter AS
  FUNCTION current() RETURNS INT IS BEGIN RETURN count; END;
  PROCEDURE inc(i INT) IS BEGIN count := count + i; END;
END;


Counter.inc(10);

PRINT Counter.current();

DROP PACKAGE Counter;


DROP PACKAGE IF EXISTS Counter;