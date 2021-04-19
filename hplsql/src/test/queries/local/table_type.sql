TYPE T_TABLE IS TABLE OF STRING INDEX BY BINARY_INTEGER;

CREATE FUNCTION iterate_forward(tbl T_TABLE) RETURNS STRING
BEGIN
  DECLARE s STRING = '';
  DECLARE idx INT = tbl.FIRST;
  WHILE idx IS NOT NULL LOOP
    s = s || tbl(idx);
    idx = tbl.NEXT(idx);
  END LOOP;
  RETURN s;
END;

CREATE FUNCTION iterate_backward(tbl T_TABLE) RETURNS STRING
BEGIN
  DECLARE s STRING = '';
  DECLARE idx INT = tbl.LAST;
  WHILE idx IS NOT NULL LOOP
    s = s || tbl(idx);
    idx = tbl.PRIOR(idx);
  END LOOP;
  RETURN s;
END;

CREATE FUNCTION init() RETURNS T_TABLE
BEGIN
  DECLARE t T_TABLE;
  t(1) := 'A';
  t(2) := 'B';
  t(3) := 'C';
  t(4) := 'D';
  t(5) := 'E';
  t(6) := 'F';
  t(7) := 'G';
  RETURN t;
END;

DECLARE tbl T_TABLE = init();

PRINT 'C=' || tbl.COUNT;

PRINT 'S1=' || iterate_forward(tbl);

tbl(2) := 'X';
tbl(3) := 'Y';
PRINT tbl.EXISTS(4);
tbl.DELETE(4);
PRINT tbl.EXISTS(4);
tbl.DELETE(6);

PRINT 'S2=' || iterate_forward(tbl);
PRINT 'S3=' || iterate_backward(tbl);

-- at this point: AXYEG

tbl.DELETE(1, 3);
PRINT 'S4=' || iterate_forward(tbl);

tbl.DELETE;
PRINT 'S5=' || iterate_forward(tbl);
PRINT 'C=' || tbl.COUNT();