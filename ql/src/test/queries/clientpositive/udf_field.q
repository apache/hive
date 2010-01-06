DESCRIBE FUNCTION field;
DESCRIBE FUNCTION EXTENDED field;

SELECT
  field("x", "a", "b", "c", "d"),
  field(NULL, "a", "b", "c", "d"),
  field(0, 1, 2, 3, 4)
FROM src LIMIT 1;

SELECT
  field("a", "a", "b", "c", "d"),
  field("b", "a", "b", "c", "d"),
  field("c", "a", "b", "c", "d"),
  field("d", "a", "b", "c", "d"),
  field("d", "a", "b", NULL, "d")
FROM src LIMIT 1;

SELECT
  field(1, 1, 2, 3, 4),
  field(2, 1, 2, 3, 4),
  field(3, 1, 2, 3, 4),
  field(4, 1, 2, 3, 4),
  field(4, 1, 2, NULL, 4)
FROM src LIMIT 1;
