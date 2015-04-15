DESCRIBE FUNCTION shiftleft;
DESC FUNCTION EXTENDED shiftleft;

explain select shiftleft(4, 1);

select
shiftleft(a, 0),
shiftleft(a, 1),
shiftleft(a, 2),
shiftleft(a, 3),
shiftleft(a, 4),
shiftleft(a, 5),
shiftleft(a, 6),
shiftleft(a, 7),
shiftleft(a, 8),
shiftleft(a, 13),
shiftleft(a, 14),
shiftleft(a, 29),
shiftleft(a, 30),
shiftleft(a, 61),
shiftleft(a, 62)
from (
  select cast(4 as tinyint) a
) t;

select
shiftleft(a, 0),
shiftleft(a, 1),
shiftleft(a, 2),
shiftleft(a, 3),
shiftleft(a, 4),
shiftleft(a, 5),
shiftleft(a, 6),
shiftleft(a, 7),
shiftleft(a, 8),
shiftleft(a, 13),
shiftleft(a, 14),
shiftleft(a, 29),
shiftleft(a, 30),
shiftleft(a, 61),
shiftleft(a, 62)
from (
  select cast(4 as smallint) a
) t;

select
shiftleft(a, 0),
shiftleft(a, 1),
shiftleft(a, 2),
shiftleft(a, 3),
shiftleft(a, 4),
shiftleft(a, 5),
shiftleft(a, 6),
shiftleft(a, 7),
shiftleft(a, 8),
shiftleft(a, 13),
shiftleft(a, 14),
shiftleft(a, 29),
shiftleft(a, 30),
shiftleft(a, 61),
shiftleft(a, 62)
from (
  select cast(4 as int) a
) t;

select
shiftleft(a, 0),
shiftleft(a, 1),
shiftleft(a, 2),
shiftleft(a, 3),
shiftleft(a, 4),
shiftleft(a, 5),
shiftleft(a, 6),
shiftleft(a, 7),
shiftleft(a, 8),
shiftleft(a, 13),
shiftleft(a, 14),
shiftleft(a, 29),
shiftleft(a, 30),
shiftleft(a, 61),
shiftleft(a, 62)
from (
  select cast(4 as bigint) a
) t;

select
shiftleft(4, 33),
shiftleft(4, 65),
shiftleft(4, 4001),
shiftleft(16, -2),
shiftleft(4, cast(null as int)),
shiftleft(cast(null as int), 4),
shiftleft(cast(null as int), cast(null as int));