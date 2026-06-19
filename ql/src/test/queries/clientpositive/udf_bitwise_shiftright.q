DESCRIBE FUNCTION shiftright;
DESC FUNCTION EXTENDED shiftright;

explain select shiftright(4, 1);

select
shiftright(a, 0),
shiftright(a, 1),
shiftright(a, 2),
shiftright(a, 3),
shiftright(a, 4),
shiftright(a, 5),
shiftright(a, 6),
shiftright(a, 31),
shiftright(a, 32)
from (
  select cast(-128 as tinyint) a
) t;

select
shiftright(a, 0),
shiftright(a, 1),
shiftright(a, 2),
shiftright(a, 8),
shiftright(a, 9),
shiftright(a, 10),
shiftright(a, 11),
shiftright(a, 12),
shiftright(a, 13),
shiftright(a, 14),
shiftright(a, 31),
shiftright(a, 32)
from (
  select cast(-32768 as smallint) a
) t;

select
shiftright(a, 0),
shiftright(a, 1),
shiftright(a, 2),
shiftright(a, 24),
shiftright(a, 25),
shiftright(a, 26),
shiftright(a, 27),
shiftright(a, 28),
shiftright(a, 29),
shiftright(a, 30),
shiftright(a, 31),
shiftright(a, 32)
from (
  select cast(-2147483648 as int) a
) t;

select
shiftright(a, 0),
shiftright(a, 1),
shiftright(a, 2),
shiftright(a, 56),
shiftright(a, 57),
shiftright(a, 58),
shiftright(a, 59),
shiftright(a, 60),
shiftright(a, 61),
shiftright(a, 62),
shiftright(a, 63),
shiftright(a, 64)
from (
  select cast(-9223372036854775808 as bigint) a
) t;

select
shiftright(1024, 33),
shiftright(1024, 65),
shiftright(1024, 4001),
shiftright(1024, -2),
shiftright(1024, cast(null as int)),
shiftright(cast(null as int), 4),
shiftright(cast(null as int), cast(null as int));