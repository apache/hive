DESCRIBE FUNCTION shiftrightunsigned;
DESC FUNCTION EXTENDED shiftrightunsigned;

explain select shiftrightunsigned(4, 1);

select
shiftrightunsigned(a, 0),
shiftrightunsigned(a, 1),
shiftrightunsigned(a, 2),
shiftrightunsigned(a, 31),
shiftrightunsigned(a, 32)
from (
  select cast(-128 as tinyint) a
) t;

select
shiftrightunsigned(a, 0),
shiftrightunsigned(a, 1),
shiftrightunsigned(a, 2),
shiftrightunsigned(a, 31),
shiftrightunsigned(a, 32)
from (
  select cast(-32768 as smallint) a
) t;

select
shiftrightunsigned(a, 0),
shiftrightunsigned(a, 1),
shiftrightunsigned(a, 2),
shiftrightunsigned(a, 31),
shiftrightunsigned(a, 32)
from (
  select cast(-2147483648 as int) a
) t;

select
shiftrightunsigned(a, 0),
shiftrightunsigned(a, 1),
shiftrightunsigned(a, 2),
shiftrightunsigned(a, 63),
shiftrightunsigned(a, 64)
from (
  select cast(-9223372036854775808 as bigint) a
) t;

select
shiftrightunsigned(1024, 33),
shiftrightunsigned(1024, 65),
shiftrightunsigned(1024, 4001),
shiftrightunsigned(1024, -2),
shiftrightunsigned(1024, cast(null as int)),
shiftrightunsigned(cast(null as int), 4),
shiftrightunsigned(cast(null as int), cast(null as int));