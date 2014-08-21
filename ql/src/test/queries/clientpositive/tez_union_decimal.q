select sum(a) from (
  select cast(1.1 as decimal) a from src tablesample (1 rows)
  union all
  select cast(null as decimal) a from src tablesample (1 rows)
) t;

select sum(a) from (
  select cast(1 as tinyint) a from src tablesample (1 rows)
  union all
  select cast(null as tinyint) a from src tablesample (1 rows)
  union all
  select cast(1.1 as decimal) a from src tablesample (1 rows)
) t;

select sum(a) from (
  select cast(1 as smallint) a from src tablesample (1 rows)
  union all
  select cast(null as smallint) a from src tablesample (1 rows)
  union all
  select cast(1.1 as decimal) a from src tablesample (1 rows)
) t;

select sum(a) from (
  select cast(1 as int) a from src tablesample (1 rows)
  union all
  select cast(null as int) a from src tablesample (1 rows)
  union all
  select cast(1.1 as decimal) a from src tablesample (1 rows)
) t;

select sum(a) from (
  select cast(1 as bigint) a from src tablesample (1 rows)
  union all
  select cast(null as bigint) a from src tablesample (1 rows)
  union all
  select cast(1.1 as decimal) a from src tablesample (1 rows)
) t;
