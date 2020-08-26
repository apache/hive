--! qt:dataset:src
set hive.fetch.task.conversion=none;
SET hive.vectorized.execution.enabled=true;

-- HIVE-19024: Vectorization: Disable complex type constants for VectorUDFAdaptor

describe function inline;

explain vectorization expression
SELECT inline( 
  ARRAY(
    STRUCT (1,'dude!'),
    STRUCT (2,'Wheres'),
    STRUCT (3,'my car?')
  )
)  as (id, text) FROM SRC limit 2;

SELECT inline( 
  ARRAY(
    STRUCT (1,'dude!'),
    STRUCT (2,'Wheres'),
    STRUCT (3,'my car?')
  )
)  as (id, text) FROM SRC limit 2;

-- HIVE-3475 INLINE UDTF doesn't convert types properly
select * from (SELECT
  ARRAY(
    STRUCT (1,'dude!'),
    STRUCT (2,'Wheres'),
    STRUCT (3,'my car?')
  ) as value FROM SRC limit 1) input
 LATERAL VIEW inline(value) myTable AS id, text;
