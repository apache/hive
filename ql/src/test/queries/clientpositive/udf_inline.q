describe function inline;

explain SELECT inline( 
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

