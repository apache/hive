-- HIVE-30228: vectorized LEAD/LAG returned the wrong default value when the default
-- literal's type (e.g. STRING) differs from the window column's type (e.g. DATE),
-- silently falling back to epoch (1970-01-01) instead of the specified default.

CREATE TABLE vector_ptf_lead_lag_date(name string, rowindex int, mydate date) stored as orc;

INSERT INTO vector_ptf_lead_lag_date values
('first', 1, '2024-01-01'),
('first', 2, '2024-02-01'),
('first', 3, '2024-03-01'),
('second', 4, '2024-01-10'),
('second', 5, '2024-02-10'),
(NULL, 6, '2024-01-01');

select "************ DATE, STRING-LITERAL DEFAULT, NON-VECTORIZED ************";
set hive.vectorized.execution.ptf.enabled=false;
select name, rowindex, mydate,
lag(mydate, 1, '1999-01-01') over (partition by name order by mydate) as lag_default_literal,
lead(mydate, 1, '9999-01-01') over (partition by name order by mydate) as lead_default_literal,
lag(mydate, 1, mydate) over (partition by name order by mydate) as lag_default_col,
lead(mydate, 1, mydate) over (partition by name order by mydate) as lead_default_col
from vector_ptf_lead_lag_date;

set hive.vectorized.execution.ptf.enabled=true;
explain vectorization detail select name, rowindex, mydate,
lag(mydate, 1, '1999-01-01') over (partition by name order by mydate) as lag_default_literal,
lead(mydate, 1, '9999-01-01') over (partition by name order by mydate) as lead_default_literal,
lag(mydate, 1, mydate) over (partition by name order by mydate) as lag_default_col,
lead(mydate, 1, mydate) over (partition by name order by mydate) as lead_default_col
from vector_ptf_lead_lag_date;

select "************ DATE, STRING-LITERAL DEFAULT, VECTORIZED ************";
select name, rowindex, mydate,
lag(mydate, 1, '1999-01-01') over (partition by name order by mydate) as lag_default_literal,
lead(mydate, 1, '9999-01-01') over (partition by name order by mydate) as lead_default_literal,
lag(mydate, 1, mydate) over (partition by name order by mydate) as lag_default_col,
lead(mydate, 1, mydate) over (partition by name order by mydate) as lead_default_col
from vector_ptf_lead_lag_date;
