declare v_float float;
declare v_double double;
declare v_double2 double precision;

select
  cast(1.1 as float),
  cast(1.1 as double),
  cast(1.1 as double)  
into
  v_float,
  v_double,
  v_double2
from src limit 1;
        
print 'float: ' || v_float;
print 'double: ' || v_double;
print 'double precision: ' || v_double2;
