SET hive.vectorized.execution.enabled=true;
explain SELECT cbigint, cdouble FROM alltypesorc WHERE cbigint < cdouble and cint > 0 limit 7;
SELECT cbigint, cdouble FROM alltypesorc WHERE cbigint < cdouble and cint > 0 limit 7;
