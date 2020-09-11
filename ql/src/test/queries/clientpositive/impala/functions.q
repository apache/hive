--! qt:dataset:impala_dataset
-- grabbed from QE tableau tests and changed to use no table
explain SELECT  (1 + IF(7<0, PMOD((IF(7<0, PMOD(DATEDIFF(TO_DATE('1996-01-01'), '1995-01-01'),7)-7, PMOD(DATEDIFF(TO_DATE('1996-01-01'), '1995-01-01'), 7)) + 7),7)-7, PMOD((IF(7<0, PMOD(DATEDIFF(TO_DATE('1996-01-01'), '1995-01-01'),7)-7, PMOD(DATEDIFF(TO_DATE('1996-01-01'), '1995-01-01'), 7)) + 7), 7))) AS `sum_z_datepart_weekday_ok`;
