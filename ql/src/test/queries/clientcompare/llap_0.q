SET hive.vectorized.execution.enabled=true;
set hive.mapred.mode=nonstrict;
SELECT   cfloat,
         cint,
         cdouble,
         cbigint,
         cstring1
FROM     alltypesorc
WHERE    (cbigint > -23)
           AND ((cdouble != 988888)
                OR (cint > -863.257))
ORDER BY cbigint, cfloat;
