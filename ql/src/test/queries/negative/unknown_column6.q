--! qt:dataset:src
FROM src
INSERT OVERWRITE TABLE dest1 SELECT '1234', dummysrc.value WHERE src.key < 100 group by src.key
