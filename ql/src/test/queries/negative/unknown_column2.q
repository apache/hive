--! qt:dataset:src
FROM src
INSERT OVERWRITE TABLE dest1 SELECT '1234', src.value WHERE src.dummykey < 100
