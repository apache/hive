--! qt:dataset:src
FROM dummySrc
INSERT OVERWRITE TABLE dest1 SELECT '1234', src.value WHERE src.key < 100
