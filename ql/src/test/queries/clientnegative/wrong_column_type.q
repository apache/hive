--! qt:dataset:src
CREATE TABLE dest1(a float);

INSERT OVERWRITE TABLE dest1
SELECT array(1.0,2.0) FROM src;
