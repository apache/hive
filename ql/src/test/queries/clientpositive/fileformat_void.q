EXPLAIN
CREATE TABLE dest1(key INT, value STRING) STORED AS
  INPUTFORMAT 'java.lang.Void'
  OUTPUTFORMAT 'java.lang.Void';

CREATE TABLE dest1(key INT, value STRING) STORED AS
  INPUTFORMAT 'java.lang.Void'
  OUTPUTFORMAT 'java.lang.Void';

DESCRIBE EXTENDED dest1;

DROP TABLE dest1;
