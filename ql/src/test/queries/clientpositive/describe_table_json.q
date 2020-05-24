set hive.ddl.output.format=json;

CREATE TABLE IF NOT EXISTS jsontable (key INT, value STRING) COMMENT 'json table' STORED AS TEXTFILE;

SHOW TABLES LIKE 'jsontab%';

SHOW TABLE EXTENDED LIKE 'jsontab%';

ALTER TABLE jsontable SET TBLPROPERTIES ('id' = 'jsontable');

DESCRIBE jsontable;

DESCRIBE extended jsontable;

DROP TABLE jsontable;

set hive.ddl.output.format=text;
