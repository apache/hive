set hive.mapred.mode=nonstrict;
DROP TABLE covar_tab_n1;
CREATE TABLE covar_tab_n1 (a INT, b INT, c INT)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE;
LOAD DATA LOCAL INPATH '../../data/files/covar_tab.txt' OVERWRITE
INTO TABLE covar_tab_n1;

DESCRIBE FUNCTION covar_samp;
DESCRIBE FUNCTION EXTENDED covar_samp;
SELECT covar_samp(b, c) FROM covar_tab_n1 WHERE a < 1;
SELECT covar_samp(b, c) FROM covar_tab_n1 WHERE a < 3;
SELECT covar_samp(b, c) FROM covar_tab_n1 WHERE a = 3;
SELECT a, covar_samp(b, c) FROM covar_tab_n1 GROUP BY a ORDER BY a;
SELECT ROUND(covar_samp(b, c), 5) FROM covar_tab_n1;

DROP TABLE covar_tab_n1;
