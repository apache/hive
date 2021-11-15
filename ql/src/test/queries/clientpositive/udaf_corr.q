--! qt:disabled:disabled in HIVE-20741

set hive.mapred.mode=nonstrict;
DROP TABLE covar_tab_n0;
CREATE TABLE covar_tab_n0 (a INT, b INT, c INT)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE;
LOAD DATA LOCAL INPATH '../../data/files/covar_tab.txt' OVERWRITE
INTO TABLE covar_tab_n0;

DESCRIBE FUNCTION corr;
DESCRIBE FUNCTION EXTENDED corr;
SELECT corr(b, c) FROM covar_tab_n0 WHERE a < 1;
SELECT corr(b, c) FROM covar_tab_n0 WHERE a < 3;
SELECT corr(b, c) FROM covar_tab_n0 WHERE a = 3;
SELECT a, corr(b, c) FROM covar_tab_n0 GROUP BY a ORDER BY a;
SELECT corr(b, c) FROM covar_tab_n0;

DROP TABLE covar_tab_n0;
