-- Based on display_colstats_tbllvl.q, output should be almost exactly the same.
DROP TABLE IF EXISTS UserVisits_web_text_none;

-- Hack, set external location because generated filename changes during test runs
CREATE TEMPORARY EXTERNAL TABLE UserVisits_web_text_none (
  sourceIP string,
  destURL string,
  visitDate string,
  adRevenue float,
  userAgent string,
  cCode string,
  lCode string,
  sKeyword string,
  avgTimeOnSite int)
row format delimited fields terminated by '|'  stored as textfile
location 'pfile://${system:test.tmp.dir}/uservisits_web_text_none';

LOAD DATA LOCAL INPATH "../../data/files/UserVisits.dat" INTO TABLE UserVisits_web_text_none;

desc extended UserVisits_web_text_none sourceIP;
desc formatted UserVisits_web_text_none sourceIP;

explain
analyze table UserVisits_web_text_none compute statistics for columns sourceIP, avgTimeOnSite, adRevenue;

explain extended
analyze table UserVisits_web_text_none compute statistics for columns sourceIP, avgTimeOnSite, adRevenue;

analyze table UserVisits_web_text_none compute statistics for columns sourceIP, avgTimeOnSite, adRevenue;
desc formatted UserVisits_web_text_none sourceIP;
desc formatted UserVisits_web_text_none avgTimeOnSite;
desc formatted UserVisits_web_text_none adRevenue;

CREATE TEMPORARY TABLE empty_tab(
   a int,
   b double,
   c string,
   d boolean,
   e binary)
row format delimited fields terminated by '|'  stored as textfile;

desc formatted empty_tab a;
explain
analyze table empty_tab compute statistics for columns a,b,c,d,e;

analyze table empty_tab compute statistics for columns a,b,c,d,e;
desc formatted empty_tab a;
desc formatted empty_tab b;

CREATE DATABASE test;
USE test;

CREATE TEMPORARY TABLE UserVisits_web_text_none (
  sourceIP string,
  destURL string,
  visitDate string,
  adRevenue float,
  userAgent string,
  cCode string,
  lCode string,
  sKeyword string,
  avgTimeOnSite int)
row format delimited fields terminated by '|'  stored as textfile;

LOAD DATA LOCAL INPATH "../../data/files/UserVisits.dat" INTO TABLE UserVisits_web_text_none;

desc extended UserVisits_web_text_none sourceIP;
desc extended test.UserVisits_web_text_none sourceIP;
desc extended default.UserVisits_web_text_none sourceIP;
desc formatted UserVisits_web_text_none sourceIP;
desc formatted test.UserVisits_web_text_none sourceIP;
desc formatted default.UserVisits_web_text_none sourceIP;

analyze table UserVisits_web_text_none compute statistics for columns sKeyword;
desc extended UserVisits_web_text_none sKeyword;
desc formatted UserVisits_web_text_none sKeyword;
desc formatted test.UserVisits_web_text_none sKeyword;

