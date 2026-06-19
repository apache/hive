DROP TABLE IF EXISTS UserVisits_web_text_none_n0;

CREATE TABLE UserVisits_web_text_none_n0 (
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

LOAD DATA LOCAL INPATH "../../data/files/UserVisits.dat" INTO TABLE UserVisits_web_text_none_n0;

desc extended UserVisits_web_text_none_n0 sourceIP;
desc formatted UserVisits_web_text_none_n0 sourceIP;

explain
analyze table UserVisits_web_text_none_n0 compute statistics for columns sourceIP, avgTimeOnSite, adRevenue;

explain extended
analyze table UserVisits_web_text_none_n0 compute statistics for columns sourceIP, avgTimeOnSite, adRevenue;

analyze table UserVisits_web_text_none_n0 compute statistics for columns sourceIP, avgTimeOnSite, adRevenue;
desc formatted UserVisits_web_text_none_n0 sourceIP;
desc formatted UserVisits_web_text_none_n0 avgTimeOnSite;
desc formatted UserVisits_web_text_none_n0 adRevenue;

CREATE TABLE empty_tab_n0(
   a int,
   b double,
   c string,
   d boolean,
   e binary)
row format delimited fields terminated by '|'  stored as textfile;

desc formatted empty_tab_n0 a;
explain
analyze table empty_tab_n0 compute statistics for columns a,b,c,d,e;

analyze table empty_tab_n0 compute statistics for columns a,b,c,d,e;
desc formatted empty_tab_n0 a;
desc formatted empty_tab_n0 b;

CREATE DATABASE test;
USE test;

CREATE TABLE UserVisits_web_text_none_n0 (
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

LOAD DATA LOCAL INPATH "../../data/files/UserVisits.dat" INTO TABLE UserVisits_web_text_none_n0;

desc extended UserVisits_web_text_none_n0 sourceIP;
desc extended test.UserVisits_web_text_none_n0 sourceIP;
desc extended default.UserVisits_web_text_none_n0 sourceIP;
desc formatted UserVisits_web_text_none_n0 sourceIP;
desc formatted test.UserVisits_web_text_none_n0 sourceIP;
desc formatted default.UserVisits_web_text_none_n0 sourceIP;

analyze table UserVisits_web_text_none_n0 compute statistics for columns sKeyword;
desc extended UserVisits_web_text_none_n0 sKeyword;
desc formatted UserVisits_web_text_none_n0 sKeyword;
desc formatted test.UserVisits_web_text_none_n0 sKeyword;

