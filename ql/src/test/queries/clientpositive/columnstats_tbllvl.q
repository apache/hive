
DROP TABLE IF EXISTS UserVisits_web_text_none;

CREATE TABLE UserVisits_web_text_none (
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

explain 
analyze table UserVisits_web_text_none compute statistics for columns sourceIP, avgTimeOnSite, adRevenue;

explain extended
analyze table UserVisits_web_text_none compute statistics for columns sourceIP, avgTimeOnSite, adRevenue;

analyze table UserVisits_web_text_none compute statistics for columns sourceIP, avgTimeOnSite, adRevenue;

explain 
analyze table default.UserVisits_web_text_none compute statistics for columns;

analyze table default.UserVisits_web_text_none compute statistics for columns;

describe formatted UserVisits_web_text_none destURL;
describe formatted UserVisits_web_text_none adRevenue;
describe formatted UserVisits_web_text_none avgTimeOnSite;
 
CREATE TABLE empty_tab(
   a int,
   b double,
   c string, 
   d boolean,
   e binary)
row format delimited fields terminated by '|'  stored as textfile;

explain 
analyze table empty_tab compute statistics for columns a,b,c,d,e;

analyze table empty_tab compute statistics for columns a,b,c,d,e;

create database if not exists dummydb;

use dummydb;

analyze table default.UserVisits_web_text_none compute statistics for columns destURL;

describe formatted default.UserVisits_web_text_none destURL;

CREATE TABLE UserVisits_in_dummy_db (
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

LOAD DATA LOCAL INPATH "../../data/files/UserVisits.dat" INTO TABLE UserVisits_in_dummy_db;

use default;

explain 
analyze table dummydb.UserVisits_in_dummy_db compute statistics for columns sourceIP, avgTimeOnSite, adRevenue;

explain extended
analyze table dummydb.UserVisits_in_dummy_db compute statistics for columns sourceIP, avgTimeOnSite, adRevenue;

analyze table dummydb.UserVisits_in_dummy_db compute statistics for columns sourceIP, avgTimeOnSite, adRevenue;

explain 
analyze table dummydb.UserVisits_in_dummy_db compute statistics for columns;

analyze table dummydb.UserVisits_in_dummy_db compute statistics for columns;

describe formatted dummydb.UserVisits_in_dummy_db destURL;
describe formatted dummydb.UserVisits_in_dummy_db adRevenue;
describe formatted dummydb.UserVisits_in_dummy_db avgTimeOnSite;

drop table dummydb.UserVisits_in_dummy_db;

drop database dummydb;





