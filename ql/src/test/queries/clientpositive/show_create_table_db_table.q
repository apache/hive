-- Test SHOW CREATE TABLE on a table name of format "db.table".

CREATE DATABASE tmp_feng comment 'for show create table test';
SHOW DATABASES;
CREATE TABLE tmp_feng.tmp_showcrt1(key string, value int);
CREATE TABLE tmp_feng.tmp_showcrt2(key string, value int) skewed by (key) on ('1','2');
CREATE TABLE tmp_feng.tmp_showcrt3(key string, value int) skewed by (key) on ('1','2') stored as directories;
USE default;
SHOW CREATE TABLE tmp_feng.tmp_showcrt1;
SHOW CREATE TABLE tmp_feng.tmp_showcrt2;
SHOW CREATE TABLE tmp_feng.tmp_showcrt3;
DROP TABLE tmp_feng.tmp_showcrt1;
DROP TABLE tmp_feng.tmp_showcrt2;
DROP TABLE tmp_feng.tmp_showcrt3;
DROP DATABASE tmp_feng;

