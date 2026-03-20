CREATE DATABASE `@firstdb`;
CREATE TABLE `@firstdb`.testtable (c1 INT);
ALTER TABLE `@firstdb`.testtable ADD COLUMNS (c2 INT);
DROP TABLE `@firstdb`.testtable;
DROP DATABASE `@firstdb`;
