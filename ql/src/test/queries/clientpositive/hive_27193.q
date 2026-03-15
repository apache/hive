CREATE DATABASE `@test`;
USE `@test`;
CREATE TABLE testtable (c1 INT);
ALTER TABLE testtable ADD COLUMNS (c2 INT);
DROP TABLE testtable;
DROP DATABASE `@test`;
