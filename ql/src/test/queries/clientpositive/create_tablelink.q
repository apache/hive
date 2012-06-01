DROP DATABASE IF EXISTS tmpdb CASCADE;
CREATE DATABASE tmpdb;
USE tmpdb;

CREATE TABLELINK TO src@default LINKPROPERTIES('RETENTION'='7');
DESC src@default;
DESC EXTENDED src@default;
DESC FORMATTED src@default;

CREATE STATIC TABLELINK TO srcbucket@default;
DESC srcbucket@default;
DESC EXTENDED srcbucket@default;
DESC FORMATTED srcbucket@default;

SHOW TABLES;

DROP TABLELINK src@default;
DROP TABLELINK srcbucket@default;
