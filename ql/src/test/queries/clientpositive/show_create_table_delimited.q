-- Test SHOW CREATE TABLE on a table with delimiters, stored format, and location.

CREATE TABLE tmp_showcrt1 (key int, value string, newvalue bigint)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' COLLECTION ITEMS TERMINATED BY '|' MAP KEYS TERMINATED BY '\045' LINES TERMINATED BY '\n'
STORED AS textfile
LOCATION 'file:${system:test.tmp.dir}/tmp_showcrt1';
SHOW CREATE TABLE tmp_showcrt1;
DROP TABLE tmp_showcrt1;

create table esc_special_delimiter(age int, name string) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\u0001' stored as textfile;
show create table esc_special_delimiter;
drop table esc_special_delimiter;

create table esc_special_delimiter(age int, name string) ROW FORMAT DELIMITED FIELDS TERMINATED BY '`' stored as textfile;
show create table esc_special_delimiter;
drop table esc_special_delimiter;

create table esc_special_delimiter(age int, name string) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\'' stored as textfile;
show create table esc_special_delimiter;
drop table esc_special_delimiter;

create table esc_special_delimiter(age int, name string) ROW FORMAT DELIMITED FIELDS TERMINATED BY ';' stored as textfile;
show create table esc_special_delimiter;
drop table esc_special_delimiter;
