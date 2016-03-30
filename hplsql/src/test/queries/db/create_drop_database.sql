create database 'test' || replace('2016-03-03', '-', '');
create database if not exists test1 comment 'abc' location '/users';

drop database if exists 'test' || replace('2016-03-03', '-', '');
drop database test1;
