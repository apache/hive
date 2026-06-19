set hive.stats.autogather=true;

CREATE TABLE p1(i int) partitioned by (p string);

insert into p1 partition(p='a') values (1);
insert into p1 partition(p='A') values (2),(3);

describe formatted p1;
describe formatted p1 partition(p='a');
describe formatted p1 partition(p='A');


