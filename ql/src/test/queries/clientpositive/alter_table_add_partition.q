create table mp (a int) partitioned by (b int);

desc formatted mp;

alter table mp add partition (b=1);

desc formatted mp;
desc formatted mp partition (b=1);

insert into mp partition (b=1) values (1);

desc formatted mp;
desc formatted mp partition (b=1);
