-- SORT_QUERY_RESULTS

drop table char_join1_ch1;
drop table char_join1_ch2;
drop table char_join1_str_n0;

create table  char_join1_ch1 (
  c1 int,
  c2 char(10)
);

create table  char_join1_ch2 (
  c1 int,
  c2 char(20)
);

create table  char_join1_str_n0 (
  c1 int,
  c2 string
);

load data local inpath '../../data/files/vc1.txt' into table char_join1_ch1;
load data local inpath '../../data/files/vc1.txt' into table char_join1_ch2;
load data local inpath '../../data/files/vc1.txt' into table char_join1_str_n0;

-- Join char with same length char
select * from char_join1_ch1 a join char_join1_ch1 b on (a.c2 = b.c2);

-- Join char with different length char
select * from char_join1_ch1 a join char_join1_ch2 b on (a.c2 = b.c2);

-- Join char with string
select * from char_join1_ch1 a join char_join1_str_n0 b on (a.c2 = b.c2);

drop table char_join1_ch1;
drop table char_join1_ch2;
drop table char_join1_str_n0;
