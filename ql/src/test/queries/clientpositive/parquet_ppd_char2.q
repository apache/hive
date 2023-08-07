SET hive.optimize.index.filter=true;
SET hive.optimize.ppd=true;
SET hive.optimize.ppd.storage=true;
SET hive.vectorized.execution.enabled=false;
SET hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
SET mapred.min.split.size=1000;
SET mapred.max.split.size=5000;
SET hive.parquet.timestamp.skip.conversion=false;

drop table if exists ppd_char_test;
create table ppd_char_test (id int, a char(10), b char(10), c varchar(10), d varchar(10)) stored as parquet;

insert into ppd_char_test values(1, 'apple', 'orange', 'lemon', 'apple'),
(2, 'almond', 'cherry',  'banana', 'apple'),
(3, 'banana', 'orange', 'banana  ', 'pear'),
(4, 'cherry', 'orange', 'banana', 'lemon'),
(5, 'lemon', 'orange', 'banana', 'apple'),
(6, 'orange', 'orange', 'banana', 'apple'),
(7, 'pear', 'orange', 'banana', 'apple'),
(8, 'pear', 'orange', 'lemon', 'apple  '),
(9, 'pear', 'orange', 'banana', 'pear'),
(10, 'pear', 'cherry', 'banana', 'apple'),
(11, 'pineapple', 'cherry', 'lemon', 'apple'),
(12, 'pineapple', 'cherry', '  lemon  ', 'apple'),
(13, 'pineapple', 'cherry', 'lemon    ', 'apple'),
(14, 'pineapple', 'cherry', '   lemon   ', 'apple');

select * from ppd_char_test where ((a='pear' or a<='cherry') and (b='orange')) and (c='banana' or d<'cherry') order by id;

select * from ppd_char_test where ((a='pear' or a<='cherry') and (b='orange')) and c='banana  ' order by id;

select id, a from ppd_char_test where a='apple';

select id, a from ppd_char_test where a!='apple';

select id, a from ppd_char_test where a<'cherry';

select id, a from ppd_char_test where a<='cherry';

select id, a from ppd_char_test where a>'cherry';

select id, a from ppd_char_test where a>='cherry';

select id, c from ppd_char_test where a='pineapple' and c='lemon';

select id, c from ppd_char_test where a='pineapple' and c!='lemon';

select id, c from ppd_char_test where a='pineapple' and c!='  lemon  ';

select id, c from ppd_char_test where a='pineapple' and c='  lemon';

select id, c from ppd_char_test where a='pineapple' and c='lemon  ';

select id, c from ppd_char_test where a='pineapple' and c='lemon   ';

select id, c from ppd_char_test where a='pineapple' and c='lemon    ';

select id, c from ppd_char_test where a='pineapple' and c='   lemon  ';

select id, c from ppd_char_test where a='pineapple' and c<'lemon';

select id, c from ppd_char_test where a='pineapple' and c<='lemon';

select id, c from ppd_char_test where a='pineapple' and c>'lemon';

select id, c from ppd_char_test where a='pineapple' and c>='lemon';

select id, c from ppd_char_test where a='pineapple' and c<='lemon    ';

select id, c from ppd_char_test where a='pineapple' and c>'lemon ';

SET hive.optimize.index.filter=false;
SET hive.optimize.ppd=false;
SET hive.optimize.ppd.storage=false;

select * from ppd_char_test where ((a='pear' or a<='cherry') and (b='orange')) and (c='banana' or d<'cherry') order by id;

select * from ppd_char_test where ((a='pear' or a<='cherry') and (b='orange')) and c='banana  ' order by id;

select id, a from ppd_char_test where a='apple';

select id, a from ppd_char_test where a!='apple';

select id, a from ppd_char_test where a<'cherry';

select id, a from ppd_char_test where a<='cherry';

select id, a from ppd_char_test where a>'cherry';

select id, a from ppd_char_test where a>='cherry';

select id, c from ppd_char_test where a='pineapple' and c='lemon';

select id, c from ppd_char_test where a='pineapple' and c!='lemon';

select id, c from ppd_char_test where a='pineapple' and c!='  lemon  ';

select id, c from ppd_char_test where a='pineapple' and c='  lemon';

select id, c from ppd_char_test where a='pineapple' and c='lemon  ';

select id, c from ppd_char_test where a='pineapple' and c='lemon   ';

select id, c from ppd_char_test where a='pineapple' and c='lemon    ';

select id, c from ppd_char_test where a='pineapple' and c='   lemon  ';

select id, c from ppd_char_test where a='pineapple' and c<'lemon';

select id, c from ppd_char_test where a='pineapple' and c<='lemon';

select id, c from ppd_char_test where a='pineapple' and c>'lemon';

select id, c from ppd_char_test where a='pineapple' and c>='lemon';

select id, c from ppd_char_test where a='pineapple' and c<='lemon    ';

select id, c from ppd_char_test where a='pineapple' and c>'lemon ';

drop table ppd_char_test;
