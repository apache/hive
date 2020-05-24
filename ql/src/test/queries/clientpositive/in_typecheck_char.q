
create table ax(s char(1),t char(10));

insert into ax values ('a','a'),('a','a '),('b','bb');

explain
select 'expected 2',count(*) from ax where s = 'a' and t = 'a';
select 'expected 2',count(*) from ax where s = 'a' and t = 'a';

explain
select 'expected 3',count(*) from ax where (s,t) in (('a','a'),('b','bb'));
select 'expected 3',count(*) from ax where (s,t) in (('a','a'),('b','bb'));

select 'expected 2',count(*) from ax where t = 'a         ';
select 'expected 2',count(*) from ax where t = 'a          ';
select 'expected 0',count(*) from ax where t = 'a          d';


select 'expected 2',count(*) from ax where (s,t) in (('a','a'),(null, 'bb'));


-- this is right now broken; HIVE-20779 should fix it
explain select 'expected 1',count(*) from ax where ((s,t) in (('a','a'),(null, 'bb'))) is null;
select 'expected 1',count(*) from ax where ((s,t) in (('a','a'),(null, 'bb'))) is null;

set hive.optimize.point.lookup=false;
explain cbo select count(*) from ax where t in
('a','bb','aa','bbb','ab','ba','aaa','bbb','abc','bc','ac','bca','cab','cb','ca','cbc','z');
explain select count(*) from ax where t in
('a','bb','aa','bbb','ab','ba','aaa','bbb','abc','bc','ac','bca','cab','cb','ca','cbc','z');

set hive.optimize.transform.in.maxnodes=20;
explain cbo select count(*) from ax where t in
('a','bb','aa','bbb','ab','ba','aaa','bbb','abc','bc','ac','bca','cab','cb','ca','cbc','z');
explain select count(*) from ax where t in
('a','bb','aa','bbb','ab','ba','aaa','bbb','abc','bc','ac','bca','cab','cb','ca','cbc','z');
