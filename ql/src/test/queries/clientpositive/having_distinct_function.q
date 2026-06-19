create table t (col0 int, col1 int);

insert into t(col0, col1) values
(1, 1),
(1, 2), (1, 2), (1, 2),
(1, 3), (1, 3),
(2, 4),
(2, 5),
(3, 10)
;

explain cbo
select col0, count(distinct col1) from t
group by col0
having count(distinct col1) > 1;

select col0, count(distinct col1) from t
group by col0
having count(distinct col1) > 1;
