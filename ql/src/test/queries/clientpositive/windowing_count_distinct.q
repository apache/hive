create temporary table tmp_tbl(
`rule_id` string,
`severity` string,
`alert_id` string,
`alert_type` string);

insert into tmp_tbl values
('a', 'a', 'a', 'a'),
('a', 'b', 'a', 'b'),
('c', 'a', 'a', 'a'),
('c', 'b', 'a', 'b');

explain cbo
select `k`.`rule_id`,
count(distinct `k`.`alert_id`) over(partition by `k`.`rule_id`) `subj_cnt`
from tmp_tbl k
;


explain
select `k`.`rule_id`,
count(distinct `k`.`alert_id`) over(partition by `k`.`rule_id`) `subj_cnt`
from tmp_tbl k
;

select `k`.`rule_id`,
count(distinct `k`.`alert_id`) over(partition by `k`.`rule_id`) `subj_cnt`
,count(`k`.`alert_id`) over(partition by `k`.`rule_id`)
from tmp_tbl k
;
