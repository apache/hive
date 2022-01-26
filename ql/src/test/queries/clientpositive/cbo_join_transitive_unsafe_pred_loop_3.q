CREATE TABLE tableA
(
    `bd_id`            bigint,
    `quota_type`       string
);

explain cbo
select a.bd_id
from (
    select t.bd_id
    from tableA t
    where (t.bd_id = 8 and t.quota_type in('A','C')) or (t.bd_id = 9 and t.quota_type in ('A','B'))
 ) a join (
     select t.bd_id
     from tableA t
     where t.bd_id = 9 and t.quota_type in ('A','B')
     union all
     select t.bd_id
     from tableA t
     where (t.bd_id = 8 and t.quota_type in('A','C')) or (t.bd_id = 9 and t.quota_type in ('A','B'))
) b on a.bd_id = b.bd_id
where a.bd_id = 8 or a.bd_id <>8;
