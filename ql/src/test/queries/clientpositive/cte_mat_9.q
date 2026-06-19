set hive.optimize.cte.materialize.threshold=2;
set hive.optimize.cte.materialize.full.aggregate.only=false;

drop table if exists cte_mat_9_a;
create table cte_mat_9_a (id int);
insert into cte_mat_9_a (id) values (1);

drop table if exists cte_mat_9_b;
create table cte_mat_9_b (id int);
insert into cte_mat_9_b (id) values (1);

explain with a0 AS (
  select id, 'a0' as tag from cte_mat_9_a
),
a1 as (
  select id, 'a1 <- ' || tag as tag from a0
),
b0 as (
  select id, 'b0' as tag from cte_mat_9_b
),
b1 as (
  select id, 'b1 <- ' || tag as tag from b0
),
b2 as (
  select id, 'b2 <- ' || tag as tag  from b1
),
b3 as (
  select id, 'b3 <- ' || tag as tag from b2
),
c as (
  select b2.id, 'c <- (' || b2.tag || ' & ' || b3.tag || ')' as tag
  from b2
  full outer join b3 on b2.id = b3.id
)
select b1.id, b1.tag, a1.tag, c.tag
from b1
full outer join a1 on b1.id = a1.id
full outer join c on c.id = c.id;

with a0 AS (
  select id, 'a0' as tag from cte_mat_9_a
),
a1 as (
  select id, 'a1 <- ' || tag as tag from a0
),
b0 as (
  select id, 'b0' as tag from cte_mat_9_b
),
b1 as (
  select id, 'b1 <- ' || tag as tag from b0
),
b2 as (
  select id, 'b2 <- ' || tag as tag  from b1
),
b3 as (
  select id, 'b3 <- ' || tag as tag from b2
),
c as (
  select b2.id, 'c <- (' || b2.tag || ' & ' || b3.tag || ')' as tag
  from b2
  full outer join b3 on b2.id = b3.id
)
select b1.id, b1.tag, a1.tag, c.tag
from b1
full outer join a1 on b1.id = a1.id
full outer join c on c.id = c.id;
