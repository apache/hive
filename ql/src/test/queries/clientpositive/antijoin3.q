SET hive.vectorized.execution.enabled=false;
set hive.mapred.mode=nonstrict;
SET hive.auto.convert.join=false;
SET hive.auto.convert.anti.join=true;
-- SORT_QUERY_RESULTS

create table antijoin3_t1 (t1id int not null, t1notnull string not null, t1nullable string);
create table antijoin3_t2 (t2id int not null, t2notnull string not null, t2nullable string);
create table antijoin3_t3 (t3id int not null);

insert into antijoin3_t1 values
(0, "val_0", null),
(1, "val_1", null),
(2, "val_2", "val_2"),
(3, "val_3", "val_3"),
(4, "val_4", "val_4");

insert into antijoin3_t2 values
(0, "val_0", null),
(1, "val_1", null),
(4, "val_4", "val_4"),
(5, "val_5", "val_5");

insert into antijoin3_t3 values (0), (4), (6);

-- do not introduce anti-join if filtering a nullable column with IS NULL
explain cbo select t1id, t1notnull, t1nullable from antijoin3_t1 t1 left join antijoin3_t2 t2 on t1id=t2id where t2nullable is null;
select t1id, t1notnull, t1nullable from antijoin3_t1 t1 left join antijoin3_t2 t2 on t1id=t2id where t2nullable is null;

-- but introduce anti-join if filtering a NOT NULL column with IS NULL
explain cbo select t1id, t1notnull, t1nullable from antijoin3_t1 t1 left join antijoin3_t2 t2 on t1id=t2id where t2notnull is null;
select t1id, t1notnull, t1nullable from antijoin3_t1 t1 left join antijoin3_t2 t2 on t1id=t2id where t2notnull is null;

-- play it safe and do not introduce antijoin for filters combining LHS and RHS columns
explain cbo select t1id, t1notnull, t1nullable from antijoin3_t1 t1 left join antijoin3_t2 t2 on t1id=t2id where (coalesce(t1notnull,t2notnull)) is null;
select t1id, t1notnull, t1nullable from antijoin3_t1 t1 left join antijoin3_t2 t2 on t1id=t2id where (coalesce(t1notnull,t2notnull)) is null;

-- selecting constants do not prevent an anti-join (HIVE-29164)
explain cbo select t1id, t1notnull, t1nullable, "foo" from antijoin3_t1 t1 left join antijoin3_t2 t2 on t1id=t2id where t2notnull is null;
select t1id, t1notnull, t1nullable, "foo" from antijoin3_t1 t1 left join antijoin3_t2 t2 on t1id=t2id where t2notnull is null;

-- check whether nullability is propagated correctly
explain cbo select t1id, t1notnull, t1nullable from antijoin3_t1 t1
left join (select * from antijoin3_t2 left join antijoin3_t3 on t2id=t3id) sq
on t1id=t2id
where t2notnull is null;

select t1id, t1notnull, t1nullable from antijoin3_t1 t1
left join (select * from antijoin3_t2 left join antijoin3_t3 on t2id=t3id) sq
on t1id=t2id
where t2notnull is null;

explain cbo select t1id, t1notnull, t1nullable from antijoin3_t1 t1
left join (select * from antijoin3_t2 left join antijoin3_t3 on t2id=t3id) sq
on t1id=t2id
-- t3id is from the RHS of the left join, so it becomes nullable, so no antijoin
where t3id is null;

select t1id, t1notnull, t1nullable from antijoin3_t1 t1
left join (select * from antijoin3_t2 left join antijoin3_t3 on t2id=t3id) sq
on t1id=t2id
-- t3id is from the RHS of the left join, so it becomes nullable, so no antijoin
where t3id is null;

