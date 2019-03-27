create table t1 as
	select array(1,2,3) as col;

insert into t1 values(null);

select explode(col) as myCol from t1;

create table t2 as
	select map(1,'one',2,'two',3,'three') as col;

insert into t2 values(null);

SELECT explode(col) AS (myCol1,myCol2) t2;


