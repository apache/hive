set hive.fetch.task.conversion=none;
set hive.optimize.ppd=false;

create table t (s1 string,s2 string);

insert into t values (null,null);

explain
select
	coalesce(s1, 'null_value' ), coalesce(s2, 'null_value' ), 
	coalesce(s1, 'null_value' )=coalesce(s2, 'null_value' ),
	case when coalesce(s1, 'null_value' )=coalesce(s2, 'null_value' ) then 'eq' else 'noteq' end
from t;

select
	coalesce(s1, 'null_value' ), coalesce(s2, 'null_value' ), 
	coalesce(s1, 'null_value' )=coalesce(s2, 'null_value' ),
	case when coalesce(s1, 'null_value' )=coalesce(s2, 'null_value' ) then 'eq' else 'noteq' end
from t;

