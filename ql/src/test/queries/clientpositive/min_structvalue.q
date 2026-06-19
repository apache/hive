select max(a), min(a) FROM (select named_struct("field",1) as a union all select named_struct("field",2) as a union all select named_struct("field",cast(null as int)) as a) tmp;

select min(a) FROM (select named_struct("field",1) as a union all select named_struct("field",-2) as a union all select named_struct("field",cast(null as int)) as a) tmp;

select min(a) FROM (select named_struct("field",1) as a union all select named_struct("field",2) as a union all select named_struct("field",cast(5 as int)) as a) tmp;

select min(a) FROM (select named_struct("field",1, "secf", cast(null as int) ) as a union all select named_struct("field",2, "secf", 3) as a union all select named_struct("field",cast(5 as int), "secf", 4) as a) tmp;

select min(a) FROM (select named_struct("field",1, "secf", 2) as a union all select named_struct("field",-2, "secf", 3) as a union all select named_struct("field",cast(null as int), "secf", 1) as a) tmp;

