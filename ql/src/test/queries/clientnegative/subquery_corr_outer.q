create table alltypestiny(
id int,
int_col int,
bigint_col bigint,
bool_col boolean
);

insert into alltypestiny(id, int_col, bigint_col, bool_col) values
(1, 1, 10, true),
(2, 4, 5, false),
(3, 5, 15, true),
(10, 10, 30, false);

create table alltypesagg(
id int,
int_col int,
bool_col boolean
);

insert into alltypesagg(id, int_col, bool_col) values
(1, 1, true),
(2, 4, false),
(5, 6, true),
(null, null, false);

-- Fails because outer joins are not decorrelated properly. Explain plans still contain correlation variables
-- after HiveRelDecorrelator.decorrelateQuery(calcitePlan)
select *
from alltypesagg t1
where t1.id not in
    (select tt1.id
     from alltypestiny tt1 left JOIN alltypesagg tt2
     on tt1.int_col = t1.int_col);
