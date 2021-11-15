CREATE TABLE complex1 (
    c0 int,
    c1 array<int>,
    c2 map<int, string>,
    c3 struct<f1:int,f2:string,f3:array<int>>,
    c4 array<struct<f1:int,f2:string,f3:array<int>>>);

INSERT INTO complex1
    SELECT 3,
       array(1, 2, null),
       map(1, 'one', 2, null),
       named_struct('f1', cast(null as int), 'f2', cast(null as string), 'f3', array(1, 2, null)),
       array(named_struct('f1', 11, 'f2', 'two', 'f3', array(2, 3, 4)));

select * from complex1;
