create table test_struct
(
    f1 string,
    demo_struct struct<f1:string,f2:string,f3:string>,
    datestr string
);

insert into test_struct(f1, datestr) select 'test_f1','datestr_1';

drop table test_struct;