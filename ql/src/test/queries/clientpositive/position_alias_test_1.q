create table alias_test_01(a INT, b STRING) ;
                         create table alias_test_02(a INT, b STRING) ;
                         create table alias_test_03(a INT, b STRING) ;
                         set hive.groupby.position.alias = true;
                         set hive.cbo.enable=true;


                         explain
                         select * from
                         alias_test_01 alias01
                         left join
                         (
                         select 2017 as a, b from alias_test_02 group by 1, 2
                         ) alias02
                         on alias01.a = alias02.a
                         left join
                         alias_test_03 alias03
                         on alias01.a = alias03.a;