set hive.security.authorization.manager=org.apache.hadoop.hive.ql.security.authorization.DefaultHiveAuthorizationProvider;

create table my_passwd (
username string,
uid int);

insert into my_passwd values
                      ("Deepak", 1),
                      ("Gunther", 2),
                      ("Jason", 3),
                      ("Prasanth", 4),
                      ("Gopal", 5),
                      ("Sergey", 6);


set hive.cbo.enable=false;
create view my_passwd_vw as select * from my_passwd limit 3;

set hive.security.authorization.enabled=true;
grant select on table my_passwd to user hive_test_user;
grant select on table my_passwd_vw to user hive_test_user;

select * from my_passwd_vw;