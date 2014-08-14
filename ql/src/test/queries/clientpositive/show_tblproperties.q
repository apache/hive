
create table tmpfoo (a String);
show tblproperties tmpfoo("bar");
show tblproperties default.tmpfoo("bar");

alter table tmpfoo set tblproperties ("bar" = "bar value");
alter table tmpfoo set tblproperties ("tmp" = "true");

show tblproperties tmpfoo;
show tblproperties tmpfoo("bar");

show tblproperties default.tmpfoo;
show tblproperties default.tmpfoo("bar");

CREATE DATABASE db1;
USE db1;

CREATE TABLE tmpfoo (b STRING);
alter table tmpfoo set tblproperties ("bar" = "bar value1");
alter table tmpfoo set tblproperties ("tmp" = "true1");

-- from db1 to default db
show tblproperties default.tmpfoo;
show tblproperties default.tmpfoo("bar");

-- from db1 to db1
show tblproperties tmpfoo;
show tblproperties tmpfoo("bar");

use default;
-- from default to db1
show tblproperties db1.tmpfoo;
show tblproperties db1.tmpfoo("bar");

