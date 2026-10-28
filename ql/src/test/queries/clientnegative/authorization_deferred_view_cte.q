--! qt:authorizer

-- Deferred-auth view access check should not be bypassed through CTEs.
-- The unprivileged user has SELECT on the outer regular view only.
-- Since the inner view is Authorized=false, Hive must re-check the base table
-- privileges against the querying user. The final SELECT should be denied.

set user.name=user_dbowner;

drop view if exists reg_view;
drop view if exists deferred_view;
drop table if exists base_table;

create table base_table(i int, s string);

create view deferred_view as
select i, s from base_table;

alter view deferred_view set tblproperties ('Authorized'='false');

create view reg_view as
with c as (
  select i, s from deferred_view
)
select i, s from c;

grant select on table reg_view to user user_unauth;

set user.name=user_unauth;

select i, s from reg_view;