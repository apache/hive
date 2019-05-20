set metastore.strict.managed.tables=true;

-- External non-transactional table ok
create external table strict_managed_tables3_tab1 (c1 string, c2 string) stored as textfile;

-- Trying to change the table to non-external is not
alter table strict_managed_tables3_tab1 unset tblproperties ('EXTERNAL');
