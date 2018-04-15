--! qt:dataset:src
DESCRIBE FUNCTION current_user;
DESCRIBE FUNCTION EXTENDED current_user;

select current_user()
FROM src tablesample (1 rows);
