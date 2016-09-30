DESCRIBE FUNCTION logged_in_user;
DESCRIBE FUNCTION EXTENDED logged_in_user;

select logged_in_user()
FROM src tablesample (1 rows);
