--! qt:database:postgres:q_test_book_table.sql

CREATE EXTERNAL TABLE book (id int, title varchar(100), author int)
STORED BY 'org.apache.hive.storage.jdbc.JdbcStorageHandler'
TBLPROPERTIES (
    "hive.sql.database.type" = "POSTGRES",
    "hive.sql.jdbc.driver" = "org.postgresql.Driver",
    "hive.sql.jdbc.url" = "jdbc:postgresql://localhost:5432/qtestDB",
    "hive.sql.dbcp.username" = "qtestuser",
    "hive.sql.dbcp.password" = "qtestpassword",
    "hive.sql.table" = "book");

explain cbo
select * from book 
where id = 0 or (id = 1 and author = 11) or (id = 2 and author = 22);

-- non jdbc plan containing ROW, for reference
CREATE EXTERNAL TABLE book1 (id int, title varchar(100), author int);
explain cbo
select * from book1
where id = 0 or (id = 1 and author = 11) or (id = 2 and author = 22);
