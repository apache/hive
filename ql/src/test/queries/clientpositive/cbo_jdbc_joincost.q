--!qt:database:mysql:q_test_author_book_tables.sql
CREATE EXTERNAL TABLE author
(
    id int,
    fname varchar(20),
    lname varchar(20)
)
STORED BY 'org.apache.hive.storage.jdbc.JdbcStorageHandler'
TBLPROPERTIES (
    "hive.sql.database.type" = "MYSQL",
    "hive.sql.jdbc.driver" = "com.mysql.jdbc.Driver",
    "hive.sql.jdbc.url" = "jdbc:mysql://localhost:3306/qtestDB",
    "hive.sql.dbcp.username" = "root",
    "hive.sql.dbcp.password" = "qtestpassword",
    "hive.sql.table" = "author"
    );

CREATE EXTERNAL TABLE book
(
    id int,
    title varchar(100),
    author int
)
STORED BY 'org.apache.hive.storage.jdbc.JdbcStorageHandler'
TBLPROPERTIES (
    "hive.sql.database.type" = "MYSQL",
    "hive.sql.jdbc.driver" = "com.mysql.jdbc.Driver",
    "hive.sql.jdbc.url" = "jdbc:mysql://localhost:3306/qtestDB",
    "hive.sql.dbcp.username" = "root",
    "hive.sql.dbcp.password" = "qtestpassword",
    "hive.sql.table" = "book"
    );

EXPLAIN CBO JOINCOST SELECT a.lname, b.title FROM author a JOIN book b ON a.id=b.author;
