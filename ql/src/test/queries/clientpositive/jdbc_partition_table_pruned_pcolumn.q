--!qt:database:postgres:q_test_book_table.sql

CREATE EXTERNAL TABLE book
(
    id int,
    title varchar(20),
    author int
)
STORED BY                                          
'org.apache.hive.storage.jdbc.JdbcStorageHandler'
TBLPROPERTIES (                                    
    "hive.sql.database.type" = "POSTGRES",
    "hive.sql.jdbc.driver" = "org.postgresql.Driver",
    "hive.sql.jdbc.url" = "jdbc:postgresql://localhost:5432/qtestDB",
    "hive.sql.dbcp.username" = "qtestuser",
    "hive.sql.dbcp.password" = "qtestpassword",
    "hive.sql.table" = "book",
    "hive.sql.partitionColumn" = "author",
    "hive.sql.numPartitions" = "2"
);

set hive.fetch.task.conversion=none;
select book.id from book;
