--!qt:database:postgres:qdb:q_test_book_table.sql

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
    "hive.sql.jdbc.url" = "${system:hive.test.database.qdb.jdbc.url}",
    "hive.sql.dbcp.username" = "${system:hive.test.database.qdb.jdbc.username}",
    "hive.sql.dbcp.password" = "${system:hive.test.database.qdb.jdbc.password}",
    "hive.sql.table" = "book",
    "hive.sql.partitionColumn" = "author",
    "hive.sql.numPartitions" = "2"
);

set hive.fetch.task.conversion=none;
select book.id from book;
