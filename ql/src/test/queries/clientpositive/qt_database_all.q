--!qt:database:postgres:q_test_country_table.sql
--!qt:database:mysql:q_test_state_table.sql
--!qt:database:mariadb:q_test_city_table.sql
--!qt:database:mssql:q_test_author_table.sql
--!qt:database:oracle:q_test_book_table.sql
CREATE EXTERNAL TABLE country
(
    id int,
    name varchar(20)
)
STORED BY 'org.apache.hive.storage.jdbc.JdbcStorageHandler'
TBLPROPERTIES (
    "hive.sql.database.type" = "POSTGRES",
    "hive.sql.jdbc.driver" = "org.postgresql.Driver",
    "hive.sql.jdbc.url" = "jdbc:postgresql://localhost:5432/qtestDB",
    "hive.sql.dbcp.username" = "qtestuser",
    "hive.sql.dbcp.password" = "qtestpassword",
    "hive.sql.table" = "country"
    );
SELECT * FROM country;

CREATE EXTERNAL TABLE state
(
    name varchar(255),
    country int
)
STORED BY 'org.apache.hive.storage.jdbc.JdbcStorageHandler'
TBLPROPERTIES (
    "hive.sql.database.type" = "MYSQL",
    "hive.sql.jdbc.driver" = "com.mysql.jdbc.Driver",
    "hive.sql.jdbc.url" = "jdbc:mysql://localhost:3306/qtestDB",
    "hive.sql.dbcp.username" = "root",
    "hive.sql.dbcp.password" = "qtestpassword",
    "hive.sql.table" = "state"
    );
SELECT * FROM state;

CREATE EXTERNAL TABLE city
(
    name       varchar(255),
    state int
)
STORED BY 'org.apache.hive.storage.jdbc.JdbcStorageHandler'
TBLPROPERTIES (
    "hive.sql.database.type" = "MYSQL",
    "hive.sql.jdbc.driver" = "org.mariadb.jdbc.Driver",
    "hive.sql.jdbc.url" = "jdbc:mariadb://localhost:3309/qtestDB",
    "hive.sql.dbcp.username" = "root",
    "hive.sql.dbcp.password" = "qtestpassword",
    "hive.sql.table" = "city"
);
SELECT * FROM city;

CREATE EXTERNAL TABLE author
(
    id    int,
    fname varchar(20),
    lname varchar(20)
)
STORED BY 'org.apache.hive.storage.jdbc.JdbcStorageHandler'
TBLPROPERTIES (
    "hive.sql.database.type" = "MSSQL",
    "hive.sql.jdbc.driver" = "com.microsoft.sqlserver.jdbc.SQLServerDriver",
    "hive.sql.jdbc.url" = "jdbc:sqlserver://localhost:1433",
    "hive.sql.dbcp.username" = "sa",
    "hive.sql.dbcp.password" = "Its-a-s3cret",
    "hive.sql.table" = "author"
    );
SELECT * FROM author;

CREATE EXTERNAL TABLE book
(
    id int,
    title varchar(100),
    author int
)
    STORED BY 'org.apache.hive.storage.jdbc.JdbcStorageHandler'
    TBLPROPERTIES (
        "hive.sql.database.type" = "ORACLE",
        "hive.sql.jdbc.driver" = "oracle.jdbc.OracleDriver",
        "hive.sql.jdbc.url" = "jdbc:oracle:thin:@//localhost:1521/xe",
        "hive.sql.dbcp.username" = "SYS as SYSDBA",
        "hive.sql.dbcp.password" = "oracle",
        "hive.sql.table" = "BOOK"
        );
SELECT * FROM book;
