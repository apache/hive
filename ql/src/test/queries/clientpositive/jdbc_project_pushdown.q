--!qt:database:postgres:q_test_author_book_tables.sql

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
    "hive.sql.table" = "book"
);

CREATE EXTERNAL TABLE author
( id int,
fname string,
lname string)
STORED BY
'org.apache.hive.storage.jdbc.JdbcStorageHandler'
TBLPROPERTIES (                                    
    "hive.sql.database.type" = "POSTGRES",
    "hive.sql.jdbc.driver" = "org.postgresql.Driver",
    "hive.sql.jdbc.url" = "jdbc:postgresql://localhost:5432/qtestDB",
    "hive.sql.dbcp.username" = "qtestuser",
    "hive.sql.dbcp.password" = "qtestpassword",
    "hive.sql.table" = "author"
);

explain cbo select id from book where substring(title, 0, 4) = 'Les';
explain select id from book where substring(title, 0, 4) = 'Les';
select id from book where substring(title, 0, 4) = 'Les';
----

explain cbo select id from book where ucase(title) = 'LES MISERABLES';
explain select id from book where ucase(title) = 'LES MISERABLES';
select id from book where ucase(title) = 'LES MISERABLES';
----

explain cbo select book.title, author.fname from book join author on book.author = author.id;
explain select book.title, author.fname from book join author on book.author = author.id;
select book.title, author.fname from book join author on book.author = author.id;
----

explain cbo select book.title, author.fname from book join author
where book.author = author.id
and ucase(book.title) = 'LES MISERABLES'
and substring(author.lname, 0, 4) = 'Hug';

explain select book.title, author.fname from book join author
where book.author = author.id
and ucase(book.title) = 'LES MISERABLES'
and substring(author.lname, 0, 4) = 'Hug';

select book.title, author.fname from book join author
where book.author = author.id
and ucase(book.title) = 'LES MISERABLES'
and substring(author.lname, 0, 4) = 'Hug';
----

explain cbo 
select author.fname, count(book.title) as books
from book join author
where book.author = author.id
group by author.fname;

explain 
select author.fname, count(book.title) as books
from book join author
where book.author = author.id
group by author.fname;

select author.fname, count(book.title) as books
from book join author
where book.author = author.id
group by author.fname;
----

explain cbo 
select ucase(author.fname), count(book.title) as books
from book join author
where book.author = author.id
group by author.fname
order by ucase(author.fname)
limit 5;

explain 
select ucase(author.fname), count(book.title) as books
from book join author
where book.author = author.id
group by author.fname
order by ucase(author.fname)
limit 5;

select ucase(author.fname), count(book.title) as books
from book join author
where book.author = author.id
group by author.fname
order by ucase(author.fname)
limit 5;
----
