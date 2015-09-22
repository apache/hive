copy src to src2 at mysqlconn;
copy (select * from src) to src2 at mysqlconn;