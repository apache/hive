-- Check comparison between decimal column and string literal
set hive.explain.user=false;
create table t1 (a decimal (3,1));
explain select * from t1 where a = '22.3';
explain select * from t1 where a = '2.3';
explain select * from t1 where a = '213.223';
explain select * from t1 where a = '';
explain select * from t1 where a = 'ab';

-- Check comparison between string column and decimal literal
create table str_tbl (string_col string, char_col char(20), varchar_col varchar(20));
insert into str_tbl values ('12089257425232694581','12089257425232694581','12089257425232694581');
-- Filter should not match / Empty result
select * from str_tbl where string_col=12089257425232694599;
select * from str_tbl where char_col=12089257425232694599;
select * from str_tbl where varchar_col=12089257425232694599;
-- Filter should not match / Empty result
select * from str_tbl where string_col=1208925742523269458188;
select * from str_tbl where char_col=1208925742523269458188;
select * from str_tbl where varchar_col=1208925742523269458188;
-- Filter should match / One row
select * from str_tbl where string_col=12089257425232694581;
select * from str_tbl where char_col=12089257425232694581;
select * from str_tbl where varchar_col=12089257425232694581;

explain select * from str_tbl where string_col=12089257425232694599;
explain select * from str_tbl where 12089257425232694599=string_col;
explain select * from str_tbl where string_col=223;
explain select * from str_tbl where 223=string_col;
explain select * from str_tbl where string_col=223.3;
explain select * from str_tbl where 223.3=string_col;

explain select * from str_tbl where char_col =12089257425232694599;
explain select * from str_tbl where 12089257425232694599= char_col;
explain select * from str_tbl where char_col =223;
explain select * from str_tbl where 223= char_col;
explain select * from str_tbl where char_col =223.3;
explain select * from str_tbl where 223.3 = char_col;

explain select * from str_tbl where varchar_col=12089257425232694599;
explain select * from str_tbl where 12089257425232694599= varchar_col;
explain select * from str_tbl where varchar_col=223;
explain select * from str_tbl where 223=varchar_col;
explain select * from str_tbl where varchar_col=223.3;
explain select * from str_tbl where 223.3=varchar_col;

-- Check comparison between decimal column and string literal
-- Decimal without qualifier becomes by default decimal(10,0)
create table dec_tbl (decimal_col decimal(21,1));
insert into dec_tbl values (12089257425232694599.8);
-- Filter do not match since literal is casted to decimal(10,0) and due to lose of precision ends up null
select * from dec_tbl where decimal_col='12089257425232694599';
select * from dec_tbl where decimal_col='12089257425232694599.84';

explain select * from dec_tbl where decimal_col='12089257425232694599';
explain select * from dec_tbl where '12089257425232694599'=decimal_col;

-- Check comparison between decimal and string columns
-- No matches / Empty result
select * from str_tbl inner join dec_tbl on string_col=decimal_col;
select * from str_tbl inner join dec_tbl on char_col=decimal_col;
select * from str_tbl inner join dec_tbl on varchar_col=decimal_col;

explain select * from str_tbl inner join dec_tbl on string_col= decimal_col;
explain select * from str_tbl inner join dec_tbl on decimal_col =string_col;

explain select * from str_tbl inner join dec_tbl on char_col = decimal_col;
explain select * from str_tbl inner join dec_tbl on decimal_col = char_col;

explain select * from str_tbl inner join dec_tbl on varchar_col= decimal_col;
explain select * from str_tbl inner join dec_tbl on decimal_col =varchar_col;