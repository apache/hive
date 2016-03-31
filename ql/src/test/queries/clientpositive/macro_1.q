set hive.mapred.mode=nonstrict;

CREATE TEMPORARY MACRO STRING_LEN(x string) length(x);
CREATE TEMPORARY MACRO STRING_LEN_PLUS_ONE(x string) length(x)+1;
CREATE TEMPORARY MACRO STRING_LEN_PLUS_TWO(x string) length(x)+2;

create table macro_test (x string);

insert into table macro_test values ("bb"), ("a"), ("ccc");

SELECT
    CONCAT(STRING_LEN(x), ":", STRING_LEN_PLUS_ONE(x), ":", STRING_LEN_PLUS_TWO(x)) a
FROM macro_test;

SELECT
    CONCAT(STRING_LEN(x), ":", STRING_LEN_PLUS_ONE(x), ":", STRING_LEN_PLUS_TWO(x)) a
FROM macro_test
sort by a;


SELECT
    CONCAT(STRING_LEN(x), ":", STRING_LEN_PLUS_ONE(x), ":", STRING_LEN_PLUS_TWO(x)) a
FROM macro_test
sort by a desc;





