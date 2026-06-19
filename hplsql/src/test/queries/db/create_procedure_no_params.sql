CREATE OR REPLACE PROCEDURE show_the_date
IS
today DATE DEFAULT DATE '2016-03-10';
BEGIN
-- Display the date.
DBMS_OUTPUT.PUT_LINE ('Today is ' || today);
END show_the_date;

CREATE OR REPLACE PROCEDURE show_the_date2()
IS
today DATE DEFAULT DATE '2016-03-10';
BEGIN
-- Display the date.
DBMS_OUTPUT.PUT_LINE ('Today is ' || today);
END show_the_date2;

call show_the_date;
call show_the_date2;

DECLARE
today DATE DEFAULT DATE '2016-03-10';
BEGIN
-- Display the date.
DBMS_OUTPUT.PUT_LINE ('Today is ' || today);
END;