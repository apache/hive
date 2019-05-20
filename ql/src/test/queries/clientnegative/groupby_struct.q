CREATE TABLE addresses (
    name float,
    address struct<intVals:int,strVals:string>
 );

SELECT address FROM addresses GROUP BY address.intVals;
