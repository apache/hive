
-- create a table with two columns one for each point.
create table dist_binary (pointa binary,pointb binary);

-- insert data into the table.
insert into dist_binary values
(ST_Point(12.5,16.2),ST_Point(18.7,4.5)),
(ST_Point(1.5,15.1),ST_Point(1.5,8.7)),
(ST_Point(0,1),ST_Point(0,1)),
(ST_Point(0,10),ST_Point(0,12));

-- Read the data from the table in text format.

select ST_AsText(pointa) as First_Point, ST_AsText(pointb) as Second_Point from dist_binary;

-- Calculate the distance between two points in a row.

select ST_AsText(pointa) as First_Point, ST_AsText(pointb) as Second_Point,
       ST_Length(ST_LineString(array(pointa,pointb))) as Distance from dist_binary;


