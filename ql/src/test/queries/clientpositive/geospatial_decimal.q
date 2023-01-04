
--create a table with 4 columns, 2 columns for each point(longitude, latitude)
create table distances (alongitude double, alatitude double, blongitude double, blatitude double);

-- insert some values into the table.
insert into distances values
(cast(12.5 as double),cast(16.2 as double),cast(18.7 as double),cast(4.5 as double)),
(cast(1.5 as double),cast(15.1 as double),cast(1.5 as double),cast(8.7 as double)),
(cast(0 as double),cast(1 as double),cast(0 as double),cast(1 as double)),
(cast(0 as double),cast(10 as double),cast(0 as double),cast(12 as double));

-- Read the table
select * from distances;

-- Calculate the distance between the points.

select distances.alongitude,distances.alatitude,distances.blongitude,distances.blatitude,
       ST_Length(ST_LineString(distances.alongitude,distances.alatitude,distances.blongitude,distances.blatitude)) AS distance
from distances;




