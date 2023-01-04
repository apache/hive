-- create a table with two columns one for each point.
create table geom_binary
(
  id   int,
  geom binary
);

-- insert data into the table.
insert into geom_binary values
(1, ST_GeomFromText('multipolygon (((0 0, 0 1, 1 0, 0 0)), ((2 2, 2 3, 3 2, 2 2)))')),
(2, ST_GeomFromText('multilinestring ((2 4, 10 10), (20 20, 7 8))')),
(3, ST_GeomFromText('MULTIPOINT ((10 40), (40 30), (20 20), (30 10))')),
(4, ST_GeomFromText('polygon ((0 0, 0 10, 10 10, 0 0))')),
(6, ST_GeomFromText('linestring (10 10, 20 20)')),
(7, ST_GeomFromText('point (10.02 20.01)')),
(8, ST_GeomFromText('linestring z (1.5 2.5 2, 3.0 2.2 1)')),
(9, ST_GeomFromText('multipoint z((0 0 1), (2 2 3))')),
(10, ST_GeomFromText('point z(10.02 20.01 25.0)')),
(11, ST_GeomFromText('multipolygon (((0 0, 1 0, 0 1, 0 0)), ((2 2, 1 2, 2 1, 2 2)))')),
(12, ST_GeomFromText('polygon ((0 0, 8 0, 0 8, 0 0), (1 1, 1 5, 5 1, 1 1))')),
(13, ST_PointZ(1.5, 2.5, 2)),
(14, ST_Point(5, 6)),
(15, ST_Polygon(1,1, 1,4, 4,4, 4,1)),
(16, ST_Linestring('linestring (10 10, 20 20)')),
(17, ST_LineString(0.,0., 3.,4., 0.,4., 0.,0.)),
(18, ST_MultiLineString('multilinestring ((0 0, 3 4, 2 2, 0 0), (6 2, 7 5, 6 8, 6 2))')),
(19, ST_GeomFromText('point empty')),
(20, ST_LineString(0,0, 1,0, 1,1, 0,2, 2,2, 1,1, 2,0)),
(21, ST_Point('point m(0. 3. 1)')),
(22, ST_Point('pointzm (0. 3. 1. 2.)')),
(23, ST_GeomFromText('linestring m (10 10 2, 20 20 4)')),
(24, ST_Polygon(1, 1, 1, 4, 4, 4, 4, 1)),
(25, ST_Polygon(2,0, 2,1, 3,1)),
(26, ST_GeomFromJSON(('{"x":0.0,"y":0.0}'))),
(27, ST_GeomFromGeoJSON('{"type":"LineString", "coordinates":[[1,2], [3,4]]}')),
(28, ST_GeometryN(ST_GeomFromText('multilinestring ((2 4, 10 10), (20 20, 7 8))'), 2));


-- Check the values as text.
select id, ST_AsText(geom), ST_GeometryType(ST_GeomFromWKB(ST_AsBinary(geom))), ST_MaxX(geom), ST_MaxY(geom),
       ST_MaxZ(geom), ST_MinX(geom), ST_MinY(geom), ST_MinZ(geom), ST_NumGeometries(geom), ST_AsText(ST_Centroid(geom)),
       ST_Dimension(geom), ST_IsEmpty(geom), ST_IsMeasured(geom), ST_IsSimple(geom)
from geom_binary
order by id;

select ST_GeometryType(ST_MLineFromWKB(ST_AsBinary(geom)))
from geom_binary
where id = 2;

select ST_GeometryType(ST_MPointFromWKB(ST_AsBinary(geom)))
from geom_binary
where id = 9;

select ST_GeometryType(ST_MPolyFromWKB(ST_AsBinary(geom)))
from geom_binary
where id = 11;

select ST_AsJson(ST_MultiLineString(ST_AsText(geom)))
from geom_binary
where id = 2;

select ST_Equals(ST_MultiPoint((ST_AsText(geom))), ST_GeomFromText('MULTIPOINT ((10 40), (40 30))')),
       ST_Equals(ST_MultiPoint((ST_AsText(geom))), ST_GeomFromText('MULTIPOINT ((10 40), (40 30), (20 20), (30 10))'))
from geom_binary
where id = 3;

select ST_AsJson(ST_MultiPolygon(ST_AsText(geom)))
from geom_binary
where id = 11;

select ST_AsText(ST_PointN(geom, 2))
from geom_binary
where id = 3;

select ST_NumInteriorRing(ST_Polygon(ST_AsText(geom))),
       ST_GeometryType(ST_PolyFromWKB(ST_AsBinary(geom)))
from geom_binary
where id = 4 OR id = 12;

select ST_NumPoints(geom)
from geom_binary
where id = 2 OR id = 3 OR id = 9;

select ST_Overlaps(ST_Polygon(ST_AsText(geom)), ST_Polygon(1, 1, 1, 4, 4, 4, 4, 1))
from geom_binary
where id = 4;

select ST_GeometryType(ST_PointFromWKB(ST_AsBinary(geom)))
from geom_binary
where id = 7;

select ST_GeometryType(ST_PointFromWKB(ST_AsBinary(geom)))
from geom_binary
where id = 7;

select ST_X(geom), ST_Y(geom), ST_Z(geom), ST_CoordDim(geom), ST_Is3D(geom)
from geom_binary
where id = 14 OR id = 13 order by id;

select id
from geom_binary
where (id = 4 OR id = 12 OR id = 15) AND (ST_Within(ST_Point(2, 3), (ST_Polygon(ST_AsText(geom)))));

select id
from geom_binary
where (id = 4 OR id = 12 OR id = 15) AND (ST_Contains((ST_Polygon(ST_AsText(geom))), ST_Point(2, 3)));

select id, ST_Area(ST_Polygon(ST_AsText(geom)))
from geom_binary
where (id = 4 OR id = 12 OR id = 15);

select ST_AsText(ST_Boundary(geom))
from geom_binary
where id = 4;

select ST_AsText(ST_Buffer(geom, 1))
from geom_binary
where id = 7;

SELECT ST_AsText(ST_ConvexHull(geom, ST_Point(0, 1), ST_Point(1, 1)))
from geom_binary
where id = 14;

select ST_IsClosed(geom)
from geom_binary
where id = 16 OR id = 17 OR id =18;

select ST_IsRing(geom), ST_AsText(ST_StartPoint(geom)), ST_AsText(ST_EndPoint(geom)),
       ST_Distance(geom, ST_Point(3.0, 4.0)), ST_Crosses(geom, st_linestring(15,0, 15,15))
from geom_binary
where id = 16 OR id = 17;

select ST_Equals(ST_LineFromWKB(ST_AsBinary(geom)), ST_GeomFromText('linestring (11 12, 21 23)')),
       ST_Equals(ST_LineFromWKB(ST_AsBinary(geom)), ST_GeomFromText('linestring (10 10, 20 20)'))
from geom_binary
where id = 6;

select ST_M(geom), ST_MaxM(geom), ST_MinM(geom)
from geom_binary
where id = 21 OR id = 22 OR id=23;

select ST_Touches(ST_Point(1, 2), geom), ST_Touches(ST_Point(8, 8), geom)
from geom_binary
where id = 24;

select ST_Relate(geom, ST_Polygon(1, 1, 1, 4, 4, 4, 4, 1), '****T****'),
       ST_Relate(geom, ST_Polygon(1, 1, 1, 4, 4, 4, 4, 1), 'T********')
from geom_binary
where id = 25;














