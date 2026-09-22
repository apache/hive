create table geohash_points (id int, longitude double, latitude double);

insert into geohash_points values
  (1, -126.965375, 43.234528),
  (2, 0, 0),
  (3, 19.0, 47.5),
  (4, -122.4194, 37.7749);

select id, longitude, latitude,
       ST_GeoHash(ST_Point(longitude, latitude), 12) as geohash12,
       ST_GeoHash(ST_Point(longitude, latitude), 5) as geohash5
from geohash_points
order by id;

-- Decode geohash cell.
select ST_AsText(ST_GeomFromGeoHash('9ptty', 5));

select ST_AsText(ST_GeomFromGeoHash('9ptty'));

select ST_NumPoints(ST_GeomFromGeoHash('9ptty', 5));

-- Single-row sanity checks (null / invalid precision).
select ST_GeoHash(ST_Point(0, 0), 0);

select ST_GeoHash(null);

select ST_GeomFromGeoHash('9ptty', 0);
