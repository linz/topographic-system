SELECT id_left, id_right,geom_type,
       ST_Area(ST_Transform(intersection_geom, 2193)) AS intersection_area,
	   intersection_geom
FROM
(SELECT
    a.id AS id_left,
    b.id AS id_right,
	GeometryType(ST_Intersection(a.geom, b.geom)) AS geom_type,
	ST_Intersection(a.geom, b.geom) AS intersection_geom,
	ST_Area(ST_Intersection(a.geom, b.geom)) AS intersection_area
FROM
    toposource.building a
JOIN
    toposource.building b
ON
    a.id <> b.id
    AND ST_Intersects(a.geom, b.geom));