-----------------------------------------
--BOUNDING BOX 1--
--OSM in U-District--
CREATE TABLE osm_sidewalk_udistrict AS (
	SELECT *
	FROM planet_osm_line
	WHERE 	highway = 'footway'
		AND tags -> 'footway' = 'sidewalk' -- ONLY footway = sidewalk
		AND way && st_setsrid( st_makebox2d( st_makepoint(-13616401,6049782), st_makepoint(-13615688,6050649)), 3857)
)

--SMALLER OSM in U-District--
create table osm_sidewalk_udistrict1 AS (
	SELECT *
	FROM planet_osm_line
	WHERE highway = 'footway'
		AND tags -> 'footway' = 'sidewalk' -- ONLY footway = sidewalk
		AND	way && st_setsrid( st_makebox2d( st_makepoint(-13616323, 6049894), st_makepoint(-13615733, 6050671) ), 3857)
)

ALTER TABLE osm_sidewalk_udistrict1 RENAME COLUMN way TO geom;

--Arnold in U-District--
CREATE TABLE arnold.wapr_udistrict AS (
	SELECT *
	FROM arnold.wapr_linestring
	WHERE geom && st_setsrid( st_makebox2d( st_makepoint(-13616401,6049782), st_makepoint(-13615688,6050649)), 3857)
);

--SMALLER Arnold in U-District--
CREATE TABLE arnold.wapr_udistrict1 AS (
	SELECT *
	FROM arnold.wapr_linestring
 	WHERE geom && st_setsrid( st_makebox2d( st_makepoint(-13616323, 6049894), st_makepoint(-13615733, 6050671)), 3857)
	ORDER BY objectid, routeid
);

-----------------------------------------
--CONFLATION PROCESS BB1--

--Creates a table of geometry collections (of linestrings) for the roads which are split at intersection points--
CREATE TABLE arnold.segment_test AS
WITH intersection_points AS (
  SELECT DISTINCT m1.objectid oi1, m1.routeid ri1, ST_Intersection(m1.geom, m2.geom) AS geom
  FROM arnold.wapr_udistrict1 m1
  JOIN arnold.wapr_udistrict1 m2 ON ST_Intersects(m1.geom, m2.geom) AND m1.objectid <> m2.objectid
)

SELECT a.objectid, a.routeid, ST_collect(b.geom), ST_Split(a.geom, ST_collect(b.geom))
FROM arnold.wapr_udistrict1 AS a
JOIN intersection_points AS b ON a.objectid = b.oi1 AND a.routeid = b.ri1
GROUP BY a.objectid, a.routeid, a.geom;

--Creates a table to dump linestrings out of geom collections into distinct road segments--
CREATE TABLE arnold.segment_test_line AS
SELECT objectid, routeid, (ST_Dump(st_split)).geom::geometry(LineString, 3857) AS geom
FROM arnold.segment_test;

--Create a geom index for segment_test_line and osm_sidewalk_udistrict1--
CREATE INDEX segment_test_line_geom ON arnold.segment_test_line USING GIST (geom)
CREATE INDEX osm_sidewalk_udistrict1_geom ON osm_sidewalk_udistrict1 USING GIST (geom)

--Associates sidewalks to roads based on buffer intersection, parallel-ness, and ranks by midpoint distance to roads--
WITH ranked_roads AS (
	SELECT
  	sidewalk.osm_id AS sidewalk_id,
  	sidewalk.geom AS sidewalk_geom,
    road.geom AS road_geom,
    ABS(DEGREES(ST_Angle(road.geom, sidewalk.geom))) AS angle_degrees,
    ST_Distance(ST_StartPoint(road.geom), ST_StartPoint(sidewalk.geom)) AS start_start_distance,
    ST_Distance(ST_EndPoint(road.geom), ST_EndPoint(sidewalk.geom)) AS end_end_distance,
    ST_Distance(ST_StartPoint(road.geom), ST_EndPoint(sidewalk.geom)) AS start_end_distance,
    ST_Distance(ST_EndPoint(road.geom), ST_StartPoint(sidewalk.geom)) AS end_start_distance,
    ST_Distance(ST_LineInterpolatePoint(road.geom, 0.5), ST_LineInterpolatePoint(sidewalk.geom, 0.5)) AS midpoints_distance,
    ST_length(sidewalk.geom) AS sidewalk_length,
    ROW_NUMBER() OVER (PARTITION BY sidewalk.geom ORDER BY ST_Distance(ST_LineInterpolatePoint(road.geom, 0.5), ST_LineInterpolatePoint(sidewalk.geom, 0.5)) ) AS rank
  FROM
    osm_sidewalk_udistrict1 sidewalk
  JOIN
    arnold.segment_test_line road
  ON
    ST_Intersects(ST_Buffer(sidewalk.geom, 2), ST_Buffer(road.geom, 15))
  WHERE
    	(ABS(DEGREES(ST_Angle(road.geom, sidewalk.geom))) BETWEEN 0 AND 10
    	OR ABS(DEGREES(ST_Angle(road.geom, sidewalk.geom))) BETWEEN 170 AND 190
    	OR ABS(DEGREES(ST_Angle(road.geom, sidewalk.geom))) BETWEEN 330 AND 360)
    AND ( ST_length(sidewalk.geom) > 9 )
)
--selects the widewalk with the highest rank if the sidewalk is associated with 2 roads--
SELECT
  sidewalk_id,
  sidewalk_geom,
  road_geom,
  angle_degrees,
  start_start_distance,
  end_end_distance,
  start_end_distance,
  end_start_distance,
  midpoints_distance,
  sidewalk_length
FROM
  ranked_roads
WHERE
  rank = 1;
