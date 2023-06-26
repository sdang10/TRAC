----------BOUNDING BOXES-------------

-- OSM in u-district --
CREATE TABLE osm_sidewalk_udistrict1 AS (
	SELECT *
	FROM planet_osm_line
	WHERE	highway = 'footway'
		AND tags -> 'footway' = 'sidewalk' -- ONLY footway = sidewalk
		AND way && st_setsrid( st_makebox2d( st_makepoint(-13616323, 6049894), st_makepoint(-13615733, 6050671)), 3857)
);
-- rename geom column
ALTER TABLE osm_sidewalk_udistrict1 RENAME COLUMN way TO geom;
CREATE INDEX osm_sidewalk_udistrict1_geom ON osm_sidewalk_udistrict1 USING GIST (geom); --???

  
-- Points in u-district --
CREATE table osm_point_udistrict1 AS (
	SELECT *
	FROM planet_osm_point
	WHERE way && st_setsrid( st_makebox2d( st_makepoint(-13616323, 6049894), st_makepoint(-13615733, 6050671) ), 3857)
);
-- rename geom column
ALTER TABLE osm_point_udistrict1 RENAME COLUMN way TO geom;


-- crossing in u-district
CREATE TABLE osm_crossing_udistrict1 AS (
	SELECT *
	FROM planet_osm_line
	WHERE   highway = 'footway'
		AND tags -> 'footway' = 'crossing' -- ONLY footway = sidewalk
		AND way && st_setsrid( st_makebox2d( st_makepoint(-13616323, 6049894), st_makepoint(-13615733, 6050671) ), 3857)
);
-- rename geom
ALTER TABLE osm_crossing_udistrict1 RENAME COLUMN way TO geom;

-- ARNOLD in u-district --
CREATE TABLE arnold.wapr_udistrict1 AS (
	SELECT *
 	FROM arnold.wapr_linestring
	WHERE geom && st_setsrid( st_makebox2d( st_makepoint(-13616323, 6049894), st_makepoint(-13615733, 6050671)), 3857)
	ORDER BY objectid, routeid
);



----------PROCESS----------

-- segmented roads --
-- arnold gave us road geometries which varied in length but or the most part, the roads were very long, so to better
-- associate roads to sidewalks (because one road can have many many sidewalks) we segmented the roads by intersection
CREATE TABLE arnold.segment_test AS
WITH intersection_points AS (
	SELECT DISTINCT
		m1.objectid AS oi1, 
		m1.routeid AS ri1, 
		ST_Intersection(m1.geom, m2.geom) AS geom
	FROM arnold.wapr_udistrict1 AS m1
	JOIN arnold.wapr_udistrict1 AS m2 
		ON ST_Intersects(m1.geom, m2.geom) AND m1.objectid <> m2.objectid 
)
SELECT
	a.objectid, 
	a.routeid, 
	ST_collect(b.geom), 
	ST_Split(a.geom, ST_collect(b.geom))
FROM arnold.wapr_udistrict1 AS a
JOIN intersection_points AS b 
	ON a.objectid = b.oi1 AND a.routeid = b.ri1
GROUP BY
	a.objectid,
	a.routeid,
	a.geom;
-- this makes a collection of linestrings and does not give us singe segmented linestrings
-- so, we make this table to dump the collection into individual linestrings for each segment
CREATE TABLE arnold.segment_test_line AS
SELECT
	objectid,
	routeid, (ST_Dump(st_split)).geom::geometry(LineString, 3857) AS geom
FROM arnold.segment_test;
CREATE INDEX segment_test_line_geom ON arnold.segment_test_line USING GIST (geom);

  
-- big case--
	-- makes the table associating sidewalks with roads in arnold based on buffer, angle (parallel), and midpoint distance.
	-- this conflates all sidewalks over 10 meters to road segments so long it passes 2 tests
		-- 1: the angle of the sidewalk is parallel to the angle of the road segment
		-- 2: a sidewalk buffer of 2 intersects with a sidewalk buffer of 15
	-- we then have the case where still 2 road segments pass the first 2 tests. In this case, we rank the sidewalks based on midpoint distance
CREATE TABLE conflation.long_parallel AS
WITH ranked_roads AS (
  SELECT
    sidewalk.osm_id AS sidewalk_id,
    road.objectid AS road_id,
    road.routeid AS road_routeid,
  	sidewalk.geom AS sidewalk_geom,
    road.geom AS road_geom,
    ABS(DEGREES(ST_Angle(road.geom, sidewalk.geom))) AS angle_degrees,
    ST_Distance(ST_LineInterpolatePoint(road.geom, 0.5), ST_LineInterpolatePoint(sidewalk.geom, 0.5)) AS midpoints_distance,
    ST_length(sidewalk.geom) AS sidewalk_length,
    sidewalk.tags AS tags,
    -- rank this based on the distance of the midpoint of the sidewalk to the midpoint of the road
    ROW_NUMBER() OVER (PARTITION BY sidewalk.geom ORDER BY ST_Distance(ST_LineInterpolatePoint(road.geom, 0.5), ST_LineInterpolatePoint(sidewalk.geom, 0.5)) ) AS RANK
  FROM osm_sidewalk_udistrict1 sidewalk
  JOIN arnold.segment_test_line road
  	ON ST_Intersects(ST_Buffer(sidewalk.geom, 2), ST_Buffer(road.geom, 15))  -- is there a better number?
  WHERE (
		ABS(DEGREES(ST_Angle(road.geom, sidewalk.geom))) BETWEEN 0 AND 10 -- 0
    OR ABS(DEGREES(ST_Angle(road.geom, sidewalk.geom))) BETWEEN 170 AND 190 -- 180
    OR ABS(DEGREES(ST_Angle(road.geom, sidewalk.geom))) BETWEEN 350 AND 360) -- 360
   	AND ( ST_length(sidewalk.geom) > 10 ) -- IGNORE sidewalk that ARE shorter than 10 meters
);
  -- pulls only top ranked sidewalks with the lowest midpoint distance
SELECT
  sidewalk_id,
  road_id,
  road_routeid,
  angle_degrees,
  midpoints_distance,
  sidewalk_length,
  tags,
  sidewalk_geom,
  road_geom,
  'sidewalk' AS label
FROM
  ranked_roads
WHERE
  rank = 1;

-- CREATE ENDGOAL conflation table -- 
CREATE TABLE conflation.conflation_test1 (
	osm_id INT8,
	label VARCHAR(100),
	tags hstore,
	st1_routeid VARCHAR(75),
	st2_routeid VARCHAR(75),
	st3_routeid VARCHAR(75),
	osm_geom GEOMETRY(LineString, 3857)
);

-- insert big case sidewalk
INSERT INTO conflation.conflation_test1 (osm_id, LABEL, tags, st1_routeid, osm_geom)
SELECT
	sidewalk_id AS osm_id,
	LABEL,
	tags,
	road_routeid AS st1_routeid,
	sidewalk_geom AS osm_geom
FROM conflation.long_parallel

-- insert corners --
	-- checks for 2 sidewalk connections on both endpoints of sidewalks not in the conflation table
INSERT INTO conflation.conflation_test1 (osm_id, LABEL, tags, st1_routeid, st2_routeid, osm_geom)
SELECT
	corner.osm_id AS osm_id,
	'corner' AS LABEL,
	corner.tags,
	centerline1.road_routeid AS st1_routeid,
	centerline2.road_routeid AS st2_routeid,
	corner.geom AS osm_geom
FROM osm_sidewalk_udistrict1 corner
JOIN conflation.long_parallel centerline1
	ON st_intersects(st_startpoint(corner.geom), centerline1.sidewalk_geom)
JOIN conflation.long_parallel centerline2
	ON st_intersects(st_endpoint(corner.geom), centerline2.sidewalk_geom)
WHERE corner.geom NOT IN (
	SELECT lp.sidewalk_geom
	FROM conflation.long_parallel lp) AND (centerline1.road_routeid <> centerline2.road_routeid);

-- insert entrances --
	-- checks for if the sidewalk intersects with a point that has the tags entrance or wheelchair
INSERT INTO conflation.conflation_test1 (osm_id, LABEL, tags, osm_geom)
	SELECT sw.osm_id, 'entrance' AS LABEL, point.tags AS tags, sw.geom AS osm_geom
	FROM osm_sidewalk_udistrict1 sw
	JOIN (
		SELECT *
		FROM osm_point_udistrict1 
		WHERE tags -> 'entrance' IS NOT NULL 
			OR tags -> 'wheelchair' IS NOT NULL 
	) AS point 
		ON ST_intersects(sw.geom, point.geom);
