----- BOUNDING BOX SETUP -----

-- OSM sidewalks in u-district --
CREATE TABLE shaneud1.osm_sw AS (
	SELECT *
	FROM planet_osm_line
	WHERE	highway = 'footway'
		AND tags -> 'footway' = 'sidewalk' -- ONLY footway = sidewalk
		AND way && st_setsrid( st_makebox2d( st_makepoint(-13616323, 6049894), st_makepoint(-13615733, 6050671)), 3857)
);
-- rename geom column
ALTER TABLE shaneud1.osm_sw RENAME COLUMN way TO geom;


-- OSM points in u-district --
CREATE TABLE shaneud1.osm_point AS (
	SELECT *
	FROM planet_osm_point
	WHERE way && st_setsrid( st_makebox2d( st_makepoint(-13616323, 6049894), st_makepoint(-13615733, 6050671) ), 3857)
);
-- rename geom column
ALTER TABLE shaneud1.osm_point RENAME COLUMN way TO geom;


-- OSM crossing in u-district -- 
CREATE TABLE shaneud1.osm_crossing AS (
	SELECT *
	FROM planet_osm_line
	WHERE   highway = 'footway'
		AND tags -> 'footway' = 'crossing' -- ONLY footway = crossing
		AND way && st_setsrid( st_makebox2d( st_makepoint(-13616323, 6049894), st_makepoint(-13615733, 6050671) ), 3857)
);
-- rename geom column
ALTER TABLE shaneud1.osm_crossing RENAME COLUMN way TO geom;


-- ARNOLD roads in u-district --
-- for arnold, we wanted to change the shape column to linestring (instead of multilinestring M) to match geom type of osm
-- to do this, we needed to keep track of original ID as separating one multilinestring can result in multiple linestrings
-- the og_objectid column keps track of the arnold object id when converting. 
CREATE TABLE arnold.wapr_linestring (
 	objectid SERIAL PRIMARY KEY,
  	og_objectid INT8,
  	routeid VARCHAR(75),
  	beginmeasure FLOAT8,
  	endmeasure FLOAT8,
  	shape_length FLOAT8,
  	geom geometry(linestring, 3857)
);

-- converting the MultiLineString geometries into LineString geometries
INSERT INTO arnold.wapr_linestring (og_objectid, routeid, beginmeasure, endmeasure, shape_length, geom)
	SELECT 
		objectid, 
		routeid, 
		beginmeasure, 
		endmeasure, 
		shape_length, 
		ST_Force2D((ST_Dump(shape)).geom)::geometry(linestring, 3857)
	FROM arnold.wapr_hpms_submittal;

-- table to use
CREATE TABLE shaneud1.arnold_roads AS (
	SELECT *
 	FROM arnold.wapr_linestring
	WHERE geom && st_setsrid( st_makebox2d( st_makepoint(-13616323, 6049894), st_makepoint(-13615733, 6050671)), 3857)
	ORDER BY objectid, routeid
);



----------PROCESS----------


-- segmented roads --
-- arnold gave us road geometries which varied in length but for the most part, the roads were very long, so to better
-- associate roads to sidewalks (because one road can have many many sidewalks) we segmented the roads by road intersection
CREATE TABLE shaneud1.arnold_road_break AS
	WITH intersection_points AS (
		SELECT DISTINCT
			road1.objectid AS r1oid, 
			road1.routeid AS r1rid, 
			ST_Intersection(road1.geom, road2.geom) AS geom
		FROM shaneud1.arnold_roads AS road1
		JOIN shaneud1.arnold_roads AS road2
			ON ST_Intersects(road1.geom, road2.geom) AND road1.objectid != road2.objectid 
	)
	SELECT
		arud1.objectid, 
		arud1.og_objectid,
		arud1.routeid, 
		ST_collect(ip.geom), 
		ST_Split(arud1.geom, ST_collect(ip.geom))
	FROM shaneud1.arnold_roads AS arud1
	JOIN intersection_points AS ip 
		ON arud1.objectid = ip.r1oid AND arud1.routeid = ip.r1rid
	GROUP BY
		arud1.objectid,
		arud1.og_objectid,
		arud1.routeid,
		arud1.geom;
-- this makes a collection of linestrings and does not give us single segment linestrings
-- so, we make this table to dump the collection into individual linestrings for each segment
CREATE TABLE shaneud1.arnold_road_segments AS
	SELECT
		objectid,
		og_objectid,
		routeid, 
		(ST_Dump(st_split)).geom::geometry(LineString, 3857) AS geom
	FROM shaneud1.arnold_road_break;


-- big case--
-- makes the table associating sidewalks with roads in arnold based on buffer, angle (parallel), and midpoint distance.
-- this conflates all sidewalks over 10 meters to road segments so long it passes 2 tests
	-- 1: the angle of the sidewalk is parallel to the angle of the road segment
	-- 2: a sidewalk buffer of 2 intersects with a sidewalk buffer of 15
-- we then have the case where still 2 road segments pass the first 2 tests. In this case, we rank the sidewalks based on midpoint distance
CREATE TABLE sconud1.big_case AS
	WITH ranked_roads AS (
  		SELECT
    		sw.osm_id AS osmid,
    		road.og_objectid AS arnold_objectid,
    		road.routeid AS arnold_routeid,
  			sw.geom AS sw_geom,
    		road.geom AS road_geom,
    		ABS(DEGREES(ST_Angle(road.geom, sw.geom))) AS angle_degree,
    		ST_Distance(ST_LineInterpolatePoint(road.geom, 0.5), 
    		ST_LineInterpolatePoint(sw.geom, 0.5)) AS midpoint_distance,
    		-- rank this based on the distance of the midpoint of the sidewalk to the midpoint of the road
    		ROW_NUMBER() OVER (PARTITION BY sw.geom ORDER BY ST_Distance(ST_LineInterpolatePoint(road.geom, 0.5), ST_LineInterpolatePoint(sw.geom, 0.5)) ) AS RANK
  		FROM shaneud1.osm_sw AS sw
  		JOIN shaneud1.arnold_road_segments AS road
  			ON ST_Intersects(ST_Buffer(sw.geom, 2), ST_Buffer(road.geom, 15))  -- is there a better number?
  		WHERE (
			ABS(DEGREES(ST_Angle(road.geom, sw.geom))) BETWEEN 0 AND 10 			-- 0
    		OR ABS(DEGREES(ST_Angle(road.geom, sw.geom))) BETWEEN 170 AND 190 	-- 180
    		OR ABS(DEGREES(ST_Angle(road.geom, sw.geom))) BETWEEN 350 AND 360) 	-- 360
   		AND ( ST_length(sw.geom) > 10 ) -- IGNORE sidewalk that ARE shorter than 10 meters
	)
-- pulls only top ranked sidewalks with the lowest midpoint distance
	SELECT
		'sidewalk' AS LABEL,
  		osmid,
  		arnold_objectid,
  		arnold_routeid,
  		sw_geom,
  		road_geom
	FROM ranked_roads
	WHERE rank = 1;

-- creates a table for edges, cases where sidewalks meet at a corner without connecting
CREATE TABLE sconud1.corners AS
	SELECT
		sw.osm_id AS osm_id,
		'corner' AS LABEL,
		road1.road_routeid AS road1,
		road2.road_routeid AS st2_routeid,
		sw.geom AS osm_geom
	FROM shaneud1.osm_sw AS sw
	JOIN sconud1.big_case AS sw1
		ON st_intersects(st_startpoint(sw.geom), road1.sidewalk_geom)
	JOIN sconud1.big_case AS sw2
		ON st_intersects(st_endpoint(sw.geom), road2.sidewalk_geom)
	WHERE sw.geom NOT IN (
		SELECT bc.sidewalk_geom
		FROM conflation.big_case AS bc
	) 
	AND (road1.road_routeid <> road2.road_routeid);






















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
