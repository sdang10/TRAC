-- WHAT TO FIX, WE NEED TO INCORPORATE START ADN END POINTS AND MEASURE FROM ARNOLD AND FIGURE OUT MULTILINESTRING IN ORDER TO CONFLATE

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
-- the og_object_id column keps track of the arnold object id when converting. 
CREATE TABLE shaneud1.arnold_wapr (
 	object_id SERIAL PRIMARY KEY,
  	og_object_id INT8,
  	route_id VARCHAR(75),
  	beginmeasure FLOAT8,
  	endmeasure FLOAT8,
  	shape_length FLOAT8,
  	geom geometry(linestring, 3857)
);

-- converting the MultiLineString geometries into LineString geometries
INSERT INTO shaneud1.arnold_wapr (og_object_id, route_id, beginmeasure, endmeasure, shape_length, geom)
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
 	FROM shaneud1.arnold_wapr
	WHERE geom && st_setsrid( st_makebox2d( st_makepoint(-13616323, 6049894), st_makepoint(-13615733, 6050671)), 3857)
	ORDER BY object_id, route_id
);



----------PROCESS----------


-- segmented roads --
-- arnold gave us road geometries which varied in length but for the most part, the roads were very long, so to better
-- associate roads to sidewalks (because one road can have many many sidewalks) we segmented the roads by road intersection
CREATE TABLE shaneud1.arnold_road_break AS
	WITH intersection_points AS (
		SELECT DISTINCT
			road1.object_id AS r1oid, 
			road1.route_id AS r1rid, 
			ST_Intersection(road1.geom, road2.geom) AS geom
		FROM shaneud1.arnold_roads AS road1
		JOIN shaneud1.arnold_roads AS road2
			ON ST_Intersects(road1.geom, road2.geom) AND road1.object_id != road2.object_id 
	)
	SELECT
		arud1.object_id, 
		arud1.og_object_id,
		arud1.route_id, 
		ST_collect(ip.geom), 
		ST_Split(arud1.geom, ST_collect(ip.geom))
	FROM shaneud1.arnold_roads AS arud1
	JOIN intersection_points AS ip 
		ON arud1.object_id = ip.r1oid AND arud1.route_id = ip.r1rid
	GROUP BY
		arud1.object_id,
		arud1.og_object_id,
		arud1.route_id,
		arud1.geom;
-- this makes a collection of linestrings and does not give us single segment linestrings
-- so, we make this table to dump the collection into individual linestrings for each segment
CREATE TABLE shaneud1.arnold_road_segments AS
	SELECT
		object_id,
		og_object_id,
		route_id, 
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
    		sw.osm_id AS osm_id,
    		road.og_object_id AS arnold_object_id,
    		road.route_id AS arnold_route_id,
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
  		osm_id,
  		arnold_object_id,
  		arnold_route_id,
  		sw_geom,
  		road_geom
	FROM ranked_roads
	WHERE rank = 1;


-- checking what we have left
SELECT *
FROM shaneud1.osm_sw
WHERE osm_id NOT IN (
	SELECT osm_id FROM sconud1.big_case
);


-- creates a table for edges, cases where there is a sidewalk link connecting 2 sidewalks that meet at a corner without connecting
CREATE TABLE sconud1.edge_case (
	LABEL TEXT,
    osm_id INT8,
    arnold_road1_id INT8,
    arnold_road2_id INT8,
    osm_geom GEOMETRY(LineString, 3857),
    arnold_road1_geom GEOMETRY(LineString, 3857),
    arnold_road2_geom GEOMETRY(LineString, 3857)
);


-- inserts values into the table as edges if they intersct with 2 different sidewalks from big case on both endpoints and edge is not in big case
INSERT INTO sconud1.edge_case (LABEL, osm_id, arnold_road1_id, arnold_road2_id, osm_geom, arnold_road1_geom, arnold_road2_geom)
SELECT
	'edge',
    sw.osm_id AS osm_id,
    big_case1.arnold_object_id AS arnold_road1_id,
    big_case2.arnold_object_id AS arnold_road2_id,
    sw.geom AS osm_geom,
    big_case1.road_geom AS arnold_road1_geom,
    big_case2.road_geom AS arnold_road2_geom
FROM shaneud1.osm_sw AS sw
JOIN sconud1.big_case AS big_case1
    ON ST_Intersects(ST_StartPoint(sw.geom), big_case1.sw_geom)
JOIN sconud1.big_case AS big_case2
    ON ST_Intersects(ST_EndPoint(sw.geom), big_case2.sw_geom)
WHERE sw.geom NOT IN (
	SELECT big_case.sw_geom
	FROM sconud1.big_case AS big_case
) AND (
	big_case1.osm_id <> big_case2.osm_id);

   
-- check our table
SELECT * FROM sconud1.edge_case
   

-- checking what we have left
-- note: the edges that show furthst north are not connected to street on top side because of bbox cut-off
SELECT *
FROM shaneud1.osm_sw
WHERE osm_id NOT IN (
    SELECT osm_id FROM sconud1.big_case
) AND osm_id NOT IN (
    SELECT osm_id FROM sconud1.edge_case
);

-- check bbox cut-off  here
SELECT * FROM shaneud1.osm_sw


-- creating table for entrances. this is the case when it is an exception to big case and edge case and we have classified them as entrances from
-- being smaller than 10 meters, and only intersecting with a big case sidewalk on one side. We also check to see if the sidewalk segment intersects with
-- a point from osm points that holds a tag with "entrance" 
-- NOTE: for now this does not conflate with anything, but we make this special case to define and justify it's reasoning to classify and move on from these "sidewalk" data

-- check for intersection of points and entrances
SELECT 
	sw.osm_id AS osm_sw_id, 
	point.osm_id AS osm_point_id,
	sw.tags AS sw_tags,
	point.tags AS point_tags,
	sw.geom AS osm_sw_geom,
	point.geom AS osm_point_geom
FROM shaneud1.osm_sw sw
JOIN (
	SELECT *
	FROM shaneud1.osm_point 
	WHERE tags -> 'entrance' IS NOT NULL 
		OR tags -> 'wheelchair' IS NOT NULL 
) AS point 
ON ST_intersects(sw.geom, point.geom);
	
-- create the table	
CREATE TABLE sconud1.entrance_case (
	LABEL TEXT,
	osm_sw_id INT8,
	osm_point_id INT8,
	sw_tags hstore,
	point_tags hstore,
	osm_sw_geom GEOMETRY(LineString, 3857),
	osm_point_geom GEOMETRY(Point, 3857)
);

INSERT INTO sconud1.entrance_case (LABEL, osm_sw_id, osm_point_id, sw_tags, point_tags, osm_sw_geom, osm_point_geom)
	SELECT 
		'entrance',
		sw.osm_id AS osm_sw_id, 
		point.osm_id AS osm_point_id,
		sw.tags AS sw_tags,
		point.tags AS point_tags,
		sw.geom AS osm_sw_geom,
		point.geom AS osm_point_geom
	FROM shaneud1.osm_sw AS sw
	JOIN (
		SELECT *
		FROM shaneud1.osm_point AS point
		WHERE tags -> 'entrance' IS NOT NULL 
			OR tags -> 'wheelchair' IS NOT NULL 
	) AS point 
		ON ST_intersects(sw.geom, point.geom)
	WHERE sw.osm_id NOT IN (
		SELECT osm_id
		FROM sconud1.big_case
		UNION ALL
		SELECT osm_id
		FROM sconud1.edge_case
	);

-- check our table
SELECT * FROM sconud1.entrance_case

-- check what we have left
SELECT *
FROM shaneud1.osm_sw
WHERE osm_id NOT IN (
    SELECT osm_id FROM sconud1.big_case
) AND osm_id NOT IN (
    SELECT osm_id FROM sconud1.edge_case
) AND osm_id NOT IN (
	SELECT osm_sw_id FROM sconud1.entrance_case
);
-- what we have left are primarily connecting links and special case sidewalk data


-- first we will conflate crossing to roads
-- checks
SELECT * FROM shaneud1.osm_crossing

SELECT *
FROM shaneud1.osm_sw
WHERE osm_id NOT IN (
    SELECT osm_id FROM sconud1.big_case
) AND osm_id NOT IN (
    SELECT osm_id FROM sconud1.edge_case
) AND osm_id NOT IN (
	SELECT osm_sw_id FROM sconud1.entrance_case
) AND ST_Length(geom) < 10; -- this is filtering the 2 special cases

-- observation: bbox cut-off -> some crossing wont have connecting links
SELECT * FROM shaneud1.osm_sw


-- creating the table for crossing
CREATE TABLE sconud1.crossing_case (
	LABEL TEXT,
	osm_id INT8,
	arnold_road_id INT8,
	arnold_route_id VARCHAR(75),
	osm_crossing_geom GEOMETRY(LineString, 3857),
	arnold_road_geom GEOMETRY(LineString, 3857)
);

-- inserts after conflating crossing to roads based on intersection
INSERT INTO sconud1.crossing_case (LABEL, osm_id, arnold_road_id, arnold_route_id, osm_crossing_geom, arnold_road_geom)
	SELECT 
		'crossing',
		crossing.osm_id AS osm_crossing_id,
		road.og_object_id AS arnold_road_id, 
		road.route_id AS arnold_route_id,
		crossing.geom AS osm_crossing_geom,
		road.geom AS arnold_road_geom
	FROM shaneud1.osm_crossing AS crossing
	JOIN shaneud1.arnold_road_segments AS road ON ST_Intersects(crossing.geom, road.geom);
-- 1/60 crossing was not conflated due to not intersecting with a road

-- check 
-- NOTE: the special crossing case is the only one in this bbox that has 'no' under access column, NULL otherwise
SELECT *
FROM shaneud1.osm_crossing
WHERE osm_id NOT IN (
    SELECT osm_id FROM sconud1.crossing_case
);

SELECT *
FROM shaneud1.osm_sw AS sw
WHERE osm_id NOT IN (
    SELECT osm_id FROM sconud1.big_case
) 
AND osm_id NOT IN (
    SELECT osm_id FROM sconud1.edge_case
) 
AND osm_id NOT IN (
    SELECT osm_sw_id FROM sconud1.entrance_case
)
AND EXISTS (
    SELECT 1
    FROM shaneud1.osm_crossing AS crossing
    WHERE ST_Intersects(sw.geom, crossing.geom)
)
AND ST_Length(sw.geom) < 10;










-- from there we can associate acrossing link to the road the crossing is conflated to
	-- note: we don't associate crossing link to sidewalk road bc a connecting link is vulnerable to be connected to more than 1 sidewalk
	-- we really only want a crossing link to be a


