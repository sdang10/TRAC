---- DATABASE SETUP ----


--- OSM ---
-- planet_osm_line
-- planet_osm_point
-- planet_osm_polygon
-- planet_osm_roads
-- spatial_ref_sys

-- we need to change 'way' to 'geom' column name

CREATE TABLE shane.osm_lines AS (
	SELECT * 
	FROM public.planet_osm_line
)
ALTER TABLE shane.osm_lines RENAME COLUMN way TO geom;


CREATE TABLE shane.osm_points AS (
	SELECT * 
	FROM public.planet_osm_point
)
ALTER TABLE shane.osm_points RENAME COLUMN way TO geom;


CREATE TABLE shane.osm_polygons AS (
	SELECT * 
	FROM public.planet_osm_polygon
)
ALTER TABLE shane.osm_polygons RENAME COLUMN way TO geom;


CREATE TABLE shane.osm_roads AS (
	SELECT * 
	FROM public.planet_osm_roads
)
ALTER TABLE shane.osm_roads RENAME COLUMN way TO geom;


--- ARNOLD ---
-- wapr_hpms_subittal 

-- we need to change multilinestring into linestring
-- put spacers between words like objectid, beginmeasure -> object_id, begin_measure  etc.

CREATE TABLE shane.arnold_lines (
  	og_object_id INT8,
  	object_id SERIAL,
  	route_id VARCHAR(75),
  	begin_measure FLOAT8,
  	end_measure FLOAT8,
  	shape_length FLOAT8,
  	geom geometry(linestring, 3857),
  	shape geometry(multilinestringm, 3857)
);

INSERT INTO shane.arnold_lines (og_object_id, route_id, begin_measure, end_measure, shape_length, geom, shape)
	SELECT  
		objectid, 
		routeid, 
		beginmeasure, 
		endmeasure, 
		shape_length, 
		ST_Force2D((ST_Dump(shape)).geom)::geometry(linestring, 3857),
		shape
	FROM arnold.wapr_hpms_submittal;




---- BOUNDING BOXES ----


--- U-District 1 ---

-- OSM sidewalks --
CREATE TABLE shane.ud1_osm_sw AS (
	SELECT *
	FROM shane.osm_lines
	WHERE	highway = 'footway'
		AND tags -> 'footway' = 'sidewalk' -- ONLY footway = sidewalk
		AND geom && st_setsrid( st_makebox2d( st_makepoint(-13616323, 6049894), st_makepoint(-13615733, 6050671)), 3857)
);

-- OSM points --
CREATE TABLE shane.ud1_osm_point AS (
	SELECT *
	FROM shane.osm_points
	WHERE geom && st_setsrid( st_makebox2d( st_makepoint(-13616323, 6049894), st_makepoint(-13615733, 6050671) ), 3857)
);

-- OSM crossing -- 
CREATE TABLE shane.ud1_osm_crossing AS (
	SELECT *
	FROM shane.osm_lines
	WHERE   highway = 'footway'
		AND tags -> 'footway' = 'crossing' -- ONLY footway = crossing
		AND geom && st_setsrid( st_makebox2d( st_makepoint(-13616323, 6049894), st_makepoint(-13615733, 6050671) ), 3857)
);

-- ARNOLD roads --
CREATE TABLE shane.ud1_arnold_lines AS (
	SELECT *
 	FROM shane.arnold_lines
	WHERE geom && st_setsrid( st_makebox2d( st_makepoint(-13616323, 6049894), st_makepoint(-13615733, 6050671)), 3857)
);




---- PROCESS ----


-- segmented roads --
-- arnold gave us road geometries which varied in length but for the most part, the roads were very long, so to better
-- associate roads to sidewalks (because one road can have many many sidewalks) we segmented the roads by road intersection
CREATE TABLE shane.ud1_arnold_segment_collections AS
	WITH intersection_points AS (
		SELECT DISTINCT
			road1.object_id AS road1_object_id, 
			road1.route_id AS road1_route_id, 
			ST_Intersection(road1.geom, road2.geom) AS geom
		FROM shane.ud1_arnold_lines AS road1
		JOIN shane.ud1_arnold_lines AS road2
			ON ST_Intersects(road1.geom, road2.geom) AND road1.object_id != road2.object_id 
	)
	SELECT
		ud1_arnold_lines.og_object_id, 
		ud1_arnold_lines.object_id,
		ud1_arnold_lines.route_id,
		ud1_arnold_lines.begin_measure,
		ud1_arnold_lines.end_measure,
		ud1_arnold_lines.shape_length,
		ud1_arnold_lines.geom,
		ud1_arnold_lines.shape,
		ST_Collect(intersection_points.geom), 
		ST_Split(ud1_arnold_lines.geom, ST_Collect(intersection_points.geom))
	FROM shane.ud1_arnold_lines AS ud1_arnold_lines
	JOIN intersection_points AS intersection_points 
		ON ud1_arnold_lines.object_id = intersection_points.road1_object_id AND ud1_arnold_lines.route_id = intersection_points.road1_route_id
	GROUP BY
		ud1_arnold_lines.og_object_id,
		ud1_arnold_lines.object_id,
		ud1_arnold_lines.route_id,
		ud1_arnold_lines.begin_measure,
	  	ud1_arnold_lines.end_measure,
	  	ud1_arnold_lines.shape_length,
		ud1_arnold_lines.geom,
		ud1_arnold_lines.shape;
	
-- this makes a collection of linestrings and does not give us single segment linestrings
-- so, we make this table to dump the collection into individual linestrings for each segment
CREATE TABLE shane.ud1_arnold_segments AS
	SELECT
		og_object_id,
		object_id,
		route_id,
		begin_measure,
	  	end_measure,
	  	shape_length,
		(ST_Dump(st_split)).geom::geometry(LineString, 3857) AS geom,
		shape
	FROM shane.ud1_arnold_segment_collections;

SELECT * FROM shane.ud1_arnold_segments


-- general case--
-- makes the table associating sidewalks with roads in arnold based on buffer, angle (parallel), and midpoint distance.
-- this conflates all sidewalks over 10 meters to road segments so long it passes 2 tests
	-- 1: the angle of the sidewalk is parallel to the angle of the road segment
	-- 2: a sidewalk buffer of 2 intersects with a sidewalk buffer of 15
-- we then have the case where still 2 road segments pass the first 2 tests. In this case, we rank the sidewalks based on midpoint distance
CREATE TABLE shane.ud1_conflation_general_case AS
	WITH ranked_roads AS (
  		SELECT
    		sw.osm_id AS osm_id,
  			sw.geom AS osm_geom,
  			road.og_object_id AS arnold_object_id,
    		road.geom AS arnold_geom,
    		road.shape AS arnold_shape,
    		ABS(DEGREES(ST_Angle(road.geom, sw.geom))) AS angle_degree,
    		ST_Distance(ST_LineInterpolatePoint(road.geom, 0.5), 
    		ST_LineInterpolatePoint(sw.geom, 0.5)) AS midpoint_distance,
    		-- rank this based on the distance of the midpoint of the sidewalk to the midpoint of the road
    		ROW_NUMBER() OVER (PARTITION BY sw.geom ORDER BY ST_Distance(ST_LineInterpolatePoint(road.geom, 0.5), ST_LineInterpolatePoint(sw.geom, 0.5)) ) AS RANK
  		FROM shane.ud1_osm_sw AS sw
  		JOIN shane.ud1_arnold_segments AS road
  			ON ST_Intersects(ST_Buffer(sw.geom, 2), ST_Buffer(road.geom, 15))  -- is there a better number?
  		WHERE (
			ABS(DEGREES(ST_Angle(road.geom, sw.geom))) BETWEEN 0 AND 10 			-- 0
    		OR ABS(DEGREES(ST_Angle(road.geom, sw.geom))) BETWEEN 170 AND 190 	-- 180
    		OR ABS(DEGREES(ST_Angle(road.geom, sw.geom))) BETWEEN 350 AND 360) 	-- 360
   		AND ( ST_length(sw.geom) > 10 ) -- IGNORE sidewalk that ARE shorter than 10 meters
	)
-- pulls only top ranked sidewalks with the lowest midpoint distance
	SELECT
		'sidewalk' AS osm_label,
  		osm_id,
  		osm_geom,
  		arnold_object_id,
  		arnold_geom,
  		arnold_shape
	FROM ranked_roads
	WHERE rank = 1;

-- checkpoint --
SELECT * FROM shane.ud1_arnold_segments
SELECT * FROM shane.ud1_conflation_general_case

SELECT *
FROM shane.ud1_arnold_lines
WHERE shape NOT IN (
	SELECT shape FROM shane.ud1_conflation_general_case
);










-- creates a table for edges, cases where there is a sidewalk link connecting 2 sidewalks that meet at a corner without connecting
CREATE TABLE shane.ud1_conflation_edge_case (
	osm_label TEXT,
    osm_id INT8,
    osm_geom GEOMETRY(LineString, 3857),
    arnold_road1_id INT8,
    arnold_road1_geom GEOMETRY(LineString, 3857),
    arnold_road1_shape GEOMETRY(MultilinestringM, 3857),
    arnold_road2_id INT8,
    arnold_road2_geom GEOMETRY(LineString, 3857),
    arnold_road2_shape GEOMETRY(MultilinestringM, 3857)
);


-- inserts values into the table as edges if they intersct with 2 different sidewalks from general case on both endpoints and edge is not in general case
INSERT INTO shane.ud1_conflation_edge_case (osm_label, osm_id, osm_geom, arnold_road1_id, arnold_road1_geom, arnold_road1_shape, arnold_road2_id, arnold_road2_geom, arnold_road2_shape)
SELECT
	'edge',
    sw.osm_id,
    sw.geom AS osm_geom,
    general_case1.arnold_object_id AS arnold_road1_id,
    general_case1.arnold_geom AS arnold_road1_geom,
    general_case1.arnold_shape AS arnold_road1_shape, 
    general_case2.arnold_object_id AS arnold_road2_id,
    general_case2.arnold_geom AS arnold_road2_geom,
    general_case2.arnold_shape AS arnold_road2_shape
FROM shane.ud1_osm_sw AS sw
JOIN shane.ud1_conflation_general_case AS general_case1
    ON ST_Intersects(ST_StartPoint(sw.geom), general_case1.osm_geom)
JOIN shane.ud1_conflation_general_case AS general_case2
    ON ST_Intersects(ST_EndPoint(sw.geom), general_case2.osm_geom)
WHERE sw.geom NOT IN (
	SELECT osm_geom
	FROM shane.ud1_conflation_general_case
) AND (
	general_case1.osm_id <> general_case2.osm_id);

   
-- checkpoint --
-- NOTE: the edges that show furthst north are not connected to street on top side because of bbox cut-off
SELECT * FROM shane.ud1_conflation_edge_case

SELECT *
FROM shane.osm_sw
WHERE osm_id NOT IN (
    SELECT osm_id FROM shane.general_case
) AND osm_id NOT IN (
    SELECT osm_id FROM shane.edge_case
);

-- check bbox cut-off  here
SELECT * FROM shane.osm_sw

















-- creating table for entrances. this is the case when it is an exception to general case and edge case and we have classified them as entrances from
-- being smaller than 10 meters, and only intersecting with a general case sidewalk on one side. We also check to see if the sidewalk segment intersects with
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
FROM shane.osm_sw sw
JOIN (
	SELECT *
	FROM shane.osm_point 
	WHERE tags -> 'entrance' IS NOT NULL 
		OR tags -> 'wheelchair' IS NOT NULL 
) AS point 
ON ST_intersects(sw.geom, point.geom);
	
-- create the table	
CREATE TABLE shane.entrance_case (
	LABEL TEXT,
	osm_sw_id INT8,
	osm_point_id INT8,
	sw_tags hstore,
	point_tags hstore,
	osm_sw_geom GEOMETRY(LineString, 3857),
	osm_point_geom GEOMETRY(Point, 3857)
);

INSERT INTO shane.entrance_case (LABEL, osm_sw_id, osm_point_id, sw_tags, point_tags, osm_sw_geom, osm_point_geom)
	SELECT 
		'entrance',
		sw.osm_id AS osm_sw_id, 
		point.osm_id AS osm_point_id,
		sw.tags AS sw_tags,
		point.tags AS point_tags,
		sw.geom AS osm_sw_geom,
		point.geom AS osm_point_geom
	FROM shane.osm_sw AS sw
	JOIN (
		SELECT *
		FROM shane.osm_point AS point
		WHERE tags -> 'entrance' IS NOT NULL 
			OR tags -> 'wheelchair' IS NOT NULL 
	) AS point 
		ON ST_intersects(sw.geom, point.geom)
	WHERE sw.osm_id NOT IN (
		SELECT osm_id
		FROM shane.general_case
		UNION ALL
		SELECT osm_id
		FROM shane.edge_case
	);

-- check our table
SELECT * FROM shane.entrance_case

-- check what we have left
SELECT *
FROM shane.osm_sw
WHERE osm_id NOT IN (
    SELECT osm_id FROM shane.general_case
) AND osm_id NOT IN (
    SELECT osm_id FROM shane.edge_case
) AND osm_id NOT IN (
	SELECT osm_sw_id FROM shane.entrance_case
);
-- what we have left are primarily connecting links and special case sidewalk data


-- first we will conflate crossing to roads
-- checks
SELECT * FROM shane.osm_crossing

SELECT *
FROM shane.osm_sw
WHERE osm_id NOT IN (
    SELECT osm_id FROM shane.general_case
) AND osm_id NOT IN (
    SELECT osm_id FROM shane.edge_case
) AND osm_id NOT IN (
	SELECT osm_sw_id FROM shane.entrance_case
) AND ST_Length(geom) < 10; -- this is filtering the 2 special cases

-- observation: bbox cut-off -> some crossing wont have connecting links
SELECT * FROM shane.osm_sw


-- creating the table for crossing
CREATE TABLE shane.crossing_case (
	LABEL TEXT,
	osm_id INT8,
	arnold_road_id INT8,
	arnold_route_id VARCHAR(75),
	osm_crossing_geom GEOMETRY(LineString, 3857),
	arnold_road_geom GEOMETRY(LineString, 3857)
);

-- inserts after conflating crossing to roads based on intersection
INSERT INTO shane.crossing_case (LABEL, osm_id, arnold_road_id, arnold_route_id, osm_crossing_geom, arnold_road_geom)
	SELECT 
		'crossing',
		crossing.osm_id AS osm_crossing_id,
		road.og_object_id AS arnold_road_id, 
		road.route_id AS arnold_route_id,
		crossing.geom AS osm_crossing_geom,
		road.geom AS arnold_road_geom
	FROM shane.osm_crossing AS crossing
	JOIN shane.arnold_road_segments AS road ON ST_Intersects(crossing.geom, road.geom);
-- 1/60 crossing was not conflated due to not intersecting with a road

-- check 
-- NOTE: the special crossing case is the only one in this bbox that has 'no' under access column, NULL otherwise
SELECT *
FROM shane.osm_crossing
WHERE osm_id NOT IN (
    SELECT osm_id FROM shane.crossing_case
);

SELECT *
FROM shane.osm_sw AS sw
WHERE osm_id NOT IN (
    SELECT osm_id FROM shane.general_case
) 
AND osm_id NOT IN (
    SELECT osm_id FROM shane.edge_case
) 
AND osm_id NOT IN (
    SELECT osm_sw_id FROM shane.entrance_case
)
AND EXISTS (
    SELECT 1
    FROM shane.osm_crossing AS crossing
    WHERE ST_Intersects(sw.geom, crossing.geom)
)
AND ST_Length(sw.geom) < 10;










-- from there we can associate acrossing link to the road the crossing is conflated to
	-- note: we don't associate crossing link to sidewalk road bc a connecting link is vulnerable to be connected to more than 1 sidewalk
	-- we really only want a crossing link to be a









