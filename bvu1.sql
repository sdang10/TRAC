--- Bellevue 1 ---


---- BOUNDING BOXES ----



-- OSM sidewalks --
CREATE TABLE shane.bvu1_osm_sw AS (
	SELECT *
	FROM shane.osm_lines
	WHERE	highway = 'footway'
		AND tags -> 'footway' = 'sidewalk' -- ONLY footway = sidewalk
		AND geom && st_setsrid( st_makebox2d( st_makepoint(-13603442,6043723), st_makepoint(-13602226,6044848)), 3857)
); -- count: 255

-- OSM points --
CREATE TABLE shane.bvu1_osm_point AS (
	SELECT *
	FROM shane.osm_points
	WHERE geom && st_setsrid( st_makebox2d( st_makepoint(-13603442,6043723), st_makepoint(-13602226,6044848)), 3857)
); 

-- OSM crossings -- 
CREATE TABLE shane.bvu1_osm_crossing AS (
	SELECT *
	FROM shane.osm_lines
	WHERE   highway = 'footway'
		AND tags -> 'footway' = 'crossing' -- ONLY footway = crossing
		AND geom && st_setsrid( st_makebox2d( st_makepoint(-13603442,6043723), st_makepoint(-13602226,6044848)), 3857)
); -- count: 114

-- ARNOLD roads --
CREATE TABLE shane.bvu1_arnold_lines AS (
	SELECT *
 	FROM shane.arnold_lines
	WHERE geom && st_setsrid( st_makebox2d( st_makepoint(-13603442,6043723), st_makepoint(-13602226,6044848)), 3857)
); -- count: 41

-- OSM footway NULL --
CREATE TABLE shane.bvu1_osm_footway_null AS (
	SELECT *
	FROM shane.osm_lines
	WHERE 	highway = 'footway' AND 
			(tags -> 'footway' IS NULL OR 
			tags -> 'footway' NOT IN ('sidewalk', 'crossing'))  AND
			geom && st_setsrid( st_makebox2d( st_makepoint(-13603442,6043723), st_makepoint(-13602226,6044848)), 3857)
); -- count: 270

--- OSM connecting links ---
CREATE TABLE shane.bvu1_osm_connecting_links AS (
	SELECT DISTINCT sw.*
	FROM shane.bvu1_osm_sw AS sw
	JOIN shane.bvu1_osm_crossing AS crossing
    ON ST_Intersects(sw.geom, crossing.geom)
    WHERE ST_Length(sw.geom) < 12
) UNION
	SELECT DISTINCT fn.*
	FROM shane.bvu1_osm_footway_null AS fn
	JOIN shane.bvu1_osm_crossing AS crossing
    	ON ST_Intersects(fn.geom, crossing.geom)
	WHERE ST_Length(fn.geom) < 12; -- count: 134

SELECT * FROM shane.bvu1_osm_connecting_links


---- PROCESS ----


--- segmented roads ---
-- arnold gave us road geometries which varied in length but for the most part, the roads were very long, so to better
-- associate roads to sidewalks (because one road can have many many sidewalks) we segmented the roads by road intersection
CREATE TABLE shane.bvu1_arnold_segment_collections AS
	WITH intersection_points AS (
		SELECT DISTINCT 
			road1.object_id AS road1_object_id, 
			road1.route_id AS road1_route_id, 
	  		(ST_DumpPoints(ST_Intersection(road1.geom, road2.geom))).geom AS geom
		FROM shane.bvu1_arnold_lines AS road1
		JOIN shane.bvu1_arnold_lines AS road2
			ON ST_Intersects(road1.geom, road2.geom) AND ST_Equals(road1.geom, road2.geom) IS FALSE
	)
	SELECT
	  	bvu1_arnold_lines.og_object_id, 
		bvu1_arnold_lines.object_id,
		bvu1_arnold_lines.route_id,
		bvu1_arnold_lines.begin_measure,
		bvu1_arnold_lines.end_measure,
		bvu1_arnold_lines.shape_length,
		bvu1_arnold_lines.geom,
		bvu1_arnold_lines.shape,
		ST_collect(intersection_points.geom),
		ST_Split(bvu1_arnold_lines.geom, ST_Collect(intersection_points.geom))
	FROM shane.bvu1_arnold_lines AS bvu1_arnold_lines
	JOIN intersection_points AS intersection_points 
		ON bvu1_arnold_lines.object_id = intersection_points.road1_object_id 
	GROUP BY
		bvu1_arnold_lines.og_object_id,
		bvu1_arnold_lines.object_id,
		bvu1_arnold_lines.route_id,
		bvu1_arnold_lines.begin_measure,
	  	bvu1_arnold_lines.end_measure,
	  	bvu1_arnold_lines.shape_length,
		bvu1_arnold_lines.geom,
		bvu1_arnold_lines.shape;
	
	
-- this makes a collection of linestrings and does not give us single segment linestrings
-- so, we make this table to dump the collection into individual linestrings for each segment
CREATE TABLE shane.bvu1_arnold_segments AS
	SELECT
		og_object_id,
		object_id,
		route_id,
		begin_measure,
	  	end_measure,
	  	shape_length,
		(ST_Dump(st_split)).geom::geometry(LineString, 3857) AS geom,
		shape
	FROM shane.bvu1_arnold_segment_collections;

-- checkpoint --
SELECT * FROM shane.bvu1_arnold_segments




--- general case ---

-- makes the table associating sidewalks with roads in arnold based on buffer, angle (parallel), and midpoint distance.
-- this conflates all sidewalks over 10 meters to road segments so long it passes 2 tests
	-- 1: the angle of the sidewalk is parallel to the angle of the road segment
	-- 2: a sidewalk buffer of 2 intersects with a sidewalk buffer of 15
-- we then have the case where still 2 road segments pass the first 2 tests. In this case, we rank the sidewalks based on midpoint distance
CREATE TABLE shane.bvu1_conflation_general_case AS
	WITH ranked_roads AS (
		SELECT
			sw.osm_id AS osm_id,
  			sw.geom AS osm_geom,
  			road.og_object_id AS arnold_object_id,
    		road.geom AS arnold_geom,
    		road.shape AS arnold_shape,
		  	ST_LineSubstring( road.geom, LEAST(ST_LineLocatePoint(road.geom, ST_ClosestPoint(st_startpoint(sw.geom), road.geom)) , ST_LineLocatePoint(road.geom, ST_ClosestPoint(st_endpoint(sw.geom), road.geom))), GREATEST(ST_LineLocatePoint(road.geom, ST_ClosestPoint(st_startpoint(sw.geom), road.geom)) , ST_LineLocatePoint(road.geom, ST_ClosestPoint(st_endpoint(sw.geom), road.geom))) ) AS seg_geom,
		  	-- calculate the coverage of sidewalk within the buffer of the road
		  	ST_Length(ST_Intersection(sw.geom, ST_Buffer(ST_LineSubstring( road.geom, LEAST(ST_LineLocatePoint(road.geom, ST_ClosestPoint(st_endpoint(sw.geom), road.geom))), GREATEST(ST_LineLocatePoint(road.geom, ST_ClosestPoint(st_endpoint(sw.geom), road.geom))) ), 18))) / ST_Length(sw.geom) AS sw_coverage_bigroad,
		  	-- rank this based on the distance of the midpoint of the sidewalk to the midpoint of the road
		  	ROW_NUMBER() OVER (
		  		PARTITION BY sw.geom
		  		ORDER BY ST_distance( 
		  			ST_LineSubstring( road.geom,
		  			LEAST(ST_LineLocatePoint(road.geom, ST_ClosestPoint(st_startpoint(sw.geom), road.geom)) , ST_LineLocatePoint(road.geom, ST_ClosestPoint(st_endpoint(sw.geom), road.geom))),
		  			GREATEST(ST_LineLocatePoint(road.geom, ST_ClosestPoint(st_startpoint(sw.geom), road.geom)) , ST_LineLocatePoint(road.geom, ST_ClosestPoint(st_endpoint(sw.geom), road.geom))) ),
		  			sw.geom )
		  	) AS RANK
		FROM shane.bvu1_osm_sw AS sw
		JOIN shane.bvu1_arnold_lines AS road 
			ON ST_Intersects(ST_Buffer(sw.geom, 5), ST_Buffer(road.geom, 30))
		WHERE (
			ABS(DEGREES(ST_Angle(ST_LineSubstring( road.geom, LEAST(ST_LineLocatePoint(road.geom, ST_ClosestPoint(st_startpoint(sw.geom), road.geom)) , ST_LineLocatePoint(road.geom, ST_ClosestPoint(st_endpoint(sw.geom), road.geom))), GREATEST(ST_LineLocatePoint(road.geom, ST_ClosestPoint(st_startpoint(sw.geom), road.geom)) , ST_LineLocatePoint(road.geom, ST_ClosestPoint(st_endpoint(sw.geom), road.geom))) ), sw.geom))) BETWEEN 0 AND 10 -- 0 
			OR ABS(DEGREES(ST_Angle(ST_LineSubstring( road.geom, LEAST(ST_LineLocatePoint(road.geom, ST_ClosestPoint(st_startpoint(sw.geom), road.geom)) , ST_LineLocatePoint(road.geom, ST_ClosestPoint(st_endpoint(sw.geom), road.geom))), GREATEST(ST_LineLocatePoint(road.geom, ST_ClosestPoint(st_startpoint(sw.geom), road.geom)) , ST_LineLocatePoint(road.geom, ST_ClosestPoint(st_endpoint(sw.geom), road.geom))) ), sw.geom))) BETWEEN 170 AND 190 -- 180
			OR ABS(DEGREES(ST_Angle(ST_LineSubstring( road.geom, LEAST(ST_LineLocatePoint(road.geom, ST_ClosestPoint(st_startpoint(sw.geom), road.geom)) , ST_LineLocatePoint(road.geom, ST_ClosestPoint(st_endpoint(sw.geom), road.geom))), GREATEST(ST_LineLocatePoint(road.geom, ST_ClosestPoint(st_startpoint(sw.geom), road.geom)) , ST_LineLocatePoint(road.geom, ST_ClosestPoint(st_endpoint(sw.geom), road.geom))) ), sw.geom))) BETWEEN 350 AND 360 ) -- 360
			)
	SELECT DISTINCT
		'sidewalk' AS osm_label,
	  	osm_id,
	  	osm_geom,
	  	arnold_object_id,
	  	seg_geom AS arnold_geom,
	  	arnold_shape
	FROM
		ranked_roads
	WHERE
		rank = 1; -- count 184
		
		

--- checkpoint ---
		
SELECT * FROM shane.bvu1_conflation_general_case

SELECT * FROM shane.bvu1_osm_sw AS sw
WHERE (st_length(sw.geom) < 5)

SELECT * FROM shane.bvu1_osm_crossing

SELECT * FROM shane.bvu1_osm_sw

SELECT *
FROM shane.bvu1_arnold_lines
WHERE shape NOT IN (
	SELECT arnold_shape FROM shane.bvu1_conflation_general_case
); -- how many roads weren't used yet? count: 29

SELECT *
FROM shane.bvu1_osm_sw
WHERE osm_id NOT IN (
	SELECT osm_id FROM shane.bvu1_conflation_general_case
); -- how many have NOT been classified yet? count: 117

SELECT * FROM shane.bvu1_arnold_lines

SELECT * FROM shane.arnold_lines

-- checking beyond bbox to see if roads were cut off
SELECT *
FROM shane.arnold_lines
WHERE geom && st_setsrid( st_makebox2d( st_makepoint(-13604049,6043723), st_makepoint(-13602226,6044848)), 3857)

-- OBSERVATIONS: 
-- found this area lacks road data
	-- special cases where sw with no road to conflate to: 853044392, 862758193, 853044396, 853044399
-- cases where buffer is not big enough? : 521965206
-- incline = '': lowered curb for crossing/driveway/etc.
-- wheelchair = 'yes': wheelchair access

SELECT *
FROM shane.bvu1_osm_sw
WHERE osm_id IN (
	SELECT osm_id FROM shane.bvu1_conflation_general_case
); 

SELECT *
FROM shane.bvu1_osm_sw
WHERE osm_id IN (
	SELECT osm_id FROM shane.bvu1_conflation_general_case_test
); 



    
--- crossing case ---

-- pre-check
SELECT * FROM shane.bvu1_osm_crossing

-- what do we have left aside from special cases
SELECT *
FROM shane.bvu1_osm_sw
WHERE osm_id NOT IN (
    SELECT osm_id FROM shane.bvu1_conflation_general_case
) AND osm_id NOT IN (
    SELECT osm_id FROM shane.bvu1_conflation_edge_case
) AND osm_id NOT IN (
	SELECT osm_sw_id FROM shane.bvu1_conflation_entrance_case
) AND ST_Length(geom) < 8; -- this is filtering the 2 special cases (osm_id's: 475987407 & 490774566)

-- observation: bbox cut-off -> some crossing wont have connecting links
SELECT * FROM shane.osm_sw


-- creating the table for crossing
CREATE TABLE shane.bvu1_conflation_crossing_case (
	osm_label TEXT,
	osm_id INT8,
	osm_geom GEOMETRY(LineString, 3857),
	arnold_road_id INT8,
	arnold_road_geom GEOMETRY(LineString, 3857),
	arnold_road_shape GEOMETRY(MultilinestringM, 3857)
);

-- inserts after conflating crossing to roads based on intersection
INSERT INTO shane.bvu1_conflation_crossing_case (osm_label, osm_id, osm_geom, arnold_road_id, arnold_road_geom, arnold_road_shape)
	SELECT 
		'crossing' AS osm_label,
		crossing.osm_id AS osm_id, 
		crossing.geom AS osm_geom,
		road.og_object_id AS arnold_road_id,
		road.geom AS arnold_road_geom,
		road.shape AS arnold_road_shape
	FROM shane.bvu1_osm_crossing AS crossing
	JOIN shane.bvu1_arnold_segments AS road ON ST_Intersects(crossing.geom, road.geom); -- count 55

	

--- check point ---
	
-- NOTE: the special crossing case is the only one in this bbox that has 'no' under access column, NULL otherwise
SELECT * FROM shane.bvu1_conflation_crossing_case

SELECT *
FROM shane.bvu1_osm_crossing
WHERE osm_id NOT IN (
    SELECT osm_id FROM shane.bvu1_conflation_crossing_case
);




--- connecting link case ---

-- from there we can associate a crossing link to the road the crossing is conflated to
-- note: we don't associate crossing link to sidewalk road bc a connecting link is vulnerable to be connected to more than 1 sidewalk

CREATE TABLE shane.bvu1_conflation_connecting_link_case (
	osm_label TEXT,
	osm_cl_id INT8,
	osm_cl_geom GEOMETRY(LineString, 3857),
	osm_crossing_id INT8,
	osm_crossing_geom GEOMETRY(LineString, 3857),
	arnold_road_id INT8,
	arnold_road_geom GEOMETRY(LineString, 3857),
	arnold_road_shape GEOMETRY(MultilinestringM, 3857)
);


INSERT INTO shane.bvu1_conflation_connecting_link_case (osm_label, osm_cl_id, osm_cl_geom, osm_crossing_id, osm_crossing_geom, arnold_road_id, arnold_road_geom, arnold_road_shape)
SELECT
    'connecting link' AS osm_label,
    cl.osm_id AS osm_cl_id,
    cl.geom AS osm_cl_geom,
    crossing.osm_id AS osm_crossing_id,
    crossing.osm_geom AS osm_crossing_geom,
    crossing.arnold_road_id AS arnold_road_id,
    crossing.arnold_road_geom AS arnold_road_geom,
    crossing.arnold_road_shape AS arnold_road_shape
FROM shane.bvu1_osm_connecting_links AS cl
JOIN shane.bvu1_conflation_crossing_case AS crossing
    ON ST_Intersects(cl.geom, crossing.osm_geom)
WHERE ST_Length(cl.geom) < 8; -- count: 87


DELETE FROM shane.bvu1_conflation_general_case sw
WHERE sw.osm_id IN (
	SELECT bs.osm_id
	FROM shane.bvu1_conflation_general_case bs
	JOIN shane.bvu1_conflation_connecting_link_case link
	ON link.osm_cl_id = bs.osm_id 
);



--- checkpoint ---

SELECT *
FROM shane.bvu1_osm_connecting_links
WHERE osm_id IN (607810499, 607810496, 1074618891);

SELECT * FROM shane.bvu1_osm_connecting_links
WHERE osm_id NOT IN (
	SELECT osm_cl_id FROM shane.bvu1_conflation_connecting_link_case
)

SELECT * FROM shane.bvu1_osm_crossing

SELECT * FROM shane.bvu1_arnold_lines 

-- connecting link special case: 607,810,499 / 1,074,618,891 / 607,810,496
-- crossing special case: 67,144,865 / 1,074,618,890




--- edge case --- 

-- creates a table for edges, cases where there is a sidewalk link connecting 2 sidewalks that meet at a corner without connecting
CREATE TABLE shane.bvu1_conflation_edge_case (
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
INSERT INTO shane.bvu1_conflation_edge_case (osm_label, osm_id, osm_geom, arnold_road1_id, arnold_road1_geom, arnold_road1_shape, arnold_road2_id, arnold_road2_geom, arnold_road2_shape)
SELECT
	'edge'AS osm_label,
    sw.osm_id AS osm_id,
    sw.geom AS osm_geom,
    general_case1.arnold_object_id AS arnold_road1_id,
    general_case1.arnold_geom AS arnold_road1_geom,
    general_case1.arnold_shape AS arnold_road1_shape, 
    general_case2.arnold_object_id AS arnold_road2_id,
    general_case2.arnold_geom AS arnold_road2_geom,
    general_case2.arnold_shape AS arnold_road2_shape
FROM shane.bvu1_osm_sw AS sw
JOIN shane.bvu1_conflation_general_case AS general_case1
    ON ST_Intersects(ST_StartPoint(sw.geom), general_case1.osm_geom)
JOIN shane.bvu1_conflation_general_case AS general_case2
    ON ST_Intersects(ST_EndPoint(sw.geom), general_case2.osm_geom)
WHERE sw.geom NOT IN (
	SELECT osm_geom
	FROM shane.bvu1_conflation_general_case
) AND (
	general_case1.osm_id != general_case2.osm_id
) AND (
	general_case1.arnold_object_id != general_case2.arnold_object_id
); -- count: 19

  

-- checkpoint --

SELECT * FROM shane.bvu1_conflation_edge_case

-- what do we have left?
SELECT *
FROM shane.bvu1_osm_sw
WHERE osm_id NOT IN (
    SELECT osm_id FROM shane.bvu1_conflation_general_case
) AND osm_id NOT IN (
    SELECT osm_id FROM shane.bvu1_conflation_edge_case
);




--- entrance case ---

-- creating table for entrances. this is the case when it is an exception to general case and edge case and we have classified them as entrances from
-- being smaller than 10 meters, and only intersecting with a general case sidewalk on one side. We also check to see if the sidewalk segment intersects with
-- a point from osm points that holds a tag with "entrance" 
-- NOTE: for now this does not conflate with anything because we don't consider these entrances associated to any road, but we make this special case to define
-- and justify it's reasoning to classify and move on from these "sidewalk" data

CREATE TABLE shane.bvu1_conflation_entrance_case (
	osm_label TEXT,
	osm_sw_id INT8,
	osm_sw_geom GEOMETRY(LineString, 3857),
	osm_point_id INT8,
	point_tags hstore,
	osm_point_geom GEOMETRY(Point, 3857)
);

INSERT INTO shane.bvu1_conflation_entrance_case (osm_label, osm_sw_id, osm_sw_geom, osm_point_id, point_tags, osm_point_geom)
	SELECT 
		'entrance' AS osm_label,
		sw.osm_id AS osm_sw_id, 
		sw.geom AS osm_sw_geom,
		point.osm_id AS osm_point_id,
		point.tags AS point_tags,
		point.geom AS osm_point_geom
	FROM shane.bvu1_osm_sw AS sw
	JOIN (
		SELECT *
		FROM shane.bvu1_osm_point AS point
		WHERE tags -> 'entrance' IS NOT NULL
	) AS point 
		ON ST_intersects(sw.geom, point.geom)
	WHERE sw.osm_id NOT IN (
		SELECT osm_id
		FROM shane.bvu1_conflation_general_case
		UNION ALL
		SELECT osm_id
		FROM shane.bvu1_conflation_edge_case
		UNION ALL
		SELECT osm_cl_id
		FROM shane.bvu1_conflation_connecting_link_case
	); -- count: 1


--- checkpoint ---
SELECT * FROM shane.bvu1_conflation_entrance_case

-- check what we have left
SELECT *
FROM shane.bvu1_osm_sw
WHERE osm_id NOT IN (
    SELECT osm_id FROM shane.bvu1_conflation_general_case
) AND osm_id NOT IN (
    SELECT osm_id FROM shane.bvu1_conflation_edge_case
) AND osm_id NOT IN (
	SELECT osm_sw_id FROM shane.bvu1_conflation_entrance_case
) AND osm_id NOT IN (
	SELECT osm_cl_id FROM shane.bvu1_conflation_connecting_link_case
);

-- the remaining special cases, count: 6
-- the first 3 are due to bounding box cut-off
SELECT *
FROM shane.bvu1_osm_sw
WHERE osm_id NOT IN (
    SELECT osm_id FROM shane.bvu1_conflation_general_case
) AND osm_id NOT IN (
    SELECT osm_id FROM shane.bvu1_conflation_edge_case
) AND osm_id NOT IN (
	SELECT osm_sw_id FROM shane.bvu1_conflation_entrance_case
) AND osm_id NOT IN (
	SELECT osm_sw_id FROM shane.bvu1_conflation_connecting_link_case
);

-- checking which roads weren't used if any, count: 10
SELECT *
FROM shane.bvu1_arnold_lines
WHERE shape NOT IN (
    SELECT arnold_shape FROM shane.bvu1_conflation_general_case
) AND shape NOT IN (
    SELECT arnold_road1_shape FROM shane.bvu1_conflation_edge_case
) AND shape NOT IN (
	SELECT arnold_road2_shape FROM shane.bvu1_conflation_edge_case
) AND shape NOT IN (
	SELECT arnold_road_shape FROM shane.bvu1_conflation_connecting_link_case
);

