--- U-District 1 ---


---- BOUNDING BOXES ----



-- OSM sidewalks --
CREATE TABLE shane_ud1_sw.osm_sw AS (
	SELECT *
	FROM shane_data_setup.osm_lines
	WHERE	highway = 'footway'
		AND tags -> 'footway' = 'sidewalk' -- ONLY footway = sidewalk
		AND geom && st_setsrid( st_makebox2d( st_makepoint(-13616323, 6049894), st_makepoint(-13615733, 6050671)), 3857)
); -- count: 107

-- OSM points --
CREATE TABLE shane_ud1_sw.osm_point AS (
	SELECT *
	FROM shane_data_setup.osm_points
	WHERE geom && st_setsrid( st_makebox2d( st_makepoint(-13616323, 6049894), st_makepoint(-13615733, 6050671)), 3857)
); -- count: 740

-- OSM crossings -- 
CREATE TABLE shane_ud1_sw.osm_crossing AS (
	SELECT *
	FROM shane_data_setup.osm_lines
	WHERE   highway = 'footway'
		AND tags -> 'footway' = 'crossing' -- ONLY footway = crossing
		AND geom && st_setsrid( st_makebox2d( st_makepoint(-13616323, 6049894), st_makepoint(-13615733, 6050671)), 3857)
); -- count: 60

-- ARNOLD roads --
CREATE TABLE shane_ud1_sw.arnold_lines AS (
	SELECT *
 	FROM shane_data_setup.arnold_lines
	WHERE geom && st_setsrid( st_makebox2d( st_makepoint(-13616323, 6049894), st_makepoint(-13615733, 6050671)), 3857)
); -- count: 21

-- OSM footway NULL --
CREATE TABLE shane_ud1_sw.osm_footway_null AS (
	SELECT *
	FROM shane_data_setup.osm_lines
	WHERE 	highway = 'footway' AND 
			(tags -> 'footway' IS NULL OR 
			tags -> 'footway' NOT IN ('sidewalk', 'crossing'))  AND
			geom && st_setsrid( st_makebox2d( st_makepoint(-13616323, 6049894), st_makepoint(-13615733, 6050671)), 3857)
); -- count: 118





---- PROCESS ----


--- entrance case ---

-- defined as entrance if intersects with a point from osm points that has a tag of "entrance" 
-- NOTE: for now this does not conflate with anything (roads) because we don't consider these entrances associated to any road
-- we make this special case to define and justify it's reasoning to classify and move on from these "sidewalk" data in the conflation process

CREATE TABLE shane_ud1_sw.conflation_entrance_case (
	osm_label TEXT,
	osm_sw_id INT8,
	osm_sw_geom GEOMETRY(LineString, 3857),
	osm_point_id INT8,
	point_tags hstore,
	osm_point_geom GEOMETRY(Point, 3857)
);

INSERT INTO shane_ud1_sw.conflation_entrance_case (
	osm_label, 
	osm_sw_id, 
	osm_sw_geom, 
	osm_point_id, 
	point_tags, 
	osm_point_geom
) SELECT 
	'entrance' AS osm_label,
	sw.osm_id AS osm_sw_id, 
	sw.geom AS osm_sw_geom,
	point.osm_id AS osm_point_id,
	point.tags AS point_tags,
	point.geom AS osm_point_geom
FROM shane_ud1_sw.osm_sw AS sw
JOIN (
	SELECT *
	FROM shane_ud1_sw.osm_point AS point
	WHERE tags -> 'entrance' IS NOT NULL
) AS point 
	ON ST_intersects(sw.geom, point.geom); -- count: 6

		
			
		
--- crossing case ---
		
-- we want to conflate crossing next as we need these conflations for connecting links and crossing conflation does not depend on anything else
-- crossing is conflated based on intersection with the road. 

CREATE TABLE shane_ud1_sw.conflation_crossing_case (
	osm_label TEXT,
	osm_id INT8,
	osm_geom GEOMETRY(LineString, 3857),
	arnold_route_id VARCHAR(75),
	arnold_road_geom GEOMETRY(LineString, 3857)
);

INSERT INTO shane_ud1_sw.conflation_crossing_case (
	osm_label, 
	osm_id, 
	osm_geom, 
	arnold_route_id, 
	arnold_road_geom
)
SELECT 
	'crossing' AS osm_label,
	crossing.osm_id AS osm_id, 
	crossing.geom AS osm_geom,
	road.route_id AS arnold_route_id,
	road.geom AS arnold_road_geom
FROM shane_ud1_sw.osm_crossing AS crossing
JOIN shane_ud1_sw.arnold_lines AS road ON ST_Intersects(crossing.geom, road.geom); -- count 59


	
--- check point ---
	-- what's in
SELECT * FROM shane_ud1_sw.conflation_crossing_case
	-- what's not in
SELECT *
FROM shane_ud1_sw.osm_crossing
WHERE osm_id NOT IN (
    SELECT osm_id FROM shane_ud1_sw.conflation_crossing_case
); -- count : 1 
	-- NOTE for this special case: access column in osm = 'no' when typically NULL
	-- google maps shows it's a crossing for a bicycle lane




--- connecting link case ---

-- a crossing link is a small segment that connects the sidewalk to a crossing. Therefore, a typical crossing would have 2 connecting links,
-- one on both ends. However, we want to conflate these before a general case because crossing links can be for crossings and sidewalk splits that
-- are not main roads (such as building/garage entrances, parking lot entrances, alley entrances, etc.). In these cases, we still want to conflate
-- these connecting links to the main road which is typically still parallel to it.
-- connecting links are defined here as sidewalks with a length less than 12 and intersects with a crossing.

CREATE TABLE shane_ud1_sw.osm_connecting_links AS (
	SELECT DISTINCT sw.*
	FROM shane_ud1_sw.osm_sw AS sw
	JOIN shane_ud1_sw.osm_crossing AS crossing
    	ON ST_Intersects(sw.geom, crossing.geom)
    WHERE ST_Length(sw.geom) < 12
); -- count: 14

-- We often found there to be connecting links that weren't in the sidewalk data as footway was either null or undefined
-- therefore, we implement them here using the same filters
CREATE TABLE shane_ud1_sw.osm_footway_null_connecting_links AS (
	SELECT DISTINCT fn.*
	FROM shane_ud1_sw.osm_footway_null AS fn
	JOIN shane_ud1_sw.osm_crossing AS crossing
    	ON ST_Intersects(fn.geom, crossing.geom)
	WHERE ST_Length(fn.geom) < 12
); -- count: 65


-- pre-conflation check --

	-- what's not in
SELECT * FROM shane_ud1_sw.osm_sw
WHERE osm_id NOT IN (
	SELECT osm_id FROM shane_ud1_sw.osm_connecting_links
) AND osm_id NOT IN (
	SELECT osm_sw_id FROM shane_ud1_sw.conflation_entrance_case
);


-- from there we can associate a crossing link to the road the crossing is conflated to
-- NOTE: we don't associate connecting link to sidewalk road bc a connecting link is can be connected to more than 1 sidewalk
CREATE TABLE shane_ud1_sw.conflation_connecting_link_case (
	osm_label TEXT,
	osm_cl_id INT8,
	osm_cl_geom GEOMETRY(LineString, 3857),
	osm_crossing_id INT8,
	osm_crossing_geom GEOMETRY(LineString, 3857),
	arnold_route_id VARCHAR(75),
	arnold_road_geom GEOMETRY(LineString, 3857)
);

-- conflates connecting links to roads by its crossing inflation. Meaning, whatever road is conflated to the crossing a connecting link is connected to,
-- the connecting link inherits that road conflation as well. Conflating this way also distinguishes the crossings that are not on main roads OR 
-- crossings that lack the road data completely
INSERT INTO shane_ud1_sw.conflation_connecting_link_case (
	osm_label, 
	osm_cl_id, 
	osm_cl_geom, 
	osm_crossing_id, 
	osm_crossing_geom, 
	arnold_route_id, 
	arnold_road_geom
) SELECT
	'connecting link' AS osm_label,
	cl.osm_id AS osm_cl_id,
	cl.geom AS osm_cl_geom,
	crossing.osm_id AS osm_crossing_id,
	crossing.osm_geom AS osm_crossing_geom,
	crossing.arnold_route_id AS arnold_route_id,
	crossing.arnold_road_geom AS arnold_road_geom
FROM shane_ud1_sw.osm_connecting_links AS cl
JOIN shane_ud1_sw.conflation_crossing_case AS crossing
	ON ST_Intersects(cl.geom, crossing.osm_geom); -- count: 17

-- conflates connecting links found as null values in sidewalk and footway tags
INSERT INTO shane_ud1_sw.conflation_connecting_link_case (
	osm_label, 
	osm_cl_id, 
	osm_cl_geom, 
	osm_crossing_id, 
	osm_crossing_geom, 
	arnold_route_id, 
	arnold_road_geom
) SELECT
	'connecting link' AS osm_label,
	cl.osm_id AS osm_cl_id,
	cl.geom AS osm_cl_geom,
	crossing.osm_id AS osm_crossing_id,
	crossing.osm_geom AS osm_crossing_geom,
	crossing.arnold_route_id AS arnold_route_id,
	crossing.arnold_road_geom AS arnold_road_geom
FROM shane_ud1_sw.osm_footway_null_connecting_links AS cl
JOIN shane_ud1_sw.conflation_crossing_case AS crossing
	ON ST_Intersects(cl.geom, crossing.osm_geom); -- count: 68

    
-- checkpoint --
    -- what's in?
SELECT * FROM shane_ud1_sw.conflation_connecting_link_case
	-- what's not in?
SELECT * FROM shane_ud1_sw.osm_connecting_links
WHERE osm_id NOT IN (
	SELECT osm_cl_id FROM shane_ud1_sw.conflation_connecting_link_case
);
-- IMPORTANT REMINDER: the conflation table for connecting links will have duplicates of the same osm_cl_id BECAUSE connecting links
-- can have more than 1 crossing. We handle unnecessary duplication of osm_id by choosing distinct osm id's UNTIL the road conflation query




--- general case ---

-- Conflates sidewalks with roads in arnold based on buffer, angle (parallel), and segment similarity.
	-- 1: the angle of the sidewalk is parallel to the angle of the road segment
	-- 2: a sidewalk buffer of 5 intersects with a sidewalk buffer of 18
	-- 3: we take the closest and most accurate road segment to the sidewalk based on it's geometry
-- NOTE: we moved the general case from the beginning of the workflow because after conflating connecting links, 
-- it will conflate the connecting links that pass the connecting link conflation query (commonly because they lack road data, or are vehicle
-- entrances of some sort garage, parking, alley, etc.)
CREATE TABLE shane_ud1_sw.conflation_general_case AS
WITH ranked_roads AS (
	SELECT
		sw.osm_id AS osm_id,
		sw.geom AS osm_geom,
		road.route_id AS arnold_route_id,
		road.geom AS arnold_geom,
		-- finds the closest points on the road linestring to the start and end point of the sw geom and extracts the corresponding 
		-- subseciton of the road as its own distinct linestring. 
	  	ST_LineSubstring( road.geom, LEAST( ST_LineLocatePoint( road.geom, ST_ClosestPoint( st_startpoint(sw.geom), road.geom)), 
	  		ST_LineLocatePoint( road.geom, ST_ClosestPoint( st_endpoint(sw.geom), road.geom))), 
	  		GREATEST( ST_LineLocatePoint( road.geom, ST_ClosestPoint( st_startpoint(sw.geom), road.geom)), 
	  		ST_LineLocatePoint(road.geom, ST_ClosestPoint(st_endpoint(sw.geom), road.geom))) ) AS seg_geom,
	  	-- calculate the coverage of sidewalk geom within the buffer of the road
	  	ST_Length( ST_Intersection( sw.geom, ST_Buffer( ST_LineSubstring( road.geom, LEAST( ST_LineLocatePoint(road.geom, 
	  		ST_ClosestPoint(st_endpoint(sw.geom), road.geom))), GREATEST(ST_LineLocatePoint(road.geom, 
	  		ST_ClosestPoint(st_endpoint(sw.geom), road.geom))) ), 18))) / ST_Length(sw.geom) AS sw_coverage_bigroad,
	  	--  calculates a ranking or sequential number for each row based on the distance between the sw and road geom.
	  	ROW_NUMBER() OVER (
	  		PARTITION BY sw.geom
	  		ORDER BY ST_distance( 
	  			ST_LineSubstring( road.geom, LEAST(ST_LineLocatePoint(road.geom, ST_ClosestPoint(st_startpoint(sw.geom), road.geom)), 
	  				ST_LineLocatePoint(road.geom, ST_ClosestPoint(st_endpoint(sw.geom), road.geom))),
	  				GREATEST(ST_LineLocatePoint(road.geom, ST_ClosestPoint(st_startpoint(sw.geom), road.geom)), 
	  				ST_LineLocatePoint(road.geom, ST_ClosestPoint(st_endpoint(sw.geom), road.geom)))
	  			),
	  			sw.geom
	  		)
	  	) AS RANK
	FROM shane_ud1_sw.osm_sw AS sw
	JOIN shane_ud1_sw.arnold_lines AS road 
		ON ST_Intersects(ST_Buffer(sw.geom, 5), ST_Buffer(road.geom, 18))
	WHERE (
		--  calculates the angle between a road line segment and sw line segment and checks if parallel -> angle ~ 0, 180, or 360
		ABS(DEGREES(ST_Angle(ST_LineSubstring( road.geom, LEAST(ST_LineLocatePoint(road.geom, ST_ClosestPoint(st_startpoint(sw.geom), road.geom)),
			ST_LineLocatePoint(road.geom, ST_ClosestPoint(st_endpoint(sw.geom), road.geom))),
			GREATEST(ST_LineLocatePoint(road.geom, ST_ClosestPoint(st_startpoint(sw.geom), road.geom)), 
			ST_LineLocatePoint(road.geom, ST_ClosestPoint(st_endpoint(sw.geom), road.geom))) ), sw.geom))) BETWEEN 0 AND 10 -- 0 
		OR ABS(DEGREES(ST_Angle(ST_LineSubstring( road.geom, LEAST(ST_LineLocatePoint(road.geom, ST_ClosestPoint(st_startpoint(sw.geom), road.geom)),
			ST_LineLocatePoint(road.geom, ST_ClosestPoint(st_endpoint(sw.geom), road.geom))),
			GREATEST(ST_LineLocatePoint(road.geom, ST_ClosestPoint(st_startpoint(sw.geom), road.geom)),
			ST_LineLocatePoint(road.geom, ST_ClosestPoint(st_endpoint(sw.geom), road.geom))) ), sw.geom))) BETWEEN 170 AND 190 -- 180
		OR ABS(DEGREES(ST_Angle(ST_LineSubstring( road.geom, LEAST(ST_LineLocatePoint(road.geom, ST_ClosestPoint(st_startpoint(sw.geom), road.geom)),
			ST_LineLocatePoint(road.geom, ST_ClosestPoint(st_endpoint(sw.geom), road.geom))),
			GREATEST(ST_LineLocatePoint(road.geom, ST_ClosestPoint(st_startpoint(sw.geom), road.geom)),
			ST_LineLocatePoint(road.geom, ST_ClosestPoint(st_endpoint(sw.geom), road.geom))) ), sw.geom))) BETWEEN 350 AND 360 ) -- 360
) SELECT DISTINCT
	'sidewalk' AS osm_label,
  	osm_id,
  	osm_geom,
  	arnold_route_id,
  	seg_geom AS arnold_geom
FROM  ranked_roads
WHERE rank = 1
AND osm_id NOT IN(
	SELECT osm_sw_id FROM shane_ud1_sw.conflation_entrance_case
) AND osm_id NOT IN (
	SELECT osm_cl_id FROM shane_ud1_sw.conflation_connecting_link_case
); -- count 74
		

--- checkpoint ---
	-- what's in?
SELECT * FROM shane_ud1_sw.conflation_general_case
	-- what's not in?
SELECT *
FROM shane_ud1_sw.osm_sw
WHERE osm_id NOT IN (
	SELECT osm_sw_id FROM shane_ud1_sw.conflation_entrance_case
) AND osm_id NOT IN (
	SELECT osm_cl_id FROM shane_ud1_sw.conflation_connecting_link_case
) AND osm_id NOT IN (
	SELECT osm_id FROM shane_ud1_sw.conflation_general_case
);




--- edge case --- 

-- an edge is where a sidewalk segment connects 2 sidewalks that meet at a corner without connecting. Edges typically have the following characteristic:
	-- 1: on a corner/ intersection - connected to 2 sidewalks associated to 2 different roads
CREATE TABLE shane_ud1_sw.conflation_edge_case (
	osm_label TEXT,
    osm_id INT8,
    osm_geom GEOMETRY(LineString, 3857),
    arnold_road1_route_id VARCHAR(75),
    arnold_road1_geom GEOMETRY(LineString, 3857),
    arnold_road2_route_id VARCHAR(75),
    arnold_road2_geom GEOMETRY(LineString, 3857)
);

-- checks the sidewalks at the start and endpoint of the edge. If these two sidewalks are different and have different roads conflated to them,
-- the edge is defined and inherits those conflated roads as well
INSERT INTO shane_ud1_sw.conflation_edge_case (
	osm_label, 
	osm_id, 
	osm_geom, 
	arnold_road1_route_id, 
	arnold_road1_geom, 
	arnold_road2_route_id, 
	arnold_road2_geom
) SELECT
	'edge'AS osm_label,
	sw.osm_id AS osm_id,
	sw.geom AS osm_geom,
	general_case1.arnold_route_id AS arnold_road1_route_id,
	general_case1.arnold_geom AS arnold_road1_geom,
	general_case2.arnold_route_id AS arnold_road2_route_id,
	general_case2.arnold_geom AS arnold_road2_geom
FROM shane_ud1_sw.osm_sw AS sw
JOIN shane_ud1_sw.conflation_general_case AS general_case1
	ON ST_Intersects(ST_StartPoint(sw.geom), general_case1.osm_geom)
JOIN shane_ud1_sw.conflation_general_case AS general_case2
	ON ST_Intersects(ST_EndPoint(sw.geom), general_case2.osm_geom)
WHERE sw.geom NOT IN (
	SELECT osm_geom
	FROM shane_ud1_sw.conflation_general_case
) AND (
	general_case1.osm_id != general_case2.osm_id
) AND (
	general_case1.arnold_route_id != general_case2.arnold_route_id
); -- count: 5

  

-- checkpoint --
	-- what's in?
SELECT * FROM shane_ud1_sw.conflation_edge_case
	-- what's not in?
SELECT * 
FROM shane_ud1_sw.osm_sw
WHERE osm_id NOT IN (
	SELECT osm_sw_id FROM shane_ud1_sw.conflation_entrance_case
) AND osm_id NOT IN (
	SELECT osm_cl_id FROM shane_ud1_sw.conflation_connecting_link_case
) AND osm_id NOT IN (
	SELECT osm_id FROM shane_ud1_sw.conflation_general_case
) AND osm_id NOT IN (
	SELECT osm_id FROM shane_ud1_sw.conflation_edge_case
);


-- FINAL checks --

-- remaining special cases
SELECT *
FROM shane_ud1_sw.osm_sw
WHERE osm_id NOT IN (
    SELECT osm_id FROM shane_ud1_sw.conflation_general_case
) AND osm_id NOT IN (
    SELECT osm_id FROM shane_ud1_sw.conflation_edge_case
) AND osm_id NOT IN (
	SELECT osm_sw_id FROM shane_ud1_sw.conflation_entrance_case
) AND osm_id NOT IN (
	SELECT osm_cl_id FROM shane_ud1_sw.conflation_connecting_link_case
); -- count: 8
	-- 3 edge cases are bbox cutoff, and 3 are entrances of some kind

-- which roads weren't used, if any
SELECT *
FROM shane_ud1_sw.arnold_lines
WHERE shape NOT IN (
    SELECT arnold_shape FROM shane_ud1_sw.conflation_general_case
) AND shape NOT IN (
    SELECT arnold_road1_shape FROM shane_ud1_sw.conflation_edge_case
) AND shape NOT IN (
	SELECT arnold_road2_shape FROM shane_ud1_sw.conflation_edge_case
) AND shape NOT IN (
	SELECT arnold_road_shape FROM shane_ud1_sw.conflation_connecting_link_case
); --  count: 10
