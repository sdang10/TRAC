--- Bellevue 1 ---


---- BOUNDING BOXES ----



-- OSM sidewalks --
CREATE TABLE shane_bvu1_sw.osm_sw AS (
	SELECT *
	FROM shane_data_setup.osm_lines
	WHERE	highway = 'footway'
		AND tags -> 'footway' = 'sidewalk' -- ONLY footway = sidewalk
		AND geom && st_setsrid( st_makebox2d( st_makepoint(-13603442,6043723), st_makepoint(-13602226,6044848)), 3857)
); -- count: 255

-- OSM points --
CREATE TABLE shane_bvu1_sw.osm_point AS (
	SELECT *
	FROM shane_data_setup.osm_points
	WHERE geom && st_setsrid( st_makebox2d( st_makepoint(-13603442,6043723), st_makepoint(-13602226,6044848)), 3857)
); -- count: 719

-- OSM crossings -- 
CREATE TABLE shane_bvu1_sw.osm_crossing AS (
	SELECT *
	FROM shane_data_setup.osm_lines
	WHERE   highway = 'footway'
		AND tags -> 'footway' = 'crossing' -- ONLY footway = crossing
		AND geom && st_setsrid( st_makebox2d( st_makepoint(-13603442,6043723), st_makepoint(-13602226,6044848)), 3857)
); -- count: 114

-- ARNOLD roads --
CREATE TABLE shane_bvu1_sw.arnold_lines AS (
	SELECT *
 	FROM shane_data_setup.arnold_lines
	WHERE geom && st_setsrid( st_makebox2d( st_makepoint(-13603442,6043723), st_makepoint(-13602226,6044848)), 3857)
); -- count: 41

-- OSM footway NULL --
CREATE TABLE shane_bvu1_sw.osm_footway_null AS (
	SELECT *
	FROM shane_data_setup.osm_lines
	WHERE 	highway = 'footway' AND 
			(tags -> 'footway' IS NULL OR 
			tags -> 'footway' NOT IN ('sidewalk', 'crossing'))  AND
			geom && st_setsrid( st_makebox2d( st_makepoint(-13603442,6043723), st_makepoint(-13602226,6044848)), 3857)
); -- count: 270





---- PROCESS ----


--- entrance case ---

-- defined as entrance if intersects with a point from osm points that has a tag of "entrance" 
-- NOTE: for now this does not conflate with anything (roads) because we don't consider these entrances associated to any road
-- we make this special case to define and justify it's reasoning to classify and move on from these "sidewalk" data in the conflation process

CREATE TABLE shane_bvu1_sw.conflation_entrance_case (
	osm_label TEXT,
	osm_sw_id INT8,
	osm_sw_geom GEOMETRY(LineString, 3857),
	osm_point_id INT8,
	point_tags hstore,
	osm_point_geom GEOMETRY(Point, 3857)
);

INSERT INTO shane_bvu1_sw.conflation_entrance_case (
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
FROM shane_bvu1_sw.osm_sw AS sw
JOIN (
	SELECT *
	FROM shane_bvu1_sw.osm_point AS point
	WHERE tags -> 'entrance' IS NOT NULL
) AS point 
	ON ST_intersects(sw.geom, point.geom); -- count: 1


SELECT * FROM shane_bvu1_sw.conflation_crossing_case
    
--- crossing case ---

-- we want to conflate crossing next as we need these conflations for connecting links and crossing conflation does not depend on anything else
-- crossing is conflated based on intersection with the road. 
CREATE TABLE shane_bvu1_sw.conflation_crossing_case (
	osm_label TEXT,
	osm_id INT8,
	osm_geom GEOMETRY(LineString, 3857),
	arnold_route_id VARCHAR(75),
	arnold_road_geom GEOMETRY(LineString, 3857)
);

INSERT INTO shane_bvu1_sw.conflation_crossing_case (
	osm_label, 
	osm_id, 
	osm_geom, 
	arnold_route_id, 
	arnold_road_geom
) SELECT DISTINCT ON (crossing.osm_id, road.route_id)
	'crossing' AS osm_label,
	crossing.osm_id AS osm_id, 
	crossing.geom AS osm_geom,
	road.route_id AS arnold_route_id,
	road.geom AS arnold_road_geom
FROM shane_bvu1_sw.osm_crossing AS crossing
JOIN shane_bvu1_sw.arnold_lines AS road 
	ON ST_Intersects(crossing.geom, road.geom); -- count 55
	
	
-- insert crossings that conflate from the osm road conflation
INSERT INTO shane_bvu1_sw.conflation_crossing_case (
	osm_label, 
	osm_id, 
	osm_geom, 
	arnold_route_id, 
	arnold_road_geom
) SELECT DISTINCT ON (crossing.osm_id, road.arnold_route_id) 
	'crossing' AS osm_label,
	crossing.osm_id AS osm_id, 
	crossing.geom AS osm_geom,
	road.arnold_route_id AS arnold_route_id,
	road.arnold_geom AS arnold_road_geom
FROM shane_bvu1_sw.osm_crossing AS crossing
JOIN shane_bvu1_roads.conflation_road_case AS road
	ON ST_Intersects(crossing.geom, road.osm_geom)
WHERE crossing.osm_id NOT IN (
	SELECT osm_id
	FROM shane_bvu1_sw.conflation_crossing_case 
); -- count: 3

	
	
--- check point ---
	-- what's in
SELECT * FROM shane_bvu1_sw.conflation_crossing_case
	-- what's not in
SELECT *
FROM shane_bvu1_sw.osm_crossing
WHERE osm_id NOT IN (
    SELECT osm_id FROM shane_bvu1_sw.conflation_crossing_case
);




--- connecting link case ---

-- a crossing link is a small segment that connects the sidewalk to a crossing. Therefore, a typical crossing would have 2 connecting links,
-- one on both ends. However, we want to conflate these before a general case because crossing links can be for crossings and sidewalk splits that
-- are not main roads (such as building/garage entrances, parking lot entrances, alley entrances, etc.). In these cases, we still want to conflate
-- these connecting links to the main road which is typically still parallel to it.
-- connecting links are defined here as sidewalks with a length less than 12 and intersects with a crossing.
CREATE TABLE shane_bvu1_sw.osm_connecting_links AS (
	SELECT DISTINCT sw.*
	FROM shane_bvu1_sw.osm_sw AS sw
	JOIN shane_bvu1_sw.osm_crossing AS crossing
    ON ST_Intersects(sw.geom, crossing.geom)
    WHERE ST_Length(sw.geom) < 12
); -- count: 41

-- We often found there to be connecting links that weren't in the sidewalk data as footway was either null or undefined
-- therefore, we implement them here using the same filters
CREATE TABLE shane_bvu1_sw.osm_footway_null_connecting_links AS (
	SELECT DISTINCT fn.*
	FROM shane_bvu1_sw.osm_footway_null AS fn
	JOIN shane_bvu1_sw.osm_crossing AS crossing
    	ON ST_Intersects(fn.geom, crossing.geom)
	WHERE ST_Length(fn.geom) < 12
); -- count: 93


-- pre-conflation check --

	-- what's not in
SELECT * FROM shane_bvu1_sw.osm_sw
WHERE osm_id NOT IN (
	SELECT osm_id FROM shane_bvu1_sw.osm_connecting_links
) AND osm_id NOT IN (
	SELECT osm_sw_id FROM shane_bvu1_sw.conflation_entrance_case
);


-- from there we can associate a crossing link to the road the crossing is conflated to
-- note: we don't associate crossing link to sidewalk road bc a connecting link is vulnerable to be connected to more than 1 sidewalk
CREATE TABLE shane_bvu1_sw.conflation_connecting_link_case (
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
INSERT INTO shane_bvu1_sw.conflation_connecting_link_case (
	osm_label, 
	osm_cl_id, 
	osm_cl_geom, 
	osm_crossing_id, 
	osm_crossing_geom, 
	arnold_route_id, 
	arnold_road_geom
) SELECT DISTINCT ON (cl.osm_id, crossing.arnold_route_id)
    'connecting link' AS osm_label,
    cl.osm_id AS osm_cl_id,
    cl.geom AS osm_cl_geom,
    crossing.osm_id AS osm_crossing_id,
    crossing.osm_geom AS osm_crossing_geom,
    crossing.arnold_route_id AS arnold_route_id,
    crossing.arnold_road_geom AS arnold_road_geom
FROM shane_bvu1_sw.osm_connecting_links AS cl
JOIN shane_bvu1_sw.conflation_crossing_case AS crossing
    ON ST_Intersects(cl.geom, crossing.osm_geom); -- count: 2
	-- this is where we learned there are a lot of crossings that don't have a road to conflate to due to lack of data

-- conflates connecting links found as null values in sidewalk and footway tags
INSERT INTO shane_bvu1_sw.conflation_connecting_link_case (
	osm_label, 
	osm_cl_id,
	osm_cl_geom, 
	osm_crossing_id, 
	osm_crossing_geom, 
	arnold_route_id, 
	arnold_road_geom
) SELECT DISTINCT ON (cl.osm_id, crossing.arnold_route_id)
    'connecting link' AS osm_label,
    cl.osm_id AS osm_cl_id,
    cl.geom AS osm_cl_geom,
    crossing.osm_id AS osm_crossing_id,
    crossing.osm_geom AS osm_crossing_geom,
    crossing.arnold_route_id AS arnold_route_id,
    crossing.arnold_road_geom AS arnold_road_geom
FROM shane_bvu1_sw.osm_footway_null_connecting_links AS cl
JOIN shane_bvu1_sw.conflation_crossing_case AS crossing
    ON ST_Intersects(cl.geom, crossing.osm_geom); -- count: 90

    
-- checkpoint --
    -- what's in?
SELECT * FROM shane_bvu1_sw.conflation_connecting_link_case
	-- what's not in?
SELECT * FROM shane_bvu1_sw.osm_connecting_links
WHERE osm_id NOT IN (
	SELECT osm_cl_id FROM shane_bvu1_sw.conflation_connecting_link_case
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
CREATE TABLE shane_bvu1_sw.conflation_general_case AS
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
	FROM shane_bvu1_sw.osm_sw AS sw
	JOIN shane_bvu1_sw.arnold_lines AS road 
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
	SELECT osm_sw_id FROM shane_bvu1_sw.conflation_entrance_case
) AND osm_id NOT IN (
	SELECT osm_cl_id FROM shane_bvu1_sw.conflation_connecting_link_case
); -- count 182
		

--- checkpoint ---	
	-- what's in?
SELECT * FROM shane_bvu1_sw.conflation_general_case
	-- what's not in?
SELECT *
FROM shane_bvu1_sw.osm_sw
WHERE osm_id NOT IN (
	SELECT osm_sw_id FROM shane_bvu1_sw.conflation_entrance_case
) AND osm_id NOT IN (
	SELECT osm_cl_id FROM shane_bvu1_sw.conflation_connecting_link_case
) AND osm_id NOT IN (
	SELECT osm_id FROM shane_bvu1_sw.conflation_general_case
);




--- corner case --- 

-- an corner is where a sidewalk segment connects 2 sidewalks that meet at a corner without connecting. Corners typically have the following characteristic:
	-- 1: on a corner/ intersection - connected to 2 sidewalks associated to 2 different roads
CREATE TABLE shane_bvu1_sw.conflation_corner_case (
	osm_label TEXT,
    osm_id INT8,
    osm_geom GEOMETRY(LineString, 3857),
    arnold_road1_route_id VARCHAR(75),
    arnold_road1_geom GEOMETRY(LineString, 3857),
    arnold_road2_route_id VARCHAR(75),
    arnold_road2_geom GEOMETRY(LineString, 3857)
);


-- checks the sidewalks at the start and endpoint of the corner. If these two sidewalks are different and have different roads conflated to them,
-- the corner is defined and inherits those conflated roads as well
INSERT INTO shane_bvu1_sw.conflation_corner_case (
	osm_label, 
	osm_id, 
	osm_geom, 
	arnold_road1_route_id, 
	arnold_road1_geom,
	arnold_road2_route_id, 
	arnold_road2_geom
) SELECT
	'corner'AS osm_label,
    sw.osm_id AS osm_id,
    sw.geom AS osm_geom,
    general_case1.arnold_route_id AS arnold_road1_route_id,
    general_case1.arnold_geom AS arnold_road1_geom, 
    general_case2.arnold_route_id AS arnold_road2_route_id,
    general_case2.arnold_geom AS arnold_road2_geom
FROM shane_bvu1_sw.osm_sw AS sw
JOIN shane_bvu1_sw.conflation_general_case AS general_case1
    ON ST_Intersects(ST_StartPoint(sw.geom), general_case1.osm_geom)
JOIN shane_bvu1_sw.conflation_general_case AS general_case2
    ON ST_Intersects(ST_EndPoint(sw.geom), general_case2.osm_geom)
WHERE sw.geom NOT IN (
	SELECT osm_geom
	FROM shane_bvu1_sw.conflation_general_case
) AND (
	general_case1.osm_id != general_case2.osm_id
) AND (
	general_case1.arnold_route_id != general_case2.arnold_route_id
); -- count: 19

  


-- gets the special cases where the sidewalk is at more of an angle compared to the road.
INSERT INTO shane_bvu1_sw.conflation_general_case(osm_label, osm_id, osm_geom, arnold_route_id, arnold_geom)
WITH ranked_road AS (
	SELECT DISTINCT 
		osm_sw.osm_id, 
		sidewalk.arnold_route_id, 
		osm_sw.geom AS osm_geom,
		ST_LineSubstring( road.geom, LEAST(ST_LineLocatePoint(road.geom, ST_ClosestPoint(st_startpoint(osm_sw.geom), road.geom)) ,
			ST_LineLocatePoint(road.geom, ST_ClosestPoint(st_endpoint(osm_sw.geom), road.geom))), GREATEST(ST_LineLocatePoint(road.geom,
			ST_ClosestPoint(st_startpoint(osm_sw.geom), road.geom)) , ST_LineLocatePoint(road.geom, ST_ClosestPoint(st_endpoint(osm_sw.geom),
			road.geom))) ) AS seg_geom,
	    -- rank this based on the distance of the midpoint of the sidewalk to the midpoint of the road
	    ROW_NUMBER() OVER (
	    PARTITION BY osm_sw.geom
	   	ORDER BY ST_distance(
	    	ST_LineSubstring( road.geom, LEAST(ST_LineLocatePoint(road.geom, ST_ClosestPoint(st_startpoint(osm_sw.geom), road.geom)) ,
	    		ST_LineLocatePoint(road.geom, ST_ClosestPoint(st_endpoint(osm_sw.geom), road.geom))), GREATEST(ST_LineLocatePoint(road.geom,
	    		ST_ClosestPoint(st_startpoint(osm_sw.geom), road.geom)) , ST_LineLocatePoint(road.geom, ST_ClosestPoint(st_endpoint(osm_sw.geom),
	    		road.geom))) ), osm_sw.geom )
	    ) AS RANK
	FROM shane_bvu1_sw.osm_sw
	JOIN shane_bvu1_sw.conflation_general_case AS sidewalk 
		ON ST_Intersects(st_startpoint(sidewalk.osm_geom), st_startpoint(osm_sw.geom))
		OR ST_Intersects(st_startpoint(sidewalk.osm_geom), st_endpoint(osm_sw.geom))
	    OR ST_Intersects(st_endpoint(sidewalk.osm_geom), st_startpoint(osm_sw.geom))
	    OR ST_Intersects(st_endpoint(sidewalk.osm_geom), st_endpoint(osm_sw.geom))
	JOIN shane_bvu1_sw.arnold_lines AS road
		ON ST_Intersects(ST_Buffer(osm_sw.geom, 5), ST_Buffer(osm_sw.geom, 18))
	WHERE osm_sw.geom NOT IN (
		SELECT sidewalk.osm_geom 
		FROM shane_bvu1_sw.conflation_general_case  AS sidewalk
		UNION ALL
		SELECT link.osm_cl_geom 
		FROM shane_bvu1_sw.conflation_connecting_link_case  AS link
		UNION ALL
		SELECT entrance.osm_sw_geom 
		FROM shane_bvu1_sw.conflation_entrance_case  AS entrance
		UNION ALL
		SELECT corner.osm_geom 
		FROM shane_bvu1_sw.conflation_corner_case  AS corner 
	) AND ( -- specify that the segment should be PARALLEL TO our conflated sidewalk
		ABS(DEGREES(ST_Angle(sidewalk.osm_geom, osm_sw.geom))) BETWEEN 0 AND 20 -- 0
		OR ABS(DEGREES(ST_Angle(sidewalk.osm_geom, osm_sw.geom))) BETWEEN 160 AND 200 -- 180
		OR ABS(DEGREES(ST_Angle(sidewalk.osm_geom, osm_sw.geom))) BETWEEN 340 AND 360 -- 360
	) AND road.route_id = sidewalk.arnold_route_id
) 
SELECT 
	'sidewalk' AS osm_label,
  	osm_id,
  	osm_geom,
  	arnold_route_id,
  	seg_geom AS arnold_geom
FROM ranked_road
WHERE RANK = 1; -- count: 7

 

-- insert new corner cases that appear after more sidewalks defined. 
INSERT INTO shane_bvu1_sw.conflation_corner_case (
	osm_label, 
	osm_id, 
	osm_geom, 
	arnold_road1_route_id, 
	arnold_road1_geom,
	arnold_road2_route_id, 
	arnold_road2_geom
) SELECT
	'corner'AS osm_label,
    sw.osm_id AS osm_id,
    sw.geom AS osm_geom,
    general_case1.arnold_route_id AS arnold_road1_route_id,
    general_case1.arnold_geom AS arnold_road1_geom, 
    general_case2.arnold_route_id AS arnold_road2_route_id,
    general_case2.arnold_geom AS arnold_road2_geom
FROM shane_bvu1_sw.osm_sw AS sw
JOIN shane_bvu1_sw.conflation_general_case AS general_case1
    ON ST_Intersects(ST_StartPoint(sw.geom), general_case1.osm_geom)
JOIN shane_bvu1_sw.conflation_general_case AS general_case2
    ON ST_Intersects(ST_EndPoint(sw.geom), general_case2.osm_geom)
WHERE sw.geom NOT IN (
	SELECT osm_geom
	FROM shane_bvu1_sw.conflation_general_case
) AND sw.geom NOT IN (
	SELECT osm_geom
	FROM shane_bvu1_sw.conflation_corner_case
) AND (
	general_case1.osm_id != general_case2.osm_id
) AND (
	general_case1.arnold_route_id != general_case2.arnold_route_id
); -- count: 3


-- checkpoint --
	-- what's in?
SELECT * FROM shane_bvu1_sw.conflation_corner_case
	-- what's not in?
SELECT * 
FROM shane_bvu1_sw.osm_sw
WHERE osm_id NOT IN (
	SELECT osm_sw_id FROM shane_bvu1_sw.conflation_entrance_case
) AND osm_id NOT IN (
	SELECT osm_cl_id FROM shane_bvu1_sw.conflation_connecting_link_case
) AND osm_id NOT IN (
	SELECT osm_id FROM shane_bvu1_sw.conflation_general_case
) AND osm_id NOT IN (
	SELECT osm_id FROM shane_bvu1_sw.conflation_corner_case
);



-- FINAL checks --

-- remaining special cases
SELECT *
FROM shane_bvu1_sw.osm_sw
WHERE osm_id NOT IN (
    SELECT osm_id FROM shane_bvu1_sw.conflation_general_case
) AND osm_id NOT IN (
    SELECT osm_id FROM shane_bvu1_sw.conflation_corner_case
) AND osm_id NOT IN (
	SELECT osm_sw_id FROM shane_bvu1_sw.conflation_entrance_case
) AND osm_id NOT IN (
	SELECT osm_cl_id FROM shane_bvu1_sw.conflation_connecting_link_case
); -- count: 41

-- which roads weren't used, if any
SELECT *
FROM shane_bvu1_sw.arnold_lines
WHERE shape NOT IN (
    SELECT arnold_shape FROM shane_bvu1_sw.conflation_general_case
) AND shape NOT IN (
    SELECT arnold_road1_shape FROM shane_bvu1_sw.conflation_corner_case
) AND shape NOT IN (
	SELECT arnold_road2_shape FROM shane_bvu1_sw.conflation_corner_case
) AND shape NOT IN (
	SELECT arnold_road_shape FROM shane_bvu1_sw.conflation_connecting_link_case
); -- count: 25





---- WEIRD CASES -----



-- create a table of weird cases
CREATE TABLE shane_bvu1_sw.weird_cases AS
SELECT sw.osm_id, sw.geom
FROM shane_bvu1_sw.osm_sw AS sw
WHERE osm_id NOT IN (
	SELECT osm_id FROM shane_bvu1_sw.conflation_general_case
) AND osm_id NOT IN (
    SELECT osm_id FROM shane_bvu1_sw.conflation_corner_case
) AND osm_id NOT IN (
	SELECT osm_sw_id FROM shane_bvu1_sw.conflation_entrance_case
) AND osm_id NOT IN (
	SELECT osm_cl_id FROM shane_bvu1_sw.conflation_connecting_link_case
); -- count: 41


-- split the weird case linestrings into segments by linestring vertex
CREATE TABLE shane_bvu1_sw.weird_case_segments AS
	WITH segments AS (
		SELECT osm_id,
		ROW_NUMBER() OVER (
		   PARTITION BY osm_id 
		   ORDER BY osm_id, (pt).path)-1 AS osm_segment_number,
		       (ST_MakeLine(lag((pt).geom, 1, NULL) OVER (PARTITION BY osm_id ORDER BY osm_id, (pt).path), (pt).geom)) AS geom
		FROM (SELECT osm_id, ST_DumpPoints(geom) AS pt FROM shane_bvu1_sw.weird_cases) AS dumps )
	SELECT * FROM segments WHERE geom IS NOT NULL; --count: 162
	

-- segments that are connecting links
CREATE TABLE shane_bvu1_sw.weird_case_connecting_link_case AS
	WITH connlink_rn AS (
		SELECT DISTINCT ON 
			( link.osm_id, link.osm_segment_number, crossing.arnold_route_id )  
			link.osm_id AS osm_id, 
			link.osm_segment_number, 
			crossing.arnold_route_id AS arnold_route_id, 
			crossing.arnold_road_geom AS arnold_road_geom,
			link.geom AS osm_geom, 
			crossing.osm_geom AS cross_geom
	    FROM shane_bvu1_sw.conflation_crossing_case AS crossing
	    JOIN shane_bvu1_sw.weird_case_segments AS link
	    ON ST_Intersects(crossing.osm_geom, st_startpoint(link.geom)) OR ST_Intersects(crossing.osm_geom, st_endpoint(link.geom))
	    WHERE ST_length(link.geom) < 12)
	SELECT 
		'connecting link' AS osm_label, 
		osm_id, 
		osm_segment_number, 
		osm_geom, 
		arnold_route_id, 
		arnold_road_geom
	FROM connlink_rn  -- count: 3
	
	
	
-- apply weird case segments to general case
CREATE TABLE shane_bvu1_sw.weird_case_general_case AS
WITH ranked_roads AS (
	SELECT
		sidewalk.osm_id AS osm_id,
	  	big_road.route_id AS arnold_route_id,
	  	sidewalk.geom AS osm_geom,
	  	sidewalk.osm_segment_number AS osm_segment_number,
	 	-- the road segments that the sidewalk is conflated to
	  	ST_LineSubstring( big_road.geom, LEAST(ST_LineLocatePoint(big_road.geom, ST_ClosestPoint(st_startpoint(sidewalk.geom), big_road.geom)) ,
	  		ST_LineLocatePoint(big_road.geom, ST_ClosestPoint(st_endpoint(sidewalk.geom), big_road.geom))),
	  		GREATEST(ST_LineLocatePoint(big_road.geom, ST_ClosestPoint(st_startpoint(sidewalk.geom), big_road.geom)) ,
	  		ST_LineLocatePoint(big_road.geom, ST_ClosestPoint(st_endpoint(sidewalk.geom), big_road.geom))) ) AS seg_geom,
	  	-- rank this based on the distance of the midpoint of the sidewalk to the midpoint of the road
	  	ROW_NUMBER() OVER (
	  		PARTITION BY sidewalk.geom
	  		ORDER BY ST_distance( ST_LineSubstring( big_road.geom, LEAST(ST_LineLocatePoint(big_road.geom,
	  			ST_ClosestPoint(st_startpoint(sidewalk.geom), big_road.geom)) , ST_LineLocatePoint(big_road.geom,
	  			ST_ClosestPoint(st_endpoint(sidewalk.geom), big_road.geom))), GREATEST(ST_LineLocatePoint(big_road.geom,
	  			ST_ClosestPoint(st_startpoint(sidewalk.geom), big_road.geom)) , ST_LineLocatePoint(big_road.geom,
	  			ST_ClosestPoint(st_endpoint(sidewalk.geom), big_road.geom))) ), sidewalk.geom )
	  	) AS RANK
	FROM shane_bvu1_sw.weird_case_segments AS sidewalk
	JOIN shane_bvu1_sw.arnold_lines AS big_road ON ST_Intersects(ST_Buffer(sidewalk.geom, 5), ST_Buffer(big_road.geom, 18))
	WHERE (
		ABS(DEGREES(ST_Angle(ST_LineSubstring( big_road.geom, LEAST(ST_LineLocatePoint(big_road.geom, ST_ClosestPoint(st_startpoint(sidewalk.geom), 
			big_road.geom)) , ST_LineLocatePoint(big_road.geom, ST_ClosestPoint(st_endpoint(sidewalk.geom), big_road.geom))),
			GREATEST(ST_LineLocatePoint(big_road.geom, ST_ClosestPoint(st_startpoint(sidewalk.geom), big_road.geom)) ,
			ST_LineLocatePoint(big_road.geom, ST_ClosestPoint(st_endpoint(sidewalk.geom), big_road.geom))) ), sidewalk.geom))) BETWEEN 0 AND 10 -- 0 
		OR ABS(DEGREES(ST_Angle(ST_LineSubstring( big_road.geom, LEAST(ST_LineLocatePoint(big_road.geom,
			ST_ClosestPoint(st_startpoint(sidewalk.geom), big_road.geom)) , ST_LineLocatePoint(big_road.geom,
			ST_ClosestPoint(st_endpoint(sidewalk.geom), big_road.geom))), GREATEST(ST_LineLocatePoint(big_road.geom,
			ST_ClosestPoint(st_startpoint(sidewalk.geom), big_road.geom)) , ST_LineLocatePoint(big_road.geom,
			ST_ClosestPoint(st_endpoint(sidewalk.geom), big_road.geom))) ), sidewalk.geom))) BETWEEN 170 AND 190 -- 180
		OR ABS(DEGREES(ST_Angle(ST_LineSubstring( big_road.geom, LEAST(ST_LineLocatePoint(big_road.geom,
			ST_ClosestPoint(st_startpoint(sidewalk.geom), big_road.geom)) , ST_LineLocatePoint(big_road.geom,
			ST_ClosestPoint(st_endpoint(sidewalk.geom), big_road.geom))), GREATEST(ST_LineLocatePoint(big_road.geom,
			ST_ClosestPoint(st_startpoint(sidewalk.geom), big_road.geom)) , ST_LineLocatePoint(big_road.geom,
			ST_ClosestPoint(st_endpoint(sidewalk.geom), big_road.geom))) ), sidewalk.geom))) BETWEEN 350 AND 360 ) -- 360
    AND (sidewalk.osm_id, sidewalk.osm_segment_number) NOT IN (
    	SELECT osm_id, osm_segment_number
      	FROM shane_bvu1_sw.weird_case_connecting_link_case
    )
)
SELECT
	'sidewalk' AS osm_label,
  	osm_id,
  	osm_segment_number,
  	osm_geom,
  	arnold_route_id,
  	seg_geom AS arnold_geom
FROM ranked_roads
WHERE rank = 1
ORDER BY osm_id, osm_segment_number; -- count: 15



	
	



-- in the case where a connecting link segment and general case segment are both conflated and share the same osm_id, the segments in between that
-- are NOT conflated will inherit the road conflation the conflated sidewalk and connecting link share. 
INSERT INTO shane_bvu1_sw.weird_case_general_case (osm_label, osm_id, osm_segment_number, arnold_route_id, osm_geom, arnold_geom)
WITH min_max_segments AS (
	SELECT 
		sw.osm_id, 
		MIN(LEAST(sw.osm_segment_number, link.osm_segment_number)) AS min_segment, 
		MAX(GREATEST(sw.osm_segment_number, link.osm_segment_number)) AS max_segment, 
		sw.arnold_route_id
	FROM shane_bvu1_sw.weird_case_general_case AS sw
	JOIN shane_bvu1_sw.weird_case_connecting_link_case AS link
		ON sw.osm_id = link.osm_id AND sw.arnold_route_id = link.arnold_route_id
	GROUP BY sw.osm_id, sw.arnold_route_id
)
SELECT 
	'sidewalk' AS osm_label,
	seg_sw.osm_id, 
	seg_sw.osm_segment_number,  
	mms.arnold_route_id, 
	seg_sw.geom,
	ST_LineSubstring( road.geom, LEAST(ST_LineLocatePoint(road.geom, ST_ClosestPoint(st_startpoint(seg_sw.geom), road.geom)) ,
		ST_LineLocatePoint(road.geom, ST_ClosestPoint(st_endpoint(seg_sw.geom), road.geom))), GREATEST(ST_LineLocatePoint(road.geom,
		ST_ClosestPoint(st_startpoint(seg_sw.geom), road.geom)) , ST_LineLocatePoint(road.geom, ST_ClosestPoint(st_endpoint(seg_sw.geom),
		road.geom))) ) AS seg_geom
FROM shane_bvu1_sw.weird_case_segments AS seg_sw
JOIN min_max_segments AS mms 
	ON seg_sw.osm_id = mms.osm_id
JOIN shane_bvu1_sw.arnold_lines AS road
	ON mms.arnold_route_id = road.route_id
WHERE seg_sw.osm_segment_number BETWEEN mms.min_segment AND mms.max_segment
AND (seg_sw.osm_id, seg_sw.osm_segment_number) NOT IN (
	SELECT 
		osm_id, 
		osm_segment_number
	FROM shane_bvu1_sw.weird_case_general_case
) AND (seg_sw.osm_id, seg_sw.osm_segment_number) NOT IN (
	SELECT 
		osm_id, 
		osm_segment_number
	FROM shane_bvu1_sw.weird_case_connecting_link_case
)
ORDER BY seg_sw.osm_id, seg_sw.osm_segment_number; -- count: 2


	
	
-- conflates the weird case segments that are between 2 different weird case segments that conflated to the same road, giving all the roads
-- in between that road conflation as well. 
INSERT INTO shane_bvu1_sw.weird_case_general_case (osm_label, osm_id, osm_segment_number, arnold_route_id, osm_geom, arnold_geom)
WITH min_max_segments AS (
  	SELECT 
  		osm_id, 
  		MIN(osm_segment_number) AS min_segment, 
  		MAX(osm_segment_number) AS max_segment, 
  		arnold_route_id
  	FROM shane_bvu1_sw.weird_case_general_case
  	GROUP BY osm_id, arnold_route_id
)
SELECT 
	'sidewalk' AS osm_label,
	seg_sw.osm_id AS osm_id, 
	seg_sw.osm_segment_number AS osm_segment_number, 
	mms.arnold_route_id AS arnold_route_id, 
	seg_sw.geom AS osm_geom,
	ST_LineSubstring( road.geom, LEAST(ST_LineLocatePoint(road.geom, ST_ClosestPoint(st_startpoint(seg_sw.geom), road.geom)),
		ST_LineLocatePoint(road.geom, ST_ClosestPoint(st_endpoint(seg_sw.geom), road.geom))), 
		GREATEST(ST_LineLocatePoint(road.geom, ST_ClosestPoint(st_startpoint(seg_sw.geom), road.geom)),
		ST_LineLocatePoint(road.geom, ST_ClosestPoint(st_endpoint(seg_sw.geom), road.geom))) ) AS seg_geom
FROM shane_bvu1_sw.weird_case_segments seg_sw
JOIN min_max_segments mms 
	ON seg_sw.osm_id = mms.osm_id
JOIN shane_bvu1_sw.arnold_lines  road
	ON mms.arnold_route_id = road.route_id
WHERE seg_sw.osm_segment_number BETWEEN mms.min_segment AND mms.max_segment
AND (seg_sw.osm_id, seg_sw.osm_segment_number) NOT IN (
	SELECT osm_id, osm_segment_number
	FROM shane_bvu1_sw.weird_case_general_case 
)
AND (seg_sw.osm_id, seg_sw.osm_segment_number) NOT IN (
	SELECT osm_id, osm_segment_number
	FROM shane_ud1_sw.osm_footway_null_connecting_links 
)
ORDER BY seg_sw.osm_id, seg_sw.osm_segment_number; -- count: 1



-- conflate sidewalks that are parallel to the already conflated sidewalk
INSERT INTO shane_bvu1_sw.weird_case_general_case (osm_label, osm_id, osm_segment_number, arnold_route_id, osm_geom, arnold_geom)
WITH ranked_road AS (
	SELECT DISTINCT 
		osm_sw.osm_id AS osm_id, 
		osm_sw.osm_segment_number AS osm_segment_number, 
		sidewalk.arnold_route_id AS arnold_route_id, 
		osm_sw.geom AS osm_geom,
		ST_LineSubstring( big_road.geom, LEAST(ST_LineLocatePoint(big_road.geom, ST_ClosestPoint(st_startpoint(osm_sw.geom), big_road.geom)),
			ST_LineLocatePoint(big_road.geom, ST_ClosestPoint(st_endpoint(osm_sw.geom), big_road.geom))), 
			GREATEST(ST_LineLocatePoint(big_road.geom, ST_ClosestPoint(st_startpoint(osm_sw.geom), big_road.geom)),
			ST_LineLocatePoint(big_road.geom, ST_ClosestPoint(st_endpoint(osm_sw.geom), big_road.geom))) ) AS seg_geom,
			-- rank this based on the distance of the midpoint of the sidewalk to the midpoint of the road
			ROW_NUMBER() OVER (
				PARTITION BY osm_sw.geom
				ORDER BY ST_distance( 
					ST_LineSubstring( big_road.geom, LEAST(ST_LineLocatePoint(big_road.geom, ST_ClosestPoint(st_startpoint(osm_sw.geom),
						big_road.geom)) , ST_LineLocatePoint(big_road.geom, ST_ClosestPoint(st_endpoint(osm_sw.geom), big_road.geom))),
					  	GREATEST(ST_LineLocatePoint(big_road.geom, ST_ClosestPoint(st_startpoint(osm_sw.geom), big_road.geom)),
					  	ST_LineLocatePoint(big_road.geom, ST_ClosestPoint(st_endpoint(osm_sw.geom), big_road.geom)))
					), osm_sw.geom 
				)
			) AS RANK
	FROM shane_bvu1_sw.weird_case_segments osm_sw
	JOIN shane_bvu1_sw.weird_case_general_case sidewalk
		ON 	ST_Intersects(st_startpoint(sidewalk.osm_geom), st_startpoint(osm_sw.geom))
		OR ST_Intersects(st_startpoint(sidewalk.osm_geom), st_endpoint(osm_sw.geom))
		OR ST_Intersects(st_endpoint(sidewalk.osm_geom), st_startpoint(osm_sw.geom))
		OR ST_Intersects(st_endpoint(sidewalk.osm_geom), st_endpoint(osm_sw.geom))
	JOIN shane_bvu1_sw.arnold_lines big_road
		ON ST_Intersects(ST_Buffer(osm_sw.geom, 5), ST_Buffer(osm_sw.geom, 18))
	WHERE (osm_sw.osm_id, osm_sw.osm_segment_number) NOT IN (
		SELECT sw.osm_id, sw.osm_segment_number FROM shane_bvu1_sw.weird_case_general_case AS sw
		UNION ALL 
		SELECT link.osm_id, link.osm_segment_number FROM shane_bvu1_sw.weird_case_connecting_link_case AS link
	) AND ( -- specify that the segment should be PARALLEL TO our conflated sidewalk
		ABS(DEGREES(ST_Angle(sidewalk.osm_geom, osm_sw.geom))) BETWEEN 0 AND 20 -- 0 
		OR ABS(DEGREES(ST_Angle(sidewalk.osm_geom, osm_sw.geom))) BETWEEN 160 AND 200 -- 180
		OR ABS(DEGREES(ST_Angle(sidewalk.osm_geom, osm_sw.geom))) BETWEEN 340 AND 360  ) -- 360 
		AND big_road.route_id = sidewalk.arnold_route_id
)
SELECT 
	'sidewalk' AS osm_label,
	osm_id, 
	osm_segment_number, 
	arnold_route_id, 
	osm_geom, 
	seg_geom AS arnold_geom
FROM ranked_road
WHERE RANK = 1; -- count: 3


-- checkpoint --
SELECT * FROM shane_bvu1_sw.weird_case_general_case




-- Step 3: Deal with corner
CREATE TABLE shane_bvu1_sw.weird_case_corner_case AS
	SELECT  corner.osm_id AS osm_id,
			corner.osm_segment_number AS osm_segment_number,
			road1.arnold_route_id AS arnold_road1_route_id,
			road2.arnold_route_id AS arnold_road2_route_id,
			corner.geom AS osm_geom
	FROM shane_bvu1_sw.weird_case_segments corner
	JOIN (	SELECT * 
			FROM shane_bvu1_sw.weird_case_general_case
			UNION ALL
			SELECT osm_label, osm_id, 0 AS osm_segment_number, osm_geom, arnold_route_id, arnold_geom
			FROM shane_bvu1_sw.conflation_general_case
		 ) road1
		ON st_intersects(st_startpoint(corner.geom), road1.osm_geom)
	JOIN (  SELECT * 
			FROM shane_bvu1_sw.weird_case_general_case
			UNION ALL
			SELECT osm_label, osm_id, 0 AS osm_segment_number, osm_geom, arnold_route_id, arnold_geom
			FROM shane_bvu1_sw.conflation_general_case
		 ) road2
		ON st_intersects(st_endpoint(corner.geom), road2.osm_geom)
	WHERE   (corner.osm_id, corner.osm_segment_number) NOT IN (
					SELECT sw.osm_id, sw.osm_segment_number FROM shane_bvu1_sw.weird_case_general_case sw
					UNION ALL 
					SELECT link.osm_id, link.osm_segment_number FROM shane_bvu1_sw.weird_case_connecting_link_case link
				)
			AND ST_Equals(road1.osm_geom, road2.osm_geom) IS FALSE
			AND road1.arnold_route_id != road2.arnold_route_id -- count: 3
