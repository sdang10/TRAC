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
); -- count: 719

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





---- PROCESS ----


--- entrance case ---

-- defined as entrance if intersects with a point from osm points that has a tag of "entrance" 
-- NOTE: for now this does not conflate with anything (roads) because we don't consider these entrances associated to any road
-- we make this special case to define and justify it's reasoning to classify and move on from these "sidewalk" data in the conflation process

CREATE TABLE shane.bvu1_conflation_entrance_case (
	osm_label TEXT,
	osm_sw_id INT8,
	osm_sw_geom GEOMETRY(LineString, 3857),
	osm_point_id INT8,
	point_tags hstore,
	osm_point_geom GEOMETRY(Point, 3857)
);

INSERT INTO shane.bvu1_conflation_entrance_case (
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
FROM shane.bvu1_osm_sw AS sw
JOIN (
	SELECT *
	FROM shane.bvu1_osm_point AS point
	WHERE tags -> 'entrance' IS NOT NULL
) AS point 
	ON ST_intersects(sw.geom, point.geom); -- count: 1



    
--- crossing case ---

-- we want to conflate crossing next as we need these conflations for connecting links and crossing conflation does not depend on anything else
-- crossing is conflated based on intersection with the road. 
CREATE TABLE shane.bvu1_conflation_crossing_case (
	osm_label TEXT,
	osm_id INT8,
	osm_geom GEOMETRY(LineString, 3857),
	arnold_road_id INT8,
	arnold_road_geom GEOMETRY(LineString, 3857),
	arnold_road_shape GEOMETRY(MultilinestringM, 3857)
);

INSERT INTO shane.bvu1_conflation_crossing_case (
	osm_label, 
	osm_id, 
	osm_geom, 
	arnold_road_id, 
	arnold_road_geom, 
	arnold_road_shape
) SELECT 
	'crossing' AS osm_label,
	crossing.osm_id AS osm_id, 
	crossing.geom AS osm_geom,
	road.og_object_id AS arnold_road_id,
	road.geom AS arnold_road_geom,
	road.shape AS arnold_road_shape
FROM shane.bvu1_osm_crossing AS crossing
JOIN shane.bvu1_arnold_lines AS road 
	ON ST_Intersects(crossing.geom, road.geom); -- count 55



--- check point ---
	-- what's in
SELECT * FROM shane.bvu1_conflation_crossing_case
	-- what's not in
SELECT *
FROM shane.bvu1_osm_crossing
WHERE osm_id NOT IN (
    SELECT osm_id FROM shane.bvu1_conflation_crossing_case
);




--- connecting link case ---

-- a crossing link is a small segment that connects the sidewalk to a crossing. Therefore, a typical crossing would have 2 connecting links,
-- one on both ends. However, we want to conflate these before a general case because crossing links can be for crossings and sidewalk splits that
-- are not main roads (such as building/garage entrances, parking lot entrances, alley entrances, etc.). In these cases, we still want to conflate
-- these connecting links to the main road which is typically still parallel to it.
-- connecting links are defined here as sidewalks with a length less than 12 and intersects with a crossing.
CREATE TABLE shane.bvu1_osm_connecting_links AS (
	SELECT DISTINCT sw.*
	FROM shane.bvu1_osm_sw AS sw
	JOIN shane.bvu1_osm_crossing AS crossing
    ON ST_Intersects(sw.geom, crossing.geom)
    WHERE ST_Length(sw.geom) < 12
); -- count: 41

-- We often found there to be connecting links that weren't in the sidewalk data as footway was either null or undefined
-- therefore, we implement them here using the same filters
CREATE TABLE shane.bvu1_osm_footway_null_connecting_links AS (
	SELECT DISTINCT fn.*
	FROM shane.bvu1_osm_footway_null AS fn
	JOIN shane.bvu1_osm_crossing AS crossing
    	ON ST_Intersects(fn.geom, crossing.geom)
	WHERE ST_Length(fn.geom) < 12
); -- count: 93


-- pre-conflation check --

	-- what's not in
SELECT * FROM shane.bvu1_osm_sw
WHERE osm_id NOT IN (
	SELECT osm_id FROM shane.bvu1_osm_connecting_links
) AND osm_id NOT IN (
	SELECT osm_sw_id FROM shane.bvu1_conflation_entrance_case
);


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

-- conflates connecting links to roads by its crossing inflation. Meaning, whatever road is conflated to the crossing a connecting link is connected to,
-- the connecting link inherits that road conflation as well. Conflating this way also distinguishes the crossings that are not on main roads OR 
-- crossings that lack the road data completely
INSERT INTO shane.bvu1_conflation_connecting_link_case (
	osm_label, 
	osm_cl_id, 
	osm_cl_geom, 
	osm_crossing_id, 
	osm_crossing_geom, 
	arnold_road_id, 
	arnold_road_geom, 
	arnold_road_shape
) SELECT
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
    ON ST_Intersects(cl.geom, crossing.osm_geom); -- count: 2
	-- this is where we learned there are a lot of crossings that don't have a road to conflate to due to lack of data

-- conflates connecting links found as null values in sidewalk and footway tags
INSERT INTO shane.bvu1_conflation_connecting_link_case (
	osm_label, 
	osm_cl_id,
	osm_cl_geom, 
	osm_crossing_id, 
	osm_crossing_geom, 
	arnold_road_id, 
	arnold_road_geom, 
	arnold_road_shape
) SELECT
    'connecting link' AS osm_label,
    cl.osm_id AS osm_cl_id,
    cl.geom AS osm_cl_geom,
    crossing.osm_id AS osm_crossing_id,
    crossing.osm_geom AS osm_crossing_geom,
    crossing.arnold_road_id AS arnold_road_id,
    crossing.arnold_road_geom AS arnold_road_geom,
    crossing.arnold_road_shape AS arnold_road_shape
FROM shane.bvu1_osm_footway_null_connecting_links AS cl
JOIN shane.bvu1_conflation_crossing_case AS crossing
    ON ST_Intersects(cl.geom, crossing.osm_geom); -- count: 86

    
-- checkpoint --
    -- what's in?
SELECT * FROM shane.bvu1_conflation_connecting_link_case
	-- what's not in?
SELECT * FROM shane.bvu1_osm_connecting_links
WHERE osm_id NOT IN (
	SELECT osm_cl_id FROM shane.bvu1_conflation_connecting_link_case
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
CREATE TABLE shane.bvu1_conflation_general_case AS
WITH ranked_roads AS (
	SELECT
		sw.osm_id AS osm_id,
		sw.geom AS osm_geom,
		road.og_object_id AS arnold_object_id,
		road.geom AS arnold_geom,
		road.shape AS arnold_shape,
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
	FROM shane.bvu1_osm_sw AS sw
	JOIN shane.bvu1_arnold_lines AS road 
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
  	arnold_object_id,
  	seg_geom AS arnold_geom,
  	arnold_shape
FROM  ranked_roads
WHERE rank = 1
AND osm_id NOT IN(
	SELECT osm_sw_id FROM shane.bvu1_conflation_entrance_case
) AND osm_id NOT IN (
	SELECT osm_cl_id FROM shane.bvu1_conflation_connecting_link_case
); -- count 182
		

--- checkpoint ---	
	-- what's in?
SELECT * FROM shane.bvu1_conflation_general_case
	-- what's not in?
SELECT *
FROM shane.bvu1_osm_sw
WHERE osm_id NOT IN (
	SELECT osm_sw_id FROM shane.bvu1_conflation_entrance_case
) AND osm_id NOT IN (
	SELECT osm_cl_id FROM shane.bvu1_conflation_connecting_link_case
) AND osm_id NOT IN (
	SELECT osm_id FROM shane.bvu1_conflation_general_case
);




--- edge case --- 

-- an edge is where a sidewalk segment connects 2 sidewalks that meet at a corner without connecting. Edges typically have the following characteristic:
	-- 1: on a corner/ intersection - connected to 2 sidewalks associated to 2 different roads
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


-- checks the sidewalks at the start and endpoint of the edge. If these two sidewalks are different and have different roads conflated to them,
-- the edge is defined and inherits those conflated roads as well
INSERT INTO shane.bvu1_conflation_edge_case (
	osm_label, 
	osm_id, 
	osm_geom, 
	arnold_road1_id, 
	arnold_road1_geom,
	arnold_road1_shape, 
	arnold_road2_id, 
	arnold_road2_geom, 
	arnold_road2_shape
) SELECT
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
	-- what's in?
SELECT * FROM shane.bvu1_conflation_edge_case
	-- what's not in?
SELECT * 
FROM shane.bvu1_osm_sw
WHERE osm_id NOT IN (
	SELECT osm_sw_id FROM shane.bvu1_conflation_entrance_case
) AND osm_id NOT IN (
	SELECT osm_cl_id FROM shane.bvu1_conflation_connecting_link_case
) AND osm_id NOT IN (
	SELECT osm_id FROM shane.bvu1_conflation_general_case
) AND osm_id NOT IN (
	SELECT osm_id FROM shane.bvu1_conflation_edge_case
);



-- FINAL checks --

-- remaining special cases
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
); -- count: 51

-- which roads weren't used, if any
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
); -- count: 25


-- special case specific checks
SELECT *
FROM shane.bvu1_conflation_connecting_link_case
WHERE osm_id IN (607810499, 607810496, 1074618891);

SELECT * FROM shane.bvu1_osm_connecting_links
WHERE osm_id NOT IN (
	SELECT osm_cl_id FROM shane.bvu1_conflation_connecting_link_case
)

SELECT * FROM shane.bvu1_osm_crossing

SELECT * FROM shane.bvu1_arnold_lines 

-- connecting link special case: 607,810,499 / 1,074,618,891 / 607,810,496
-- crossing special case: 67,144,865 / 1,074,618,890











-- create a table of weird cases
CREATE TABLE shane.bvu1_weird_cases AS
SELECT sw.osm_id, sw.geom
FROM shane.bvu1_osm_sw AS sw
WHERE osm_id NOT IN (
	SELECT osm_id FROM shane.bvu1_conflation_general_case
) AND osm_id NOT IN (
    SELECT osm_id FROM shane.bvu1_conflation_edge_case
) AND osm_id NOT IN (
	SELECT osm_sw_id FROM shane.bvu1_conflation_entrance_case
) AND osm_id NOT IN (
	SELECT osm_cl_id FROM shane.bvu1_conflation_connecting_link_case
);

-- Split the weird case geom by the vertex
CREATE TABLE shane.bvu1_weird_case_segments AS
	WITH segments AS (
		SELECT osm_id,
		       row_number() OVER (PARTITION BY osm_id ORDER BY osm_id, (pt).path)-1 AS segment_number,
		       (ST_MakeLine(lag((pt).geom, 1, NULL) OVER (PARTITION BY osm_id ORDER BY osm_id, (pt).path), (pt).geom)) AS geom
		FROM (SELECT osm_id, ST_DumpPoints(geom) AS pt FROM jolie_bel1.weird_case) AS dumps )
	SELECT * FROM segments WHERE geom IS NOT NULL;




SELECT * FROM shane.bvu1_weird_case_segments


















CREATE TABLE shane.bvu1_weird_case_general_case AS
	WITH ranked_roads AS (
		SELECT
		  sw.osm_id AS osm_id,
		  road.og_object_id AS arnold_objectid,
		  sw.geom AS osm_geom,
		  sw.segment_number AS segment_number,
		  -- the road segments that the sw is conflated to
		  ST_LineSubstring( road.geom, LEAST(ST_LineLocatePoint(road.geom, ST_ClosestPoint(st_startpoint(sw.geom), road.geom)), 
		  	ST_LineLocatePoint(road.geom, ST_ClosestPoint(st_endpoint(sw.geom), road.geom))),
		  	GREATEST(ST_LineLocatePoint(road.geom, ST_ClosestPoint(st_startpoint(sw.geom), road.geom)), 
		  	ST_LineLocatePoint(road.geom, ST_ClosestPoint(st_endpoint(sw.geom), road.geom))) ) AS seg_geom,
		  -- rank this based on the distance of the midpoint of the sw to the midpoint of the road
		  ROW_NUMBER() OVER (
		  	PARTITION BY sw.geom
		  	ORDER BY ST_distance( ST_LineSubstring( road.geom, LEAST(ST_LineLocatePoint(road.geom, ST_ClosestPoint(st_startpoint(sw.geom), 
		  		road.geom)) , ST_LineLocatePoint(road.geom, ST_ClosestPoint(st_endpoint(sw.geom), road.geom))),
		  		GREATEST(ST_LineLocatePoint(road.geom, ST_ClosestPoint(st_startpoint(sw.geom), road.geom)),
		  		ST_LineLocatePoint(road.geom, ST_ClosestPoint(st_endpoint(sw.geom), road.geom))) ), sw.geom )
		  ) AS RANK
		FROM shane.bvu1_weird_case_segments sw
		JOIN shane.bvu1_arnold_lines road 
			ON ST_Intersects(ST_Buffer(sw.geom, 5), ST_Buffer(road.geom, 18))
		WHERE (
			ABS(DEGREES(ST_Angle(ST_LineSubstring( road.geom, LEAST(ST_LineLocatePoint(road.geom, 
				ST_ClosestPoint(st_startpoint(sw.geom), road.geom)) , ST_LineLocatePoint(road.geom, 
				ST_ClosestPoint(st_endpoint(sw.geom), road.geom))), GREATEST(ST_LineLocatePoint(road.geom, 
				ST_ClosestPoint(st_startpoint(sw.geom), road.geom)) , ST_LineLocatePoint(road.geom, 
				ST_ClosestPoint(st_endpoint(sw.geom), road.geom))) ), sw.geom))) BETWEEN 0 AND 10 -- 0 
			OR ABS(DEGREES(ST_Angle(ST_LineSubstring( road.geom, LEAST(ST_LineLocatePoint(road.geom, 
				ST_ClosestPoint(st_startpoint(sw.geom), road.geom)) , ST_LineLocatePoint(road.geom, 
				ST_ClosestPoint(st_endpoint(sw.geom), road.geom))), GREATEST(ST_LineLocatePoint(road.geom, 
				ST_ClosestPoint(st_startpoint(sw.geom), road.geom)) , ST_LineLocatePoint(road.geom, 
				ST_ClosestPoint(st_endpoint(sw.geom), road.geom))) ), sw.geom))) BETWEEN 170 AND 190 -- 180
			OR ABS(DEGREES(ST_Angle(ST_LineSubstring( road.geom, LEAST(ST_LineLocatePoint(road.geom, 
				ST_ClosestPoint(st_startpoint(sw.geom), road.geom)) , ST_LineLocatePoint(road.geom, 
				ST_ClosestPoint(st_endpoint(sw.geom), road.geom))), GREATEST(ST_LineLocatePoint(road.geom, 
				ST_ClosestPoint(st_startpoint(sw.geom), road.geom)) , ST_LineLocatePoint(road.geom, 
				ST_ClosestPoint(st_endpoint(sw.geom), road.geom))) ), sw.geom))) BETWEEN 350 AND 360 ) -- 360
	    )
	SELECT
		  osm_id,
		  segment_number,
		  arnold_objectid,
		  osm_geom,
		  seg_geom AS arnold_geom
	FROM
		  ranked_roads
	WHERE
		  rank = 1
	ORDER BY osm_id, segment_number;







SELECT * FROM shane.bvu1_weird_case_general_case
ORDER BY osm_id, segment_number






CREATE TEMPORARY TABLE temp_osm_road AS (
	SELECT osm_id, highway, name, tags, geom
	FROM shane.osm_roads
	WHERE geom && st_setsrid( st_makebox2d( st_makepoint(-13603442,6043723), st_makepoint(-13602226,6044848)), 3857)
	UNION ;
		





-- deal with segments that have a segment_number in between the min and max segment_number of the 
INSERT INTO shane.bvu1_weird_case_general_case (osm_id, segment_number, osm_geom, arnold_objectid)
	WITH min_max_segments AS (
	  SELECT osm_id, MIN(segment_number) AS min_segment, MAX(segment_number) AS max_segment, arnold_objectid
	  FROM shane.bvu1_weird_case_general_case
	  GROUP BY osm_id, arnold_objectid
	)
	SELECT seg_sw.osm_id, seg_sw.segment_number, seg_sw.geom, mms.arnold_objectid
	FROM shane.bvu1_weird_case_segments seg_sw
	JOIN min_max_segments mms ON seg_sw.osm_id = mms.osm_id
	WHERE seg_sw.segment_number BETWEEN mms.min_segment AND mms.max_segment
		  AND (seg_sw.osm_id, seg_sw.segment_number) NOT IN (
		  		SELECT osm_id, segment_number
		  		FROM shane.bvu1_weird_case_general_case
		  )
	ORDER BY seg_sw.osm_id, seg_sw.segment_number;













-- checkpoint
-- for the case where a osm_id is looking at 2 different arnold_objectid,
-- it will look at segments that are not conflated, uptil the first segment that intersects with the link
--SELECT DISTINCT seg.osm_id, seg.segment_number
--FROM jolie_bel1.weird_case_seg seg
--JOIN jolie_conflation_bel1.connlink link ON ST_Intersects(link.osm_geom, seg.geom)
--WHERE (seg.osm_id, seg.segment_number) NOT IN (
--		SELECT osm_id, segment_number
--		FROM temp_weird_case_sw
--	)
--ORDER BY seg.osm_id, seg.segment_number



WITH partial_length AS (
	  SELECT osm_id, arnold_objectid,  MIN(segment_number) AS min_segment, MAX(segment_number) AS max_segment, SUM(ST_Length(osm_geom)) AS partial_length, st_linemerge(ST_union(osm_geom), TRUE) AS geom
	  FROM temp_weird_case_sw
	  GROUP BY osm_id,  arnold_objectid
	)
	SELECT sw.osm_id, pl.min_segment, pl.max_segment, pl.arnold_objectid, pl.geom AS seg_geom, sw.geom AS osm_geom, pl.partial_length/ST_Length(sw.geom) AS percent_conflated
	FROM jolie_bel1.weird_case sw
	JOIN partial_length pl ON sw.osm_id = pl.osm_id; 

SELECT * osm_id
FROM temp_weird_case_sw


--WITH conf_seg AS (
--	  SELECT osm_id, arnold_objectid,  MIN(segment_number) AS min_segment, MAX(segment_number) AS max_segment, st_linemerge(ST_union(osm_geom), TRUE) AS geom
--	  FROM temp_weird_case_sw
--	  GROUP BY osm_id,  arnold_objectid
--	)
--	SELECT seg.osm_id, seg.segment_number, conf_seg.min_segment, conf_seg.max_segment, conf_seg.geom AS conf_geom, seg.geom AS seg_geom, st_length(seg.geom), conf_seg.arnold_objectid
--	FROM jolie_bel1.weird_case_seg seg
--	JOIN conf_seg
--	ON seg.osm_id = conf_seg.osm_id
--	WHERE seg.segment_number NOT BETWEEN min_segment AND max_segment
--		  AND (seg.osm_id, seg.segment_number) NOT IN (SELECT osm_id, segment_number FROM temp_weird_case_sw)
--	ORDER BY seg.osm_id, conf_seg.min_segment, seg.segment_number; 

