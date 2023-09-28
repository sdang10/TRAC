-- THIS CODE IS THE CONFLATION PROCESS BETWEEN OSM ROADS AND SIDEWALKS TO ARNOLD ROADS --



-- Initializing the data into distinct categorical tables based on geometry
-- to be used in separate conflation processes and methods 
-- while also confining the scope of the geometries to a specified bounding box
CREATE OR REPLACE PROCEDURE initialize_data_to_bb()
LANGUAGE plpgsql AS $$
DECLARE

	-- The current code is set to a bounding box that of Capital Hill Seattle, WA
	bb box2d := st_setsrid( st_makebox2d( st_makepoint(-13617414,6042417), st_makepoint(-13614697,6045266)), 3857);

BEGIN

	-- OSM roads -- 
	CREATE TEMP TABLE osm_roads AS (
		SELECT *
		FROM shane_data_setup.osm_lines
		WHERE highway IN ('motorway', 'trunk', 'primary', 'secondary', 'tertiary', 'road', 'busway', 'unclassified', 'residential')
		AND geom && bb
	);

	-- OSM sidewalks --
	CREATE TEMP TABLE osm_sw AS (
		SELECT *
		FROM shane_data_setup.osm_lines
		WHERE	highway = 'footway'
			AND tags -> 'footway' = 'sidewalk'
			AND geom && bb
	);
	
	-- OSM points --
	CREATE TEMP TABLE osm_point AS (
		SELECT *
		FROM shane_data_setup.osm_points
		WHERE geom && bb
	);
	
	-- OSM crossings -- 
	CREATE TEMP TABLE osm_crossing AS (
		SELECT *
		FROM shane_data_setup.osm_lines
		WHERE   highway = 'footway'
			AND tags -> 'footway' = 'crossing'
			AND geom && bb
	);
	
	-- OSM footway NULL --
	-- this is created after discovering there were valid sidewalk and/or crossing data with the tag for it being NULL
	-- thus meaning they would not be included and conflated if not included in this table here
	CREATE TEMP TABLE osm_footway_null AS (
		SELECT *
		FROM shane_data_setup.osm_lines
		WHERE 	highway = 'footway' AND 
				(tags -> 'footway' IS NULL OR 
				tags -> 'footway' NOT IN ('sidewalk', 'crossing'))  AND
				geom && bb
	);

	-- ARNOLD roads --
	CREATE TEMP TABLE arnold_lines AS (
		SELECT *
 		FROM shane_data_setup.arnold_lines 
		WHERE geom && bb
	);

END $$;





-- Creates a new OSM roads table adding a column for lane count by extracting the lane data from the tags column when provided.
-- It then also optimizes data efficiency by checking for applicable lane data assumption. 
-- This is done by assuming lane data for roads so long as the road is connected to another road that was given a value in the lane column 
CREATE OR REPLACE PROCEDURE osm_road_add_lane_data()
LANGUAGE plpgsql AS $$
DECLARE
	
	_itr_cnt int := 0;
	_res_cnt int;

BEGIN
	
	CREATE TEMP TABLE osm_roads_add_lanes AS (
		SELECT 
			osm_id, 
			name AS osm_road_name, 
			CAST(tags -> 'lanes' AS int) AS lanes, 
			geom
		FROM osm_roads
		WHERE tags ? 'lanes' 
	);

	-- NOTE: This process is looped as we can continue to assume lane data until there are no roads without lane count connected to ones that do
	LOOP
		
		_itr_cnt := _itr_cnt + 1;
		
		WITH ranked_road AS (
			SELECT 
				r2.osm_id AS osm_id, 
				r2.name AS osm_road_name, 
				r1.lanes AS lanes, 
				r2.geom AS osm_geom,
				ROW_NUMBER() OVER (
					PARTITION BY r2.osm_id
	   				ORDER BY r1.lanes DESC
				) AS RANK 
			FROM osm_roads_add_lanes r1
			JOIN (
				SELECT osm_id, name, highway, geom
				FROM osm_roads
				WHERE tags -> 'lanes' IS NULL) AS r2
			ON ST_Intersects(st_startpoint(r1.geom), st_startpoint(r2.geom))
			OR ST_Intersects(st_startpoint(r1.geom), st_endpoint(r2.geom))
			OR ST_Intersects(st_endpoint(r1.geom), st_startpoint(r2.geom))
			OR ST_Intersects(st_endpoint(r1.geom), st_endpoint(r2.geom))
			WHERE r1.osm_id != r2.osm_id
			AND ( 
				ABS(DEGREES(ST_Angle(r1.geom, r2.geom))) BETWEEN 0 AND 10 -- 0 
				OR ABS(DEGREES(ST_Angle(r1.geom, r2.geom))) BETWEEN 170 AND 190 -- 180 
				OR ABS(DEGREES(ST_Angle(r1.geom, r2.geom))) BETWEEN 350 AND 360  -- 360
			)
			AND r2.osm_id NOT IN(
				SELECT osm_id FROM osm_roads_add_lanes
			)		    
		), ins AS (
			INSERT INTO osm_roads_add_lanes(osm_id, osm_road_name, lanes, geom)
			SELECT 
				osm_id, 
				osm_road_name, 
				lanes, 
				osm_geom
			FROM ranked_road
			WHERE RANK = 1
			RETURNING 1
		)
		SELECT count(*)
			INTO _res_cnt
		FROM ins;
	
	
		RAISE NOTICE 'Iteration %: inserted % rows', _itr_cnt, _res_cnt;
	     
		IF (_res_cnt = 0) THEN
			EXIT;
		
		END IF;
		
	END LOOP;

END $$;





-- Creates the OSM roads to ARNOLD roads conflation table --
-- For this process, we needed to segment arnold road geometries as they were often vastly larger than osm road geometries.
-- This made it easier to find the association between osm and arnold geometries.
CREATE OR REPLACE PROCEDURE road_to_road_conflation()
LANGUAGE plpgsql AS $$

BEGIN
	
	CREATE TABLE ch1_automated.road_conflation AS 
	WITH ranked_roads AS (
		SELECT 
		arnold.route_id AS arnold_route_id, 
		arnold.shape AS arnold_shape,
		osm.osm_id AS osm_id, 
		osm.lanes AS lanes, 
		osm.osm_road_name AS osm_road_name,  
		osm.geom AS osm_geom,
		
		-- segmentation of arnold road geometries 
		ST_LineSubstring( arnold.geom, LEAST(ST_LineLocatePoint(arnold.geom, ST_ClosestPoint(st_startpoint(osm.geom), arnold.geom)), 
			ST_LineLocatePoint(arnold.geom, ST_ClosestPoint(st_endpoint(osm.geom), arnold.geom))), GREATEST(ST_LineLocatePoint(arnold.geom,
			ST_ClosestPoint(st_startpoint(osm.geom), arnold.geom)) , ST_LineLocatePoint(arnold.geom, ST_ClosestPoint(st_endpoint(osm.geom), 
			arnold.geom))) ) AS seg_geom,
			
		-- ranking of associations in order to pick out the top ones for conflation
		-- ranking is done based on the amount of buffer overlap between OSM and ARNOLD roads
			-- buffer is based on the average lane width in meters and OSM roads are scaled by their lane count data
		ROW_NUMBER() OVER (
	    	PARTITION BY osm.geom
	        ORDER BY 
	        ST_Area(ST_Intersection(ST_Buffer(osm.geom, lanes * 4), ST_Buffer(arnold.geom, 1))) DESC
	        ) AS RANK,
	        
	    
	    ST_Intersection(ST_Buffer(osm.geom, lanes * 4 ), ST_Buffer(arnold.geom, 1)) AS intersection_geom,
	    ST_Area(ST_Intersection(ST_Buffer(osm.geom, lanes * 4 ), ST_Buffer(arnold.geom, 1))) AS overlap_area
		FROM osm_roads_add_lanes AS osm
		RIGHT JOIN arnold_lines AS arnold
		
		-- joins where buffer overlap
		ON ST_Intersects(ST_buffer(osm.geom, (osm.lanes) * 2), arnold.geom)
		
		-- joins where angle similarity is within a threshold of being parallel
		-- threshold is set to 10 degrees
		WHERE (  
			ABS(DEGREES(ST_Angle(ST_LineSubstring( arnold.geom, LEAST(ST_LineLocatePoint(arnold.geom, ST_ClosestPoint(st_startpoint(osm.geom), 
			arnold.geom)) , ST_LineLocatePoint(arnold.geom, ST_ClosestPoint(st_endpoint(osm.geom), arnold.geom))),
			GREATEST(ST_LineLocatePoint(arnold.geom, ST_ClosestPoint(st_startpoint(osm.geom), arnold.geom)) , ST_LineLocatePoint(arnold.geom, 
			ST_ClosestPoint(st_endpoint(osm.geom), arnold.geom))) ), osm.geom))) BETWEEN 0 AND 10 -- 0 degrees  
		OR ABS(DEGREES(ST_Angle(ST_LineSubstring( arnold.geom, LEAST(ST_LineLocatePoint(arnold.geom, ST_ClosestPoint(st_startpoint(osm.geom), 
			arnold.geom)) , ST_LineLocatePoint(arnold.geom, ST_ClosestPoint(st_endpoint(osm.geom), arnold.geom))), 
			GREATEST(ST_LineLocatePoint(arnold.geom, ST_ClosestPoint(st_startpoint(osm.geom), arnold.geom)) , ST_LineLocatePoint(arnold.geom, 
			ST_ClosestPoint(st_endpoint(osm.geom), arnold.geom))) ), osm.geom))) BETWEEN 170 AND 190 -- 180 degrees
		OR ABS(DEGREES(ST_Angle(ST_LineSubstring( arnold.geom, LEAST(ST_LineLocatePoint(arnold.geom, ST_ClosestPoint(st_startpoint(osm.geom), 
			arnold.geom)), ST_LineLocatePoint(arnold.geom, ST_ClosestPoint(st_endpoint(osm.geom), arnold.geom))), 
			GREATEST(ST_LineLocatePoint(arnold.geom, ST_ClosestPoint(st_startpoint(osm.geom), arnold.geom)), ST_LineLocatePoint(arnold.geom, 
			ST_ClosestPoint(st_endpoint(osm.geom), arnold.geom))) ), osm.geom))) BETWEEN 350 AND 360 ) -- 360 degrees
	)
	SELECT
		osm_id,
		osm_road_name,
		lanes,
		osm_geom,
		arnold_route_id,
		RANK,
		seg_geom AS arnold_geom,
		arnold_shape AS arnold_shape,
		intersection_geom,
		overlap_area
	FROM ranked_roads
	
	-- in some cases, we need to pick the top 2+ for conflation as some OSM roads are associated to more than 1 ARNOLD road
	WHERE 
		ST_Length(seg_geom) > 4
	AND RANK = 1
	OR (
		RANK = 2
		AND EXISTS (
	    	SELECT 1
	    	FROM ranked_roads AS ia2
	    	WHERE ia2.osm_id = ranked_roads.osm_id
	     	AND ia2.rank = 1
	     	AND ST_Area(ST_Intersection(ST_Buffer(ia2.seg_geom, 4), ST_Buffer(ranked_roads.seg_geom, 4))) < 100
	    ) AND ST_Length(seg_geom) > 4
	);
	
END $$;




-- do we need this? --
CREATE OR REPLACE PROCEDURE entrance_conflation()
LANGUAGE plpgsql AS $$

BEGIN 
		
	CREATE TABLE ch1_automated.entrance_conflation (
		osm_label TEXT,
		osm_sw_id INT8,
		osm_sw_geom GEOMETRY(LineString, 3857),
		osm_point_id INT8,
		point_tags hstore,
		osm_point_geom GEOMETRY(Point, 3857)
	);
	
	INSERT INTO ch1_automated.entrance_conflation (
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
	FROM osm_sw AS sw
	JOIN (
		SELECT *
		FROM osm_point AS point
		WHERE tags -> 'entrance' IS NOT NULL
	) AS point 
		ON ST_intersects(sw.geom, point.geom);
	
END $$;





-- Creates the OSM crossing to ARNOLD road conflation table --
-- This process simply consists of if the OSM crossing geometry intersects with ARNOLD road geometry, they are associated
CREATE OR REPLACE PROCEDURE crossing_to_road_conflation()
LANGUAGE plpgsql AS $$

BEGIN 
	
	CREATE TABLE ch1_automated.crossing_conflation (
		osm_label TEXT,
		osm_id INT8,
		osm_geom GEOMETRY(LineString, 3857),
		arnold_route_id VARCHAR(75),
		arnold_geom GEOMETRY(LineString, 3857)
	);
	
	INSERT INTO ch1_automated.crossing_conflation (
		osm_label, 
		osm_id, 
		osm_geom, 
		arnold_route_id, 
		arnold_geom
	) SELECT DISTINCT ON (crossing.osm_id, road.route_id)
		'crossing' AS osm_label,
		crossing.osm_id AS osm_id, 
		crossing.geom AS osm_geom,
		road.route_id AS arnold_route_id,
		road.geom AS arnold_geom
	FROM osm_crossing AS crossing
	JOIN arnold_lines AS road 
		ON ST_Intersects(crossing.geom, road.geom);
		
		
	-- insert crossings that conflate from the osm road conflation --
	-- this helps conflate crossings that do not conflate with ARNOLD geometry directly, but can be associated based on how they
	-- intersect with an OSM road that conflated to an ARNOLD road
	INSERT INTO ch1_automated.crossing_conflation (
		osm_label, 
		osm_id, 
		osm_geom, 
		arnold_route_id, 
		arnold_geom
	) SELECT DISTINCT ON (crossing.osm_id, road.arnold_route_id) 
		'crossing' AS osm_label,
		crossing.osm_id AS osm_id, 
		crossing.geom AS osm_geom,
		road.arnold_route_id AS arnold_route_id,
		road.arnold_geom AS arnold_geom
	FROM osm_crossing AS crossing
	JOIN ch1_automated.road_conflation AS road
		ON ST_Intersects(crossing.geom, road.osm_geom)
	WHERE crossing.osm_id NOT IN (
		SELECT osm_id
		FROM ch1_automated.crossing_conflation
	);
	
END $$;





-- Creates the OSM connecting link to ARNOLD road conflation table --
-- Connecting links are defined as OSM sidewalk segments that are small in size and intersect with an OSM sidewalk geometry at one endpoint 
-- while also intersecting with an OSM crossing geometry at the other.
-- Connecting links are associated to the road to which they connect to via crossing, therefore, they inherit the ARNOLD road association
-- from the crossing they are connected to if that crossing is conflated.
CREATE OR REPLACE PROCEDURE connecting_link_to_road_conflation()
LANGUAGE plpgsql AS $$

BEGIN
	
	CREATE TEMP TABLE osm_connecting_links AS (
		SELECT DISTINCT sw.*
		FROM osm_sw AS sw
		JOIN osm_crossing AS crossing
	    ON ST_Intersects(sw.geom, crossing.geom)
	    WHERE ST_Length(sw.geom) < 12
	); 
	
	-- connecting links found in the null table
	CREATE TEMP TABLE osm_footway_null_connecting_links AS (
		SELECT DISTINCT fn.*
		FROM osm_footway_null AS fn
		JOIN osm_crossing AS crossing
	    	ON ST_Intersects(fn.geom, crossing.geom)
		WHERE ST_Length(fn.geom) < 12
	);

	CREATE TABLE ch1_automated.connecting_link_conflation (
		osm_label TEXT,
		osm_cl_id INT8,
		osm_cl_geom GEOMETRY(LineString, 3857),
		osm_crossing_id INT8,
		osm_crossing_geom GEOMETRY(LineString, 3857),
		arnold_route_id VARCHAR(75),
		arnold_geom GEOMETRY(LineString, 3857)
	);

	INSERT INTO ch1_automated.connecting_link_conflation (
		osm_label, 
		osm_cl_id, 
		osm_cl_geom, 
		osm_crossing_id, 
		osm_crossing_geom, 
		arnold_route_id, 
		arnold_geom
	) SELECT DISTINCT ON (cl.osm_id, crossing.arnold_route_id)
	    'connecting link' AS osm_label,
	    cl.osm_id AS osm_cl_id,
	    cl.geom AS osm_cl_geom,
	    crossing.osm_id AS osm_crossing_id,
	    crossing.osm_geom AS osm_crossing_geom,
	    crossing.arnold_route_id AS arnold_route_id,
	    crossing.arnold_geom AS arnold_geom
	FROM osm_connecting_links AS cl
	JOIN ch1_automated.crossing_conflation AS crossing
	    ON ST_Intersects(cl.geom, crossing.osm_geom); 
	
	    
	INSERT INTO ch1_automated.connecting_link_conflation (
		osm_label, 
		osm_cl_id,
		osm_cl_geom, 
		osm_crossing_id, 
		osm_crossing_geom, 
		arnold_route_id, 
		arnold_geom
	) SELECT DISTINCT ON (cl.osm_id, crossing.arnold_route_id)
	    'connecting link' AS osm_label,
	    cl.osm_id AS osm_cl_id,
	    cl.geom AS osm_cl_geom,
	    crossing.osm_id AS osm_crossing_id,
	    crossing.osm_geom AS osm_crossing_geom,
	    crossing.arnold_route_id AS arnold_route_id,
	    crossing.arnold_geom AS arnold_geom
	FROM osm_footway_null_connecting_links AS cl
	JOIN ch1_automated.crossing_conflation AS crossing
	    ON ST_Intersects(cl.geom, crossing.osm_geom);
	
END $$;





-- Creates the OSM sidewalk to ARNOLD road conflation table -- 
-- This process consists of ranking, buffer intersection, and angle similarity checks 
-- For this process, we needed to segment arnold road geometries as they were often vastly larger than osm road geometries.
-- This made it easier to find the association between osm and arnold geometries.
CREATE OR REPLACE PROCEDURE sidewalk_to_road_conflation()
LANGUAGE plpgsql AS $$

BEGIN
	
	CREATE TABLE ch1_automated.sidewalk_conflation AS
	WITH ranked_roads AS (
		SELECT
			sw.osm_id AS osm_id,
			sw.geom AS osm_geom,
			road.route_id AS arnold_route_id,
			road.geom AS arnold_geom,
			
			
			-- code that does the segmentation of arnold geometries 
		  	ST_LineSubstring( road.geom, LEAST( ST_LineLocatePoint( road.geom, ST_ClosestPoint( st_startpoint(sw.geom), road.geom)), 
		  		ST_LineLocatePoint( road.geom, ST_ClosestPoint( st_endpoint(sw.geom), road.geom))), 
		  		GREATEST( ST_LineLocatePoint( road.geom, ST_ClosestPoint( st_startpoint(sw.geom), road.geom)), 
		  		ST_LineLocatePoint(road.geom, ST_ClosestPoint(st_endpoint(sw.geom), road.geom))) ) AS seg_geom,
		  		
		  		
		  	-- calculate the amount of coverage made on the OSM sidewalk geometry buffer by the ARNOLD road buffer
		  	ST_Length( ST_Intersection( sw.geom, ST_Buffer( ST_LineSubstring( road.geom, LEAST( ST_LineLocatePoint(road.geom, 
		  		ST_ClosestPoint(st_endpoint(sw.geom), road.geom))), GREATEST(ST_LineLocatePoint(road.geom, 
		  		ST_ClosestPoint(st_endpoint(sw.geom), road.geom))) ), 18))) / ST_Length(sw.geom) AS sw_coverage_bigroad,
		  		
		  		
		  	--  ranking based on the distance between OSM sidewalk geometries and ARNOLD road geometries
		  	ROW_NUMBER() OVER (
		  		PARTITION BY sw.geom
		  		ORDER BY 
		  			ST_distance(ST_LineSubstring( road.geom, LEAST(ST_LineLocatePoint(road.geom, ST_ClosestPoint(st_startpoint(sw.geom), road.geom)), 
		  				ST_LineLocatePoint(road.geom, ST_ClosestPoint(st_endpoint(sw.geom), road.geom))),
		  				GREATEST(ST_LineLocatePoint(road.geom, ST_ClosestPoint(st_startpoint(sw.geom), road.geom)), 
		  				ST_LineLocatePoint(road.geom, ST_ClosestPoint(st_endpoint(sw.geom), road.geom)))), sw.geom
		  			)
		  	) AS RANK
		  	
		  	
		FROM osm_sw AS sw
		JOIN arnold_lines AS road 
			ON ST_Intersects(ST_Buffer(sw.geom, 5), ST_Buffer(road.geom, 18))
		WHERE (
			-- joins where angle similarity is within a threshold of being parallel
			-- threshold is set to 10 degrees
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
		)
	SELECT DISTINCT
		'sidewalk' AS osm_label,
	  	osm_id,
	  	osm_geom,
	  	arnold_route_id,
	  	seg_geom AS arnold_geom,
	  	ST_Intersection(ST_Buffer(osm_geom, 4), ST_Buffer(seg_geom, 20)) AS intersection_geom,
	    ST_Area(ST_Intersection(ST_Buffer(osm_geom, 4), ST_Buffer(seg_geom, 20))) AS overlap_area,
	    ST_Buffer(seg_geom, 20) AS arnold_shape,
	    ST_Area(ST_Buffer(seg_geom, 20)) AS arnold_area,
	    ST_Buffer(osm_geom, 4) AS osm_shape,
	    ST_Area(ST_Buffer(osm_geom, 4)) AS osm_area
	FROM  ranked_roads
	WHERE rank = 1
	AND ST_Area(ST_Intersection(ST_Buffer(osm_geom, 4), ST_Buffer(seg_geom, 25))) > ST_Area(ST_Buffer(osm_geom, 4)) * .5
	AND osm_id NOT IN(
		SELECT osm_sw_id FROM ch1_automated.entrance_conflation
	) AND osm_id NOT IN (
		SELECT osm_id FROM osm_connecting_links
	);
	
END $$;




-- Creates the OSM corner to ARNOLD road conflation table --
-- Corners are defined as OSM sidewalk segments which are small in size and represent where a corner is. 
-- Geomtrically, this means that a corner segment is connected to 2 distinct sidewalk segements at each endpoint.
-- Corners inherit the ARNOLD road associations of the 2 OSM sidewalks they are connected to.
CREATE OR REPLACE PROCEDURE corner_to_road_conflation()
LANGUAGE plpgsql AS $$

BEGIN 
	
	CREATE TABLE ch1_automated.corner_conflation (
		osm_label TEXT,
	    osm_id INT8,
	    osm_geom GEOMETRY(LineString, 3857),
	    arnold_road1_route_id VARCHAR(75),
	    arnold_road1_geom GEOMETRY(LineString, 3857),
	    arnold_road2_route_id VARCHAR(75),
	    arnold_road2_geom GEOMETRY(LineString, 3857)
	);
	

	INSERT INTO ch1_automated.corner_conflation (
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
	    conflated_sw1.arnold_route_id AS arnold_road1_route_id,
	    conflated_sw1.arnold_geom AS arnold_road1_geom, 
	    conflated_sw2.arnold_route_id AS arnold_road2_route_id,
	    conflated_sw2.arnold_geom AS arnold_road2_geom
	FROM osm_sw AS sw
	JOIN ch1_automated.sidewalk_conflation AS conflated_sw1
	    ON ST_Intersects(ST_StartPoint(sw.geom), conflated_sw1.osm_geom)
	JOIN ch1_automated.sidewalk_conflation AS conflated_sw2
	    ON ST_Intersects(ST_EndPoint(sw.geom), conflated_sw2.osm_geom)
	WHERE sw.geom NOT IN (
		SELECT osm_geom
		FROM ch1_automated.sidewalk_conflation
	) AND (
		conflated_sw1.osm_id != conflated_sw2.osm_id
	) AND (
		conflated_sw1.arnold_route_id != conflated_sw2.arnold_route_id
	) AND ( 
		ST_Length(sw.geom) < 12
	); 
	
END $$;





-- Creates a table of all OSM sidewalks that did not conflate from any of the above initial processes
-- NOTE: crossings and roads are not included in this table
CREATE OR REPLACE PROCEDURE weird_case_table()
LANGUAGE plpgsql AS $$

BEGIN
	
	CREATE TABLE ch1_automated.weird_cases AS
	SELECT sw.osm_id, sw.geom
	FROM osm_sw AS sw
	WHERE osm_id NOT IN (
		SELECT osm_id FROM ch1_automated.sidewalk_conflation
	) AND osm_id NOT IN (
	    SELECT osm_id FROM ch1_automated.corner_conflation
	) AND osm_id NOT IN (
		SELECT osm_sw_id FROM ch1_automated.entrance_conflation
	) AND osm_id NOT IN (
		SELECT osm_cl_id FROM ch1_automated.connecting_link_conflation
	);
	
END $$;




-- Entire process for creating the weird case conflation tables for connecting links, corners, and sidewalks --
-- This process is done by taking the osm sidewalk geometries that did not conflate (weird cases), breaking them up into smaller subsegments,
-- and then checking to see if our own, smaller, segmentation can be associated to ARNOLD roads
-- NOTE: segmentation maintains original OSM id, but we also add a column numbering the subsegment ie.) osm id 12345 can have segments 1,2,3,... etc.
CREATE OR REPLACE PROCEDURE weird_case_conflation()
LANGUAGE plpgsql AS $$

BEGIN
	
	-- splitting OSM sidewalk linestring geometries, identified as weird cases, by vertex
	CREATE TEMP TABLE weird_case_segments AS
		WITH segments AS (
			SELECT osm_id,
			ROW_NUMBER() OVER (
			   PARTITION BY osm_id 
			   ORDER BY osm_id, (pt).path)-1 AS osm_segment_number,
			       (ST_MakeLine(lag((pt).geom, 1, NULL) OVER (PARTITION BY osm_id ORDER BY osm_id, (pt).path), (pt).geom)) AS geom
			FROM (SELECT osm_id, ST_DumpPoints(geom) AS pt FROM ch1_automated.weird_cases) AS dumps )
		SELECT * FROM segments WHERE geom IS NOT NULL;
		
	
	-- applying the weird cases to connecting link conflation process
	CREATE TABLE ch1_automated.weird_connecting_link_conflation AS
		WITH connlink_rn AS (
			SELECT DISTINCT ON 
				( link.osm_id, link.osm_segment_number, crossing.arnold_route_id )  
				link.osm_id AS osm_cl_id, 
				link.osm_segment_number, 
				crossing.arnold_route_id AS arnold_route_id, 
				crossing.arnold_geom AS arnold_geom,
				link.geom AS osm_geom, 
				crossing.osm_geom AS cross_geom
		    FROM ch1_automated.crossing_conflation AS crossing
		    JOIN weird_case_segments AS link
		    ON ST_Intersects(crossing.osm_geom, st_startpoint(link.geom)) OR ST_Intersects(crossing.osm_geom, st_endpoint(link.geom))
		    WHERE ST_length(link.geom) < 12)
		SELECT 
			'connecting link' AS osm_label, 
			osm_cl_id, 
			osm_segment_number, 
			osm_geom, 
			arnold_route_id, 
			arnold_geom
		FROM connlink_rn;
		
		
		
	-- applying the weird cases to sidewalk conflation
	CREATE TABLE ch1_automated.weird_sidewalk_conflation AS
	WITH ranked_roads AS (
		SELECT
			sidewalk.osm_id AS osm_id,
		  	big_road.route_id AS arnold_route_id,
		  	sidewalk.geom AS osm_geom,
		  	sidewalk.osm_segment_number AS osm_segment_number,
		 	
		  	-- segmentation of arnold road geometries
		  	ST_LineSubstring( big_road.geom, LEAST(ST_LineLocatePoint(big_road.geom, ST_ClosestPoint(st_startpoint(sidewalk.geom), big_road.geom)) ,
		  		ST_LineLocatePoint(big_road.geom, ST_ClosestPoint(st_endpoint(sidewalk.geom), big_road.geom))),
		  		GREATEST(ST_LineLocatePoint(big_road.geom, ST_ClosestPoint(st_startpoint(sidewalk.geom), big_road.geom)) ,
		  		ST_LineLocatePoint(big_road.geom, ST_ClosestPoint(st_endpoint(sidewalk.geom), big_road.geom))) ) AS seg_geom,
		  	
		  	--  ranking based on the distance between OSM sidewalk geometries and ARNOLD road geometries
		  	ROW_NUMBER() OVER (
		  		PARTITION BY sidewalk.geom
		  		ORDER BY ST_distance( ST_LineSubstring( big_road.geom, LEAST(ST_LineLocatePoint(big_road.geom,
		  			ST_ClosestPoint(st_startpoint(sidewalk.geom), big_road.geom)) , ST_LineLocatePoint(big_road.geom,
		  			ST_ClosestPoint(st_endpoint(sidewalk.geom), big_road.geom))), GREATEST(ST_LineLocatePoint(big_road.geom,
		  			ST_ClosestPoint(st_startpoint(sidewalk.geom), big_road.geom)) , ST_LineLocatePoint(big_road.geom,
		  			ST_ClosestPoint(st_endpoint(sidewalk.geom), big_road.geom))) ), sidewalk.geom )
		  	) AS RANK
		  	
		FROM weird_case_segments AS sidewalk
		JOIN arnold_lines AS big_road ON ST_Intersects(ST_Buffer(sidewalk.geom, 5), ST_Buffer(big_road.geom, 18))
		WHERE (
			-- joins where angle similarity is within a threshold of being parallel
			-- threshold is set to 10 degrees
			ABS(DEGREES(ST_Angle(ST_LineSubstring( big_road.geom, LEAST(ST_LineLocatePoint(big_road.geom, 
				ST_ClosestPoint(st_startpoint(sidewalk.geom), big_road.geom)) , ST_LineLocatePoint(big_road.geom, 
				ST_ClosestPoint(st_endpoint(sidewalk.geom), big_road.geom))), GREATEST(ST_LineLocatePoint(big_road.geom, 
				ST_ClosestPoint(st_startpoint(sidewalk.geom), big_road.geom)) , ST_LineLocatePoint(big_road.geom, 
				ST_ClosestPoint(st_endpoint(sidewalk.geom), big_road.geom))) ), sidewalk.geom))) BETWEEN 0 AND 10 -- 0 degrees
			OR ABS(DEGREES(ST_Angle(ST_LineSubstring( big_road.geom, LEAST(ST_LineLocatePoint(big_road.geom,
				ST_ClosestPoint(st_startpoint(sidewalk.geom), big_road.geom)) , ST_LineLocatePoint(big_road.geom,
				ST_ClosestPoint(st_endpoint(sidewalk.geom), big_road.geom))), GREATEST(ST_LineLocatePoint(big_road.geom,
				ST_ClosestPoint(st_startpoint(sidewalk.geom), big_road.geom)) , ST_LineLocatePoint(big_road.geom,
				ST_ClosestPoint(st_endpoint(sidewalk.geom), big_road.geom))) ), sidewalk.geom))) BETWEEN 170 AND 190 -- 180 degrees
			OR ABS(DEGREES(ST_Angle(ST_LineSubstring( big_road.geom, LEAST(ST_LineLocatePoint(big_road.geom,
				ST_ClosestPoint(st_startpoint(sidewalk.geom), big_road.geom)) , ST_LineLocatePoint(big_road.geom,
				ST_ClosestPoint(st_endpoint(sidewalk.geom), big_road.geom))), GREATEST(ST_LineLocatePoint(big_road.geom,
				ST_ClosestPoint(st_startpoint(sidewalk.geom), big_road.geom)) , ST_LineLocatePoint(big_road.geom,
				ST_ClosestPoint(st_endpoint(sidewalk.geom), big_road.geom))) ), sidewalk.geom))) BETWEEN 350 AND 360 ) -- 360 degrees
	    AND (sidewalk.osm_id, sidewalk.osm_segment_number) NOT IN (
	    	SELECT osm_cl_id, osm_segment_number
	      	FROM ch1_automated.weird_connecting_link_conflation
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
	ORDER BY osm_id, osm_segment_number;
	


	-- in the case where a connecting link segment and general case segment are both conflated and share the same osm_id, the segments in between that
	-- are NOT conflated will inherit the road conflation the conflated sidewalk and connecting link share. 
	INSERT INTO ch1_automated.weird_sidewalk_conflation (osm_label, osm_id, osm_segment_number, arnold_route_id, osm_geom, arnold_geom)
	WITH min_max_segments AS (
		SELECT 
			sw.osm_id, 
			MIN(LEAST(sw.osm_segment_number, link.osm_segment_number)) AS min_segment, 
			MAX(GREATEST(sw.osm_segment_number, link.osm_segment_number)) AS max_segment, 
			sw.arnold_route_id
		FROM ch1_automated.weird_sidewalk_conflation AS sw
		JOIN ch1_automated.weird_connecting_link_conflation AS link
			ON sw.osm_id = link.osm_cl_id AND sw.arnold_route_id = link.arnold_route_id
		GROUP BY sw.osm_id, sw.arnold_route_id
	)
	SELECT 
		'sidewalk' AS osm_label,
		seg_sw.osm_id, 
		seg_sw.osm_segment_number,  
		mms.arnold_route_id, 
		seg_sw.geom,
		ST_LineSubstring( road.geom, LEAST(ST_LineLocatePoint(road.geom, ST_ClosestPoint(st_startpoint(seg_sw.geom), road.geom)),
			ST_LineLocatePoint(road.geom, ST_ClosestPoint(st_endpoint(seg_sw.geom), road.geom))), GREATEST(ST_LineLocatePoint(road.geom,
			ST_ClosestPoint(st_startpoint(seg_sw.geom), road.geom)) , ST_LineLocatePoint(road.geom, ST_ClosestPoint(st_endpoint(seg_sw.geom),
			road.geom))) ) AS seg_geom
	FROM weird_case_segments AS seg_sw
	JOIN min_max_segments AS mms 
		ON seg_sw.osm_id = mms.osm_id
	JOIN arnold_lines AS road
		ON mms.arnold_route_id = road.route_id
	WHERE seg_sw.osm_segment_number BETWEEN mms.min_segment AND mms.max_segment
	AND (seg_sw.osm_id, seg_sw.osm_segment_number) NOT IN (
		SELECT 
			osm_id, 
			osm_segment_number
		FROM ch1_automated.weird_sidewalk_conflation
	) AND (seg_sw.osm_id, seg_sw.osm_segment_number) NOT IN (
		SELECT 
			osm_cl_id, 
			osm_segment_number
		FROM ch1_automated.weird_connecting_link_conflation
	)
	ORDER BY seg_sw.osm_id, seg_sw.osm_segment_number;
	
	
		
		
	-- conflates the weird case segments that are between 2 different weird case segments that conflated to the same road, giving all the roads
	-- in between that road conflation as well. 
	INSERT INTO ch1_automated.weird_sidewalk_conflation (osm_label, osm_id, osm_segment_number, arnold_route_id, osm_geom, arnold_geom)
	WITH min_max_segments AS (
	  	SELECT 
	  		osm_id, 
	  		MIN(osm_segment_number) AS min_segment, 
	  		MAX(osm_segment_number) AS max_segment, 
	  		arnold_route_id
	  	FROM ch1_automated.weird_sidewalk_conflation
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
	FROM weird_case_segments AS seg_sw
	JOIN min_max_segments AS mms 
		ON seg_sw.osm_id = mms.osm_id
	JOIN arnold_lines AS road
		ON mms.arnold_route_id = road.route_id
	WHERE seg_sw.osm_segment_number BETWEEN mms.min_segment AND mms.max_segment
	AND (seg_sw.osm_id, seg_sw.osm_segment_number) NOT IN (
		SELECT osm_id, osm_segment_number
		FROM ch1_automated.weird_sidewalk_conflation 
	)
	AND (seg_sw.osm_id, seg_sw.osm_segment_number) NOT IN (
		SELECT osm_id, osm_segment_number
		FROM osm_footway_null_connecting_links 
	)
	ORDER BY seg_sw.osm_id, seg_sw.osm_segment_number; 
	
	
	
	-- conflate sidewalks that are parallel to the already conflated sidewalk
	INSERT INTO ch1_automated.weird_sidewalk_conflation (osm_label, osm_id, osm_segment_number, arnold_route_id, osm_geom, arnold_geom)
	WITH ranked_road AS (
		SELECT DISTINCT 
			osm_sw.osm_id AS osm_id, 
			osm_sw.osm_segment_number AS osm_segment_number, 
			sidewalk.arnold_route_id AS arnold_route_id, 
			osm_sw.osm_geom AS osm_geom,
			ST_LineSubstring( big_road.geom, LEAST(ST_LineLocatePoint(big_road.geom, ST_ClosestPoint(st_startpoint(osm_sw.osm_geom), big_road.geom)),
				ST_LineLocatePoint(big_road.geom, ST_ClosestPoint(st_endpoint(osm_sw.osm_geom), big_road.geom))), 
				GREATEST(ST_LineLocatePoint(big_road.geom, ST_ClosestPoint(st_startpoint(osm_sw.osm_geom), big_road.geom)),
				ST_LineLocatePoint(big_road.geom, ST_ClosestPoint(st_endpoint(osm_sw.osm_geom), big_road.geom))) ) AS seg_geom,
				-- rank this based on the distance of the midpoint of the sidewalk to the midpoint of the road
				ROW_NUMBER() OVER (
					PARTITION BY osm_sw.osm_geom
					ORDER BY ST_distance( 
						ST_LineSubstring( big_road.geom, LEAST(ST_LineLocatePoint(big_road.geom, ST_ClosestPoint(st_startpoint(osm_sw.osm_geom),
							big_road.geom)) , ST_LineLocatePoint(big_road.geom, ST_ClosestPoint(st_endpoint(osm_sw.osm_geom), big_road.geom))),
						  	GREATEST(ST_LineLocatePoint(big_road.geom, ST_ClosestPoint(st_startpoint(osm_sw.osm_geom), big_road.geom)),
						  	ST_LineLocatePoint(big_road.geom, ST_ClosestPoint(st_endpoint(osm_sw.osm_geom), big_road.geom)))
						), osm_sw.osm_geom 
					)
				) AS RANK
		FROM ch1_automated.weird_sidewalk_conflation AS osm_sw
		JOIN ch1_automated.weird_sidewalk_conflation AS sidewalk
			ON 	ST_Intersects(st_startpoint(sidewalk.osm_geom), st_startpoint(osm_sw.osm_geom))
			OR ST_Intersects(st_startpoint(sidewalk.osm_geom), st_endpoint(osm_sw.osm_geom))
			OR ST_Intersects(st_endpoint(sidewalk.osm_geom), st_startpoint(osm_sw.osm_geom))
			OR ST_Intersects(st_endpoint(sidewalk.osm_geom), st_endpoint(osm_sw.osm_geom))
		JOIN arnold_lines AS big_road
			ON ST_Intersects(ST_Buffer(osm_sw.osm_geom, 5), ST_Buffer(osm_sw.osm_geom, 18))
		WHERE (osm_sw.osm_id, osm_sw.osm_segment_number) NOT IN (
			SELECT sw.osm_id, sw.osm_segment_number FROM ch1_automated.weird_sidewalk_conflation AS sw
			UNION ALL 
			SELECT link.osm_cl_id, link.osm_segment_number FROM ch1_automated.weird_connecting_link_conflation AS link
		) AND ( -- specify that the segment should be PARALLEL TO our conflated sidewalk
			ABS(DEGREES(ST_Angle(sidewalk.osm_geom, osm_sw.osm_geom))) BETWEEN 0 AND 20 -- 0 
			OR ABS(DEGREES(ST_Angle(sidewalk.osm_geom, osm_sw.osm_geom))) BETWEEN 160 AND 200 -- 180
			OR ABS(DEGREES(ST_Angle(sidewalk.osm_geom, osm_sw.osm_geom))) BETWEEN 340 AND 360  ) -- 360 
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
	WHERE RANK = 1;
	
	
	
	
	
	
	-- weid case corner conflation
	CREATE TABLE ch1_automated.weird_corner_conflation AS
		SELECT  corner.osm_id AS osm_id,
				corner.osm_segment_number AS osm_segment_number,
				road1.arnold_route_id AS arnold_road1_route_id,
				road2.arnold_route_id AS arnold_road2_route_id,
				corner.geom AS osm_geom
		FROM weird_case_segments AS corner
		JOIN (	SELECT * 
				FROM ch1_automated.weird_sidewalk_conflation
				UNION ALL
				SELECT osm_label, osm_id, 0 AS osm_segment_number, osm_geom, arnold_route_id, arnold_geom
				FROM ch1_automated.sidewalk_conflation
			 ) road1
			ON st_intersects(st_startpoint(corner.geom), road1.osm_geom)
		JOIN (  SELECT * 
				FROM ch1_automated.weird_sidewalk_conflation
				UNION ALL
				SELECT osm_label, osm_id, 0 AS osm_segment_number, osm_geom, arnold_route_id, arnold_geom
				FROM ch1_automated.sidewalk_conflation
			 ) road2
			ON st_intersects(st_endpoint(corner.geom), road2.osm_geom)
		WHERE   (corner.osm_id, corner.osm_segment_number) NOT IN (
						SELECT sw.osm_id, sw.osm_segment_number FROM ch1_automated.weird_sidewalk_conflation AS sw
						UNION ALL 
						SELECT link.osm_cl_id, link.osm_segment_number FROM ch1_automated.weird_connecting_link_conflation AS link
					)
				AND ST_Equals(road1.osm_geom, road2.osm_geom) IS FALSE
				AND road1.arnold_route_id != road2.arnold_route_id;

	
END $$;





----- MAIN PROCEDURES -----



CREATE OR REPLACE PROCEDURE osm_to_road_procedure()
LANGUAGE plpgsql AS $$

BEGIN
	
	CALL initialize_data_to_bb();
	CALL osm_road_add_lane_data();
	CALL road_to_road_conflation();
	CALL entrance_conflation();
	CALL crossing_to_road_conflation();
	CALL connecting_link_to_road_conflation();
	CALL sidewalk_to_road_conflation();
	CALL corner_to_road_conflation();
	CALL weird_case_table();
	CALL weird_case_conflation();
	
END $$;




CALL osm_to_road_procedure();




-- checkpoint --
SELECT * FROM ch1_automated.road_conflation;
SELECT * FROM ch1_automated.entrance_conflation;
SELECT * FROM ch1_automated.crossing_conflation;
SELECT * FROM ch1_automated.connecting_link_conflation;
SELECT * FROM ch1_automated.sidewalk_conflation;
SELECT * FROM ch1_automated.corner_conflation;
SELECT * FROM ch1_automated.weird_connecting_link_conflation;
SELECT * FROM ch1_automated.weird_sidewalk_conflation;
SELECT * FROM ch1_automated.weird_corner_conflation;

