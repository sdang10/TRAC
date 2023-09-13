-- INSERT DOCUMENTATION HERE --


CREATE OR REPLACE PROCEDURE initialize_data_to_bb()
LANGUAGE plpgsql AS $$
DECLARE

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
	CREATE TEMP TABLE osm_footway_null AS (
		SELECT *
		FROM shane_data_setup.osm_lines
		WHERE 	highway = 'footway' AND 
				(tags -> 'footway' IS NULL OR 
				tags -> 'footway' NOT IN ('sidewalk', 'crossing'))  AND
				geom && bb
	);

	-- imported database table --
	CREATE TEMP TABLE imported_data AS (
		SELECT *
 		FROM shane_data_setup.arnold_lines --------- CHANGE this TO be MORE flexible
		WHERE geom && bb
	);

END $$;





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

	LOOP
		
		-- increment counter
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





CREATE OR REPLACE PROCEDURE road_to_road_conflation()
LANGUAGE plpgsql AS $$

BEGIN
	
	CREATE TABLE ch1_automated.road_conflation AS 
	WITH ranked_roads AS (
		SELECT 
		imported_data.route_id AS imported_data_id, 
		imported_data.shape AS imported_data_shape,
		osm.osm_id AS osm_id, 
		osm.lanes AS lanes, 
		osm.osm_road_name AS osm_road_name,  
		osm.geom AS osm_geom,
		ST_LineSubstring( imported_data.geom, LEAST(ST_LineLocatePoint(imported_data.geom, ST_ClosestPoint(st_startpoint(osm.geom), imported_data.geom)), 
			ST_LineLocatePoint(imported_data.geom, ST_ClosestPoint(st_endpoint(osm.geom), imported_data.geom))), GREATEST(ST_LineLocatePoint(imported_data.geom,
			ST_ClosestPoint(st_startpoint(osm.geom), imported_data.geom)) , ST_LineLocatePoint(imported_data.geom, ST_ClosestPoint(st_endpoint(osm.geom), 
			imported_data.geom))) ) AS seg_geom,
		ROW_NUMBER() OVER (
	    	PARTITION BY osm.geom
	        ORDER BY 
	        ST_Area(ST_Intersection(ST_Buffer(osm.geom, lanes * 4), ST_Buffer(imported_data.geom, 1))) DESC
	        ) AS RANK,
	    ST_Intersection(ST_Buffer(osm.geom, lanes * 4 ), ST_Buffer(imported_data.geom, 1)) AS intersection_geom,
	    ST_Area(ST_Intersection(ST_Buffer(osm.geom, lanes * 4 ), ST_Buffer(imported_data.geom, 1))) AS overlap_area
		FROM osm_roads_add_lanes AS osm
		RIGHT JOIN imported_data AS imported_data
		ON ST_Intersects(ST_buffer(osm.geom, (osm.lanes) * 2), imported_data.geom)
		WHERE (  ABS(DEGREES(ST_Angle(ST_LineSubstring( imported_data.geom, LEAST(ST_LineLocatePoint(imported_data.geom, 
			ST_ClosestPoint(st_startpoint(osm.geom), imported_data.geom)) , ST_LineLocatePoint(imported_data.geom, ST_ClosestPoint(st_endpoint(osm.geom), imported_data.geom))),
			GREATEST(ST_LineLocatePoint(imported_data.geom, ST_ClosestPoint(st_startpoint(osm.geom), imported_data.geom)) , ST_LineLocatePoint(imported_data.geom, 
			ST_ClosestPoint(st_endpoint(osm.geom), imported_data.geom))) ), osm.geom))) BETWEEN 0 AND 10 -- 0 
		OR ABS(DEGREES(ST_Angle(ST_LineSubstring( imported_data.geom,
			LEAST(ST_LineLocatePoint(imported_data.geom, ST_ClosestPoint(st_startpoint(osm.geom), imported_data.geom)) , ST_LineLocatePoint(imported_data.geom, 
			ST_ClosestPoint(st_endpoint(osm.geom), imported_data.geom))), GREATEST(ST_LineLocatePoint(imported_data.geom, ST_ClosestPoint(st_startpoint(osm.geom),
			imported_data.geom)) , ST_LineLocatePoint(imported_data.geom, ST_ClosestPoint(st_endpoint(osm.geom), imported_data.geom))) ), osm.geom))) BETWEEN 170 AND 190 -- 180
		OR ABS(DEGREES(ST_Angle(ST_LineSubstring( imported_data.geom, LEAST(ST_LineLocatePoint(imported_data.geom, ST_ClosestPoint(st_startpoint(osm.geom), imported_data.geom)),
			ST_LineLocatePoint(imported_data.geom, ST_ClosestPoint(st_endpoint(osm.geom), imported_data.geom))), 
			GREATEST(ST_LineLocatePoint(imported_data.geom, ST_ClosestPoint(st_startpoint(osm.geom), imported_data.geom)), 
			ST_LineLocatePoint(imported_data.geom, ST_ClosestPoint(st_endpoint(osm.geom), imported_data.geom))) ), osm.geom))) BETWEEN 350 AND 360 ) -- 360
	)
	SELECT
		osm_id,
		osm_road_name,
		lanes,
		osm_geom,
		imported_data_id,
		RANK,
		seg_geom AS imported_data_geom,
		imported_data_shape AS imported_data_shape,
		intersection_geom,
		overlap_area
	FROM ranked_roads
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
		ON ST_intersects(sw.geom, point.geom); -- count: 1
	
END $$;





CREATE OR REPLACE PROCEDURE crossing_to_road_conflation()
LANGUAGE plpgsql AS $$

BEGIN 
	
	CREATE TABLE ch1_automated.crossing_conflation (
		osm_label TEXT,
		osm_id INT8,
		osm_geom GEOMETRY(LineString, 3857),
		imported_data_id VARCHAR(75),
		imported_data_geom GEOMETRY(LineString, 3857)
	);
	
	INSERT INTO ch1_automated.crossing_conflation (
		osm_label, 
		osm_id, 
		osm_geom, 
		imported_data_id, 
		imported_data_geom
	) SELECT DISTINCT ON (crossing.osm_id, road.route_id)
		'crossing' AS osm_label,
		crossing.osm_id AS osm_id, 
		crossing.geom AS osm_geom,
		road.route_id AS imported_data_id,
		road.geom AS imported_data_geom
	FROM osm_crossing AS crossing
	JOIN imported_data AS road 
		ON ST_Intersects(crossing.geom, road.geom);
		
		
	-- insert crossings that conflate from the osm road conflation
	INSERT INTO ch1_automated.crossing_conflation (
		osm_label, 
		osm_id, 
		osm_geom, 
		imported_data_id, 
		imported_data_geom
	) SELECT DISTINCT ON (crossing.osm_id, road.imported_data_id) 
		'crossing' AS osm_label,
		crossing.osm_id AS osm_id, 
		crossing.geom AS osm_geom,
		road.imported_data_id AS imported_data_id,
		road.imported_data_geom AS imported_data_geom
	FROM osm_crossing AS crossing
	JOIN ch1_automated.road_conflation AS road
		ON ST_Intersects(crossing.geom, road.osm_geom)
	WHERE crossing.osm_id NOT IN (
		SELECT osm_id
		FROM ch1_automated.crossing_conflation
	);
	
END $$;





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
		imported_data_id VARCHAR(75),
		imported_data_geom GEOMETRY(LineString, 3857)
	);

	INSERT INTO ch1_automated.connecting_link_conflation (
		osm_label, 
		osm_cl_id, 
		osm_cl_geom, 
		osm_crossing_id, 
		osm_crossing_geom, 
		imported_data_id, 
		imported_data_geom
	) SELECT DISTINCT ON (cl.osm_id, crossing.imported_data_id)
	    'connecting link' AS osm_label,
	    cl.osm_id AS osm_cl_id,
	    cl.geom AS osm_cl_geom,
	    crossing.osm_id AS osm_crossing_id,
	    crossing.osm_geom AS osm_crossing_geom,
	    crossing.imported_data_id AS imported_data_id,
	    crossing.imported_data_geom AS imported_data_geom
	FROM osm_connecting_links AS cl
	JOIN ch1_automated.crossing_conflation AS crossing
	    ON ST_Intersects(cl.geom, crossing.osm_geom); 
	
	    
	INSERT INTO ch1_automated.connecting_link_conflation (
		osm_label, 
		osm_cl_id,
		osm_cl_geom, 
		osm_crossing_id, 
		osm_crossing_geom, 
		imported_data_id, 
		imported_data_geom
	) SELECT DISTINCT ON (cl.osm_id, crossing.imported_data_id)
	    'connecting link' AS osm_label,
	    cl.osm_id AS osm_cl_id,
	    cl.geom AS osm_cl_geom,
	    crossing.osm_id AS osm_crossing_id,
	    crossing.osm_geom AS osm_crossing_geom,
	    crossing.imported_data_id AS imported_data_id,
	    crossing.imported_data_geom AS imported_data_geom
	FROM osm_footway_null_connecting_links AS cl
	JOIN ch1_automated.crossing_conflation AS crossing
	    ON ST_Intersects(cl.geom, crossing.osm_geom);
	
END $$;





CREATE OR REPLACE PROCEDURE sidewalk_to_road_conflation()
LANGUAGE plpgsql AS $$

BEGIN
	
	CREATE TABLE ch1_automated.sidewalk_conflation AS
	WITH ranked_roads AS (
		SELECT
			sw.osm_id AS osm_id,
			sw.geom AS osm_geom,
			road.route_id AS imported_data_id,
			road.geom AS imported_data_geom,
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
		  		ORDER BY 
		  			ST_distance(ST_LineSubstring( road.geom, LEAST(ST_LineLocatePoint(road.geom, ST_ClosestPoint(st_startpoint(sw.geom), road.geom)), 
		  				ST_LineLocatePoint(road.geom, ST_ClosestPoint(st_endpoint(sw.geom), road.geom))),
		  				GREATEST(ST_LineLocatePoint(road.geom, ST_ClosestPoint(st_startpoint(sw.geom), road.geom)), 
		  				ST_LineLocatePoint(road.geom, ST_ClosestPoint(st_endpoint(sw.geom), road.geom)))), sw.geom
		  			)
		  	) AS RANK
		FROM osm_sw AS sw
		JOIN imported_data AS road 
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
		)
	SELECT DISTINCT
		'sidewalk' AS osm_label,
	  	osm_id,
	  	osm_geom,
	  	imported_data_id,
	  	seg_geom AS imported_data_geom,
	  	ST_Intersection(ST_Buffer(osm_geom, 4), ST_Buffer(seg_geom, 20)) AS intersection_geom,
	    ST_Area(ST_Intersection(ST_Buffer(osm_geom, 4), ST_Buffer(seg_geom, 20))) AS overlap_area,
	    ST_Buffer(seg_geom, 20) AS imported_data_shape,
	    ST_Area(ST_Buffer(seg_geom, 20)) AS imported_data_area,
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





CREATE OR REPLACE PROCEDURE corner_to_road_conflation()
LANGUAGE plpgsql AS $$

BEGIN 
	
	CREATE TABLE ch1_automated.corner_conflation (
		osm_label TEXT,
	    osm_id INT8,
	    osm_geom GEOMETRY(LineString, 3857),
	    imported_data_road1_id VARCHAR(75),
	    imported_data_road1_geom GEOMETRY(LineString, 3857),
	    imported_data_road2_id VARCHAR(75),
	    imported_data_road2_geom GEOMETRY(LineString, 3857)
	);
	

	INSERT INTO ch1_automated.corner_conflation (
		osm_label, 
		osm_id, 
		osm_geom, 
		imported_data_road1_id, 
		imported_data_road1_geom,
		imported_data_road2_id, 
		imported_data_road2_geom
	) SELECT
		'corner'AS osm_label,
	    sw.osm_id AS osm_id,
	    sw.geom AS osm_geom,
	    conflated_sw1.imported_data_id AS imported_data_road1_id,
	    conflated_sw1.imported_data_geom AS imported_data_road1_geom, 
	    conflated_sw2.imported_data_id AS imported_data_road2_id,
	    conflated_sw2.imported_data_geom AS imported_data_road2_geom
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
		conflated_sw1.imported_data_id != conflated_sw2.imported_data_id
	) AND ( 
		ST_Length(sw.geom) < 12
	); 
	
END $$;





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





CREATE OR REPLACE PROCEDURE weird_case_conflation()
LANGUAGE plpgsql AS $$

BEGIN
	
	-- split the weird case linestrings into segments by linestring vertex
	CREATE TEMP TABLE weird_case_segments AS
		WITH segments AS (
			SELECT osm_id,
			ROW_NUMBER() OVER (
			   PARTITION BY osm_id 
			   ORDER BY osm_id, (pt).path)-1 AS osm_segment_number,
			       (ST_MakeLine(lag((pt).geom, 1, NULL) OVER (PARTITION BY osm_id ORDER BY osm_id, (pt).path), (pt).geom)) AS geom
			FROM (SELECT osm_id, ST_DumpPoints(geom) AS pt FROM ch1_automated.weird_cases) AS dumps )
		SELECT * FROM segments WHERE geom IS NOT NULL;
		
	
	-- segments that are connecting links
	CREATE TABLE ch1_automated.weird_connecting_link_conflation AS
		WITH connlink_rn AS (
			SELECT DISTINCT ON 
				( link.osm_id, link.osm_segment_number, crossing.imported_data_id )  
				link.osm_id AS osm_cl_id, 
				link.osm_segment_number, 
				crossing.imported_data_id AS imported_data_id, 
				crossing.imported_data_geom AS imported_data_geom,
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
			imported_data_id, 
			imported_data_geom
		FROM connlink_rn;
		
		
		
	-- apply weird case segments to general case
	CREATE TABLE ch1_automated.weird_sidewalk_conflation AS
	WITH ranked_roads AS (
		SELECT
			sidewalk.osm_id AS osm_id,
		  	big_road.route_id AS imported_data_id,
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
		FROM weird_case_segments AS sidewalk
		JOIN imported_data AS big_road ON ST_Intersects(ST_Buffer(sidewalk.geom, 5), ST_Buffer(big_road.geom, 18))
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
	    	SELECT osm_cl_id, osm_segment_number
	      	FROM ch1_automated.weird_connecting_link_conflation
	    )
	)
	SELECT
		'sidewalk' AS osm_label,
	  	osm_id,
	  	osm_segment_number,
	  	osm_geom,
	  	imported_data_id,
	  	seg_geom AS imported_data_geom
	FROM ranked_roads
	WHERE rank = 1
	ORDER BY osm_id, osm_segment_number;
	
	
	
		
		
	
	
	
	-- in the case where a connecting link segment and general case segment are both conflated and share the same osm_id, the segments in between that
	-- are NOT conflated will inherit the road conflation the conflated sidewalk and connecting link share. 
	INSERT INTO ch1_automated.weird_sidewalk_conflation (osm_label, osm_id, osm_segment_number, imported_data_id, osm_geom, imported_data_geom)
	WITH min_max_segments AS (
		SELECT 
			sw.osm_id, 
			MIN(LEAST(sw.osm_segment_number, link.osm_segment_number)) AS min_segment, 
			MAX(GREATEST(sw.osm_segment_number, link.osm_segment_number)) AS max_segment, 
			sw.imported_data_id
		FROM ch1_automated.weird_sidewalk_conflation AS sw
		JOIN ch1_automated.weird_connecting_link_conflation AS link
			ON sw.osm_id = link.osm_cl_id AND sw.imported_data_id = link.imported_data_id
		GROUP BY sw.osm_id, sw.imported_data_id
	)
	SELECT 
		'sidewalk' AS osm_label,
		seg_sw.osm_id, 
		seg_sw.osm_segment_number,  
		mms.imported_data_id, 
		seg_sw.geom,
		ST_LineSubstring( road.geom, LEAST(ST_LineLocatePoint(road.geom, ST_ClosestPoint(st_startpoint(seg_sw.geom), road.geom)) ,
			ST_LineLocatePoint(road.geom, ST_ClosestPoint(st_endpoint(seg_sw.geom), road.geom))), GREATEST(ST_LineLocatePoint(road.geom,
			ST_ClosestPoint(st_startpoint(seg_sw.geom), road.geom)) , ST_LineLocatePoint(road.geom, ST_ClosestPoint(st_endpoint(seg_sw.geom),
			road.geom))) ) AS seg_geom
	FROM weird_case_segments AS seg_sw
	JOIN min_max_segments AS mms 
		ON seg_sw.osm_id = mms.osm_id
	JOIN imported_data AS road
		ON mms.imported_data_id = road.route_id
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
	INSERT INTO ch1_automated.weird_sidewalk_conflation (osm_label, osm_id, osm_segment_number, imported_data_id, osm_geom, imported_data_geom)
	WITH min_max_segments AS (
	  	SELECT 
	  		osm_id, 
	  		MIN(osm_segment_number) AS min_segment, 
	  		MAX(osm_segment_number) AS max_segment, 
	  		imported_data_id
	  	FROM ch1_automated.weird_sidewalk_conflation
	  	GROUP BY osm_id, imported_data_id
	)
	SELECT 
		'sidewalk' AS osm_label,
		seg_sw.osm_id AS osm_id, 
		seg_sw.osm_segment_number AS osm_segment_number, 
		mms.imported_data_id AS imported_data_id, 
		seg_sw.geom AS osm_geom,
		ST_LineSubstring( road.geom, LEAST(ST_LineLocatePoint(road.geom, ST_ClosestPoint(st_startpoint(seg_sw.geom), road.geom)),
			ST_LineLocatePoint(road.geom, ST_ClosestPoint(st_endpoint(seg_sw.geom), road.geom))), 
			GREATEST(ST_LineLocatePoint(road.geom, ST_ClosestPoint(st_startpoint(seg_sw.geom), road.geom)),
			ST_LineLocatePoint(road.geom, ST_ClosestPoint(st_endpoint(seg_sw.geom), road.geom))) ) AS seg_geom
	FROM weird_case_segments AS seg_sw
	JOIN min_max_segments AS mms 
		ON seg_sw.osm_id = mms.osm_id
	JOIN imported_data AS road
		ON mms.imported_data_id = road.route_id
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
	INSERT INTO ch1_automated.weird_sidewalk_conflation (osm_label, osm_id, osm_segment_number, imported_data_id, osm_geom, imported_data_geom)
	WITH ranked_road AS (
		SELECT DISTINCT 
			osm_sw.osm_id AS osm_id, 
			osm_sw.osm_segment_number AS osm_segment_number, 
			sidewalk.imported_data_id AS imported_data_id, 
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
		JOIN imported_data AS big_road
			ON ST_Intersects(ST_Buffer(osm_sw.osm_geom, 5), ST_Buffer(osm_sw.osm_geom, 18))
		WHERE (osm_sw.osm_id, osm_sw.osm_segment_number) NOT IN (
			SELECT sw.osm_id, sw.osm_segment_number FROM ch1_automated.weird_sidewalk_conflation AS sw
			UNION ALL 
			SELECT link.osm_cl_id, link.osm_segment_number FROM ch1_automated.weird_connecting_link_conflation AS link
		) AND ( -- specify that the segment should be PARALLEL TO our conflated sidewalk
			ABS(DEGREES(ST_Angle(sidewalk.osm_geom, osm_sw.osm_geom))) BETWEEN 0 AND 20 -- 0 
			OR ABS(DEGREES(ST_Angle(sidewalk.osm_geom, osm_sw.osm_geom))) BETWEEN 160 AND 200 -- 180
			OR ABS(DEGREES(ST_Angle(sidewalk.osm_geom, osm_sw.osm_geom))) BETWEEN 340 AND 360  ) -- 360 
			AND big_road.route_id = sidewalk.imported_data_id
	)
	SELECT 
		'sidewalk' AS osm_label,
		osm_id, 
		osm_segment_number, 
		imported_data_id, 
		osm_geom, 
		seg_geom AS imported_data_geom
	FROM ranked_road
	WHERE RANK = 1;
	
	
	
	
	
	
	-- Step 3: Deal with corner
	CREATE TABLE ch1_automated.weird_corner_conflation AS
		SELECT  corner.osm_id AS osm_id,
				corner.osm_segment_number AS osm_segment_number,
				road1.imported_data_id AS imported_data_road1_id,
				road2.imported_data_id AS imported_data_road2_id,
				corner.geom AS osm_geom
		FROM weird_case_segments AS corner
		JOIN (	SELECT * 
				FROM ch1_automated.weird_sidewalk_conflation
				UNION ALL
				SELECT osm_label, osm_id, 0 AS osm_segment_number, osm_geom, imported_data_id, imported_data_geom
				FROM ch1_automated.sidewalk_conflation
			 ) road1
			ON st_intersects(st_startpoint(corner.geom), road1.osm_geom)
		JOIN (  SELECT * 
				FROM ch1_automated.weird_sidewalk_conflation
				UNION ALL
				SELECT osm_label, osm_id, 0 AS osm_segment_number, osm_geom, imported_data_id, imported_data_geom
				FROM ch1_automated.sidewalk_conflation
			 ) road2
			ON st_intersects(st_endpoint(corner.geom), road2.osm_geom)
		WHERE   (corner.osm_id, corner.osm_segment_number) NOT IN (
						SELECT sw.osm_id, sw.osm_segment_number FROM ch1_automated.weird_sidewalk_conflation AS sw
						UNION ALL 
						SELECT link.osm_cl_id, link.osm_segment_number FROM ch1_automated.weird_connecting_link_conflation AS link
					)
				AND ST_Equals(road1.osm_geom, road2.osm_geom) IS FALSE
				AND road1.imported_data_id != road2.imported_data_id;

	
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
