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
	
	CREATE TABLE automated.road_conflation AS 
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
		
	CREATE TABLE automated.entrance_conflation (
		osm_label TEXT,
		osm_sw_id INT8,
		osm_sw_geom GEOMETRY(LineString, 3857),
		osm_point_id INT8,
		point_tags hstore,
		osm_point_geom GEOMETRY(Point, 3857)
	);
	
	INSERT INTO automated.entrance_conflation (
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
	
	CREATE TABLE automated.crossing_conflation (
		osm_label TEXT,
		osm_id INT8,
		osm_geom GEOMETRY(LineString, 3857),
		imported_data_id VARCHAR(75),
		imported_data_geom GEOMETRY(LineString, 3857)
	);
	
	INSERT INTO automated.crossing_conflation (
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
	INSERT INTO automated.crossing_conflation (
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
	JOIN automated.road_conflation AS road
		ON ST_Intersects(crossing.geom, road.osm_geom)
	WHERE crossing.osm_id NOT IN (
		SELECT osm_id
		FROM automated.crossing_conflation
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

	CREATE TABLE automated.connecting_link_conflation (
		osm_label TEXT,
		osm_cl_id INT8,
		osm_cl_geom GEOMETRY(LineString, 3857),
		osm_crossing_id INT8,
		osm_crossing_geom GEOMETRY(LineString, 3857),
		imported_data_id VARCHAR(75),
		imported_data_geom GEOMETRY(LineString, 3857)
	);

	INSERT INTO automated.connecting_link_conflation (
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
	JOIN automated.crossing_conflation AS crossing
	    ON ST_Intersects(cl.geom, crossing.osm_geom); 
	
	    
	INSERT INTO automated.connecting_link_conflation (
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
	JOIN automated.crossing_conflation AS crossing
	    ON ST_Intersects(cl.geom, crossing.osm_geom);
	
END $$;





CREATE OR REPLACE PROCEDURE sidewalk_to_road_conflation()
LANGUAGE plpgsql AS $$

BEGIN
	
	CREATE TABLE automated.sidewalk_conflation AS
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
		SELECT osm_sw_id FROM automated.entrance_conflation
	) AND osm_id NOT IN (
		SELECT osm_id FROM osm_connecting_links
	);
	
END $$;





CREATE OR REPLACE PROCEDURE corner_to_road_conflation()
LANGUAGE plpgsql AS $$

BEGIN 
	
	CREATE TABLE automated.corner_conflation (
		osm_label TEXT,
	    osm_id INT8,
	    osm_geom GEOMETRY(LineString, 3857),
	    imported_data_road1_id VARCHAR(75),
	    imported_data_road1_geom GEOMETRY(LineString, 3857),
	    imported_data_road2_id VARCHAR(75),
	    imported_data_road2_geom GEOMETRY(LineString, 3857)
	);
	

	INSERT INTO automated.corner_conflation (
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
	    general_case1.imported_data_id AS imported_data_road1_id,
	    general_case1.imported_data_geom AS imported_data_road1_geom, 
	    general_case2.imported_data_id AS imported_data_road2_id,
	    general_case2.imported_data_geom AS imported_data_road2_geom
	FROM osm_sw AS sw
	JOIN automated.sidewalk_conflation AS general_case1
	    ON ST_Intersects(ST_StartPoint(sw.geom), general_case1.osm_geom)
	JOIN automated.sidewalk_conflation AS general_case2
	    ON ST_Intersects(ST_EndPoint(sw.geom), general_case2.osm_geom)
	WHERE sw.geom NOT IN (
		SELECT osm_geom
		FROM automated.sidewalk_conflation
	) AND (
		general_case1.osm_id != general_case2.osm_id
	) AND (
		general_case1.imported_data_id != general_case2.imported_data_id
	) AND ( 
		ST_Length(sw.geom) < 12
	); 
	
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
	
END $$;




CALL osm_to_road_procedure();




-- checkpoint --
SELECT * FROM automated.road_conflation;
SELECT * FROM automated.entrance_conflation;
SELECT * FROM automated.crossing_conflation;
SELECT * FROM automated.connecting_link_conflation;
SELECT * FROM automated.sidewalk_conflation;
SELECT * FROM automated.corner_conflation;

