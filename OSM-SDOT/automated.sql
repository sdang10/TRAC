--- THIS CODE CONTAINS THE SETUP AND PROCESS FOR THE CONFLATION BETWEEN OSM AND SDOT ---





-- FUNCTIONS --


-- this function will take an input of computed angle (_rad) between any 2 linestring, and an input of a tolerence angle threshold (_thresh)
-- to calculate whether or not the computed angle between 2 linestrings are within the threshold so we know if these 2 are parallel
CREATE OR REPLACE FUNCTION public.f_within_degrees(_rad DOUBLE PRECISION, _thresh integer) RETURNS boolean AS $$
    WITH m AS (SELECT mod(degrees(_rad)::NUMERIC, 180) AS angle)
        ,a AS (SELECT CASE WHEN m.angle > 90 THEN m.angle - 180 ELSE m.angle END AS angle FROM m)
    SELECT abs(a.angle) < _thresh FROM a;
$$ LANGUAGE SQL IMMUTABLE STRICT;








-- DATA SETUP --


-- This procedure pulls the necessary data from OSM and SDOT databases which are confined to a defined bounding box and creates tables
-- formatted for efficiency in the conflation process.

CREATE OR REPLACE PROCEDURE data_setup()
LANGUAGE plpgsql AS $$
DECLARE

	-- defined bounding box
	bb box2d := st_setsrid( st_makebox2d( st_makepoint(-13632381,6008135), st_makepoint(-13603222,6066353)), 3857);

	-- defined boundary buffer
	buffer integer = 15;

	

BEGIN 
	
	-- SDOT boundary
	CREATE TEMP TABLE sdot_boundary AS
	SELECT 
		ST_Buffer(
			ST_Union(
				ARRAY(
					SELECT wkb_geometry
					FROM sdot.censustract
				)
			), buffer
		) AS geom;
 
	-- index for better spatial query performance
	CREATE INDEX bound_geom ON sdot_boundary USING GIST (geom);
	
	SELECT * FROM sdot_sidewalk
	
	-- SDOT sidewalk
	CREATE TEMP TABLE sdot_sidewalk AS
		SELECT  
			ogc_fid,
			objectid,
			sw_width/39.37 AS sw_width,
			surftype, 
			primarycrossslope AS cross_slope,
			sw_category,
			(ST_Dump(wkb_geometry)).geom AS geom,
			ST_Length(wkb_geometry) AS length
		FROM sdot.sidewalks
		-- pulling only the SDOT data pertaining to sidewalk geometries that exist/have data within the defined bounding box
		WHERE st_astext(wkb_geometry) != 'LINESTRING EMPTY'
			AND surftype != 'UIMPRV'
			AND sw_width != 0;
		
	-- index for better spatial query performance
	CREATE INDEX sdot_geom ON sdot_sidewalk USING GIST (geom);


	
	-- SDOT access signals
	-- used in the crossing conflation to aid in defining crossing locations
	CREATE TEMP TABLE sdot_accpedsig AS
		SELECT *
		FROM sdot.accessible_pedestrian_signals AS aps;
		
	-- better naming convention
	ALTER TABLE sdot_accpedsig RENAME COLUMN wkb_geometry TO geom;
	
	
	
	-- OSM sidewalk
	CREATE TEMP TABLE osm_sidewalk AS
		SELECT 
			*, 
			ST_Length(way) AS length
		-- pulling only the OSM data pertaining to sidewalk geometries within the defined bounding box
		FROM planet_osm_line AS pol 
		WHERE highway='footway' 
		AND tags->'footway'='sidewalk' 
		AND way && bb;
	
	-- better naming convention
	ALTER TABLE osm_sidewalk RENAME COLUMN way TO geom;



	-- OSM data within SDOT boundary
	CREATE TEMP TABLE osm_sw_in_boundary AS
		SELECT DISTINCT sw.*
		FROM osm_sidewalk AS sw
		JOIN sdot_boundary AS b
		ON ST_Intersects(b.geom, sw.geom);
		   
	-- index for better spatial query performance
	CREATE INDEX sw_geom ON osm_sw_in_boundary USING GIST (geom);



	-- OSM crossing
	CREATE TEMP TABLE osm_crossing AS
		SELECT *
		FROM planet_osm_line AS pol 
		-- pulling only the OSM data pertaining to crossing geometries within the defined bounding box
		WHERE highway='footway' 
		AND tags->'footway'='crossing' 
		AND way && bb;
			
	-- better naming convention
	ALTER TABLE osm_crossing RENAME COLUMN way TO geom;



	-- OSM crossing in SDOT boundary
	CREATE TEMP TABLE osm_crossing_in_boundary AS
		SELECT DISTINCT sw.*
		FROM osm_crossing AS sw
		JOIN sdot_boundary AS b
		ON ST_Intersects(b.geom, sw.geom);

	-- index for better spatial query performance
	CREATE INDEX crossing_geom ON osm_crossing_in_boundary USING GIST (geom);
		    

END $$;





-- This procedure is the preprocessing of sidewalk geometries in OSM where we deal with sidewalk geometry cases that are closed off
-- meaning the start and endpoint of the linestring gemetry meet. One example case is when a sidewalk geometry is coded to wrap around an entire block. 
-- This procedure would take that linestring shaped like a square and break it up into 4 separate linestrings for the conflation process.
-- Therefore, we create our own segmentation breaks. The process is done in 3 main stages:
	-- 1. Check which OSM sidewalks are closed linestrings, break these OSM at vertices to sub-seg, number them by the order from 
	--    start to end points
	-- 2. Check if the sub-seg are adjacent and parallel to each other, we run a procedure to line them up 
	-- 3. For those that's not adjacent and parallel to others, we also want to insert them into the adjacent_parallel table
CREATE OR REPLACE PROCEDURE preprocess()
LANGUAGE plpgsql AS $$
DECLARE 

	_rec record;
	_first record;
	_prev record;
	_osm_id int8;

	angle_degree integer = 15;

BEGIN
	
	
	-- 1. Check which OSM sidewalks are closed linestrings, break these OSM at vertices to sub-seg, number them by the order from 
	-- 	  start to end points
	CREATE TEMP TABLE polygon_osm_break AS
		WITH segments AS (
		    SELECT 
		        osm_id,
		   		-- assigns number to segments of each segment from a given broken linestring
		        row_number() OVER (PARTITION BY osm_id ORDER BY osm_id, (pt).path) - 1 AS segment_number,
		        (ST_MakeLine(lag((pt).geom, 1, NULL) OVER (PARTITION BY osm_id ORDER BY osm_id, (pt).path), (pt).geom)) AS geom
		    FROM (
		        SELECT 
		        	osm_id, 
		            ST_DumpPoints(geom) AS pt 
		        FROM osm_sw_in_boundary
		        -- closed linestring defined as linestrings where startpoint and endpoint meet/equal each other
		        WHERE ST_StartPoint(geom) = ST_Endpoint(geom)
			) AS dumps 
		)
		SELECT *
		FROM segments
		WHERE geom IS NOT NULL;
	
	
	
	-- 2. Then check if the sub-seg are adjacent and parallel to each other, we run a procedure to line them up
	CREATE TEMP TABLE adjacent_lines AS
		SELECT 
			p1.osm_id AS osm_id,
			p1.segment_number AS seg_a,
			p1.geom AS geom_a,
			p2.segment_number AS seg_b,
			p2.geom AS geom_b
		FROM polygon_osm_break AS p1
		JOIN polygon_osm_break AS p2
			ON p1.osm_id = p2.osm_id 
			AND p1.segment_number < p2.segment_number 
			AND ST_Intersects(p1.geom, p2.geom)
		-- parallel is checked for and defined here with the value "angle_degree"
		WHERE  public.f_within_degrees(ST_Angle(p1.geom, p2.geom), angle_degree);
	
	
	
	CREATE TEMP TABLE adjacent_linestrings (
		osm_id int8 NOT NULL,
		start_seg int8 NOT NULL,
		end_seg int8 NOT NULL,
		geom GEOMETRY(linestring, 3857) NOT NULL
	);
		




	-- iterates over results and join adjacent segments
	-- NOTE: if doing for other tables, update to pass input and output tables and use dynamic SQL
	FOR _rec IN
		
		SELECT *
			FROM  adjacent_lines
			ORDER BY 
				osm_id ASC, 
				seg_a ASC,
				seg_b DESC -- sort seg_b IN descending order so that first/last segment is first on new osm_id
	LOOP 
		
		
		IF _first IS DISTINCT FROM NULL OR _prev IS DISTINCT FROM NULL THEN	-- _prev or _first record exists 
			-- check if current record is same osm_id as previous
			IF _rec.osm_id = _osm_id 
				THEN	-- working on same osm_id
		 			-- should we stich to the first or previous segment?
		 			IF _prev IS DISTINCT FROM NULL 
		 				THEN
		 					IF _rec.seg_a = _prev.seg_b 
		 						THEN 
			 						-- update previous record with info from current record
			 						_prev.geom_a := ST_Linemerge(ST_Union(_prev.geom_a,_prev.geom_b));
			 						_prev.geom_b := _rec.geom_b;
		 							_prev.seg_b := _rec.seg_b;
		 					ELSE
		 						-- segment is not a continuation of previous; write to table
			 					INSERT INTO adjacent_linestrings VALUES (_prev.osm_id, _prev.seg_a, _prev.seg_b, 
			 						ST_Linemerge(ST_Union(_prev.geom_a, _prev.geom_b)));
			 					-- set _prev record since 'new' line part
			 					_prev := _rec;
			 				END IF;
		 			ELSIF _first IS DISTINCT FROM NULL AND _rec.seg_a = _first.seg_a 
		 				THEN
		 					-- update _first record with info from current record
		 		    		_first.geom_a := ST_Linemerge(ST_Union(_first.geom_b,_first.geom_a));
		 					_first.geom_b := _rec.geom_b;
		 					_first.seg_a := _rec.seg_b;		 		
			 		ELSE
			 			-- set _prev record since 'new' line part
			 			_prev := _rec;
		 			END IF;
		    ELSE -- NEW osm_id
		    	-- set new osm_id
		    	_osm_id := _rec.osm_id;
		    	-- check for non-NULL first/last record
		    	-- NOTE: can assume existence of prev when appending first because geometry is a polygon that not all parts can be parallel
		    	IF _first IS DISTINCT FROM NULL 
		    		THEN
		    			IF _first.seg_b = _prev.seg_b 
		    				THEN
			    				-- merge _first with _prev and inverst min/max to indicate that it 'wrapped' around the end 
			    				_prev.geom_a := ST_Linemerge(ST_Union(_prev.geom_a,_first.geom_a));		-- e.g., 21 -> 24 + 25 -> 2
			    				_prev.geom_b := _first.geom_b;
		    					_prev.seg_b := _first.seg_a;	-- will 'wrap' around END
		    			ELSE
		    				-- insert first record if not appended with previous at end
			    			INSERT INTO adjacent_linestrings VALUES (_first.osm_id, _first.seg_b, _first.seg_a, 
			    				ST_Linemerge(ST_Union(_first.geom_a, _first.geom_b)));
			    		END IF;
		    	END IF;
		    	-- write final segment from previous osm_id to table
		    	INSERT INTO adjacent_linestrings VALUES (_prev.osm_id, _prev.seg_a, _prev.seg_b, 
		    		ST_Linemerge(ST_Union(_prev.geom_a, _prev.geom_b)));
		    	-- check if record is "first/last" record where seg_a is 1 and seg_b is not 2
			 	IF _rec.seg_a = 1 
			 	AND _rec.seg_b <> 2 
			 		THEN 
			    		-- set as first record and set prev to NULL
		    			_first := _rec;
		    			_prev := NULL;
		    	ELSE
		    		-- set _first to NULL and set _prev record since 'new' line part
		    		_first := NULL;
		 			_prev := _rec;
		    	END IF;
		 	END IF;
		ELSE -- first record, set _prev OR _first
			-- set new osm_id
			_osm_id := _rec.osm_id;
		 	-- check if first/last record
			IF _rec.seg_a = 1 
			AND _rec.seg_b <> 2 
				THEN 
		    		-- set as first record and set prev to NULL
		    	 	_first := _rec;
		    	 	_prev := NULL;
		   	ELSE
				-- set _first to NULL and set _prev record since 'new' line part
		   		_first := NULL;
		 		_prev := _rec;	   	
			END IF;
		END IF;
	
	
	END LOOP;
	
	-- check for first/last record; append to prev if matching; then insert final prev record
    IF _first IS DISTINCT FROM NULL 
    	THEN
    		IF _first.seg_b = _prev.seg_b 
    			THEN
					-- merge _first with _prev and inverst min/max to indicate that it 'wrapped' around the end 
					_prev.geom_a := ST_Linemerge(ST_Union(_prev.geom_a,_first.geom_a));		-- e.g., 21 -> 24 + 25 -> 2
					_prev.geom_b := _first.geom_b;
					_prev.seg_b := _first.seg_a;	-- will 'wrap' around END
			ELSE
				-- insert first record if not appended with previous at end
				INSERT INTO adjacent_linestrings VALUES (_first.osm_id, _first.seg_b, _first.seg_a, 
					ST_Linemerge(ST_Union(_first.geom_a, _first.geom_b)));
			END IF;
	END IF;
	-- write final segment from previous osm_id to table
	INSERT INTO adjacent_linestrings VALUES (_prev.osm_id, _prev.seg_a, _prev.seg_b,
		ST_Linemerge(ST_Union(_prev.geom_a, _prev.geom_b)));



	-- 3. Finally, for those that's not adjacent and parallel to others, we also want to insert them into the adjacent_parallel table
	INSERT INTO adjacent_linestrings
		SELECT 
			osm_id, 
			segment_number AS start_seg,
			segment_number AS end_seg, 
			geom
		FROM polygon_osm_break
		WHERE (osm_id, segment_number) NOT IN (
			SELECT 
				osm_id, 
				seg_a AS seg
			FROM adjacent_lines
			
			UNION ALL
			
			SELECT 
				osm_id, 
				seg_b AS seg
			FROM adjacent_lines
		); 


END	$$;





-- conflation process
CREATE OR REPLACE PROCEDURE sidewalk_to_sidewalk_conflation()
LANGUAGE plpgsql AS $$

DECLARE

	angle_degree integer = 15;
	
	-- buffer for when we only buffer one side of sidewalk 
	buffer1 integer = 18;

	-- buffer for when we buffer both sides of sidewalk
	buffer2 integer = 10;

	-- 0.15
	score_min float = 0.10;

	-- 0.4
	overlap_perc_min float = 0.4;

	

BEGIN
	
	
	-- create a table where we union all the osm sidewalks together
	CREATE TEMP TABLE osm_sidewalk_preprocessed AS (
		SELECT 
			osm_id, 
			CONCAT(start_seg, '-', end_seg) AS start_end_seg, 
			geom, 
			ST_Length(geom) AS length
		FROM adjacent_linestrings
		
		UNION ALL 
		
		SELECT 
			osm_id, 
			'null' AS start_end_seg, 
			geom, 
			ST_Length(geom) AS length
		FROM osm_sw_in_boundary
		WHERE NOT ST_IsClosed(geom)
	);

	
	CREATE INDEX osm_geom ON osm_sidewalk_preprocessed USING GIST (geom);
	
	
	
	-- conflation
	CREATE TEMP TABLE sdot2osm_sw_raw AS (
		WITH ranked_roads AS (
			SELECT 
				osm.osm_id AS osm_id,
			  	osm.start_end_seg AS start_end_seg,
			  	sdot.objectid AS sdot_objectid,
			  	sdot.sw_width,
			  	sdot.surftype, 
			  	sdot.cross_slope,
			  	sdot.sw_category,
			  	osm.geom AS osm_geom,
			  	sdot.geom AS sdot_geom,
				CASE
					-- when the OSM geometry is greater than the SDOT geometry, then the osm_seg is defined as the portion of the linestring
					-- cut off at the closest points to the start and endpoint of the SDOT geometry. Otherwise null since it doesn't need to be changed
					WHEN (sdot.length < osm.length) 
						THEN 
							ST_LineSubstring( osm.geom, LEAST(ST_LineLocatePoint(osm.geom, ST_ClosestPoint(st_startpoint(sdot.geom), osm.geom)), 
								ST_LineLocatePoint(osm.geom, ST_ClosestPoint(st_endpoint(sdot.geom), osm.geom))), 
								GREATEST(ST_LineLocatePoint(osm.geom, ST_ClosestPoint(st_startpoint(sdot.geom), osm.geom)),
								ST_LineLocatePoint(osm.geom, ST_ClosestPoint(st_endpoint(sdot.geom), osm.geom))) )
					ELSE NULL
			  	END AS osm_seg,
			  	CASE
				  	-- when the SDOT geometry is greater than the OSM geometry, then the sdot_seg is defined as the portion of the linestring
					-- cut off at the closest points to the start and endpoint of the OSM geometry. Otherwise null since it doesn't need to be changed
					WHEN (sdot.length > osm.length)
						THEN 
							ST_LineSubstring( sdot.geom, LEAST(ST_LineLocatePoint(sdot.geom, ST_ClosestPoint(st_startpoint(osm.geom),
								sdot.geom)) , ST_LineLocatePoint(sdot.geom, ST_ClosestPoint(st_endpoint(osm.geom), sdot.geom))),
								GREATEST(ST_LineLocatePoint(sdot.geom, ST_ClosestPoint(st_startpoint(osm.geom), sdot.geom)),
								ST_LineLocatePoint(sdot.geom, ST_ClosestPoint(st_endpoint(osm.geom), sdot.geom))) )
					ELSE NULL
			  	END AS sdot_seg,
			  	ROW_NUMBER() OVER (
			  		PARTITION BY (
			  			CASE
					  		WHEN (sdot.length > osm.length) 
					  			THEN  osm.geom
					  		ELSE sdot.geom
				  		END
			  		)
			  		ORDER BY
			  		-- ranks every SDOT2OSM comparison based on buffer coverage of whichever sidewalk is the smaller of SDOT and OSM. 
			  		CASE
					  	WHEN (sdot.length > osm.length)
					  		THEN 
					  			ST_Area(ST_Intersection(ST_Buffer(ST_LineSubstring( sdot.geom, LEAST(ST_LineLocatePoint(sdot.geom,
					  				ST_ClosestPoint(st_startpoint(osm.geom), sdot.geom)) , ST_LineLocatePoint(sdot.geom, 
					  				ST_ClosestPoint(st_endpoint(osm.geom), sdot.geom))), GREATEST(ST_LineLocatePoint(sdot.geom, 
					  				ST_ClosestPoint(st_startpoint(osm.geom), sdot.geom)) , ST_LineLocatePoint(sdot.geom, 
					  				ST_ClosestPoint(st_endpoint(osm.geom), sdot.geom))) ), buffer2, 'endcap=flat join=round'),
					  				ST_Buffer(osm.geom, buffer2, 'endcap=flat join=round'))) 
					  	ELSE
					  		ST_Area(ST_Intersection(ST_Buffer(ST_LineSubstring( osm.geom, LEAST(ST_LineLocatePoint(osm.geom, 
					  			ST_ClosestPoint(st_startpoint(sdot.geom), osm.geom)) , ST_LineLocatePoint(osm.geom, 
					  			ST_ClosestPoint(st_endpoint(sdot.geom), osm.geom))), GREATEST(ST_LineLocatePoint(osm.geom, 
					  			ST_ClosestPoint(st_startpoint(sdot.geom), osm.geom)) , ST_LineLocatePoint(osm.geom, 
					  			ST_ClosestPoint(st_endpoint(sdot.geom), osm.geom))) ), buffer2, 'endcap=flat join=round'),
					  			ST_Buffer(sdot.geom, buffer2, 'endcap=flat join=round'))) 
					END DESC
				)  AS RANK
			FROM osm_sidewalk_preprocessed AS osm
			JOIN sdot_sidewalk AS sdot
				ON ST_Intersects(ST_Buffer(sdot.geom, buffer2, 'endcap=flat join=round'), ST_Buffer(osm.geom, buffer2, 
					'endcap=flat join=round'))
			WHERE  -- the the sw geometries have a similarity IN angle (parallel to each other) WITH a leniency value OF "angle_degree"
				CASE
					WHEN (sdot.length > osm.length)
						THEN 
							public.f_within_degrees(ST_Angle(ST_LineSubstring( sdot.geom, LEAST(ST_LineLocatePoint(sdot.geom, 
								ST_ClosestPoint(st_startpoint(osm.geom), sdot.geom)) , ST_LineLocatePoint(sdot.geom, 
								ST_ClosestPoint(st_endpoint(osm.geom), sdot.geom))), GREATEST(ST_LineLocatePoint(sdot.geom, 
								ST_ClosestPoint(st_startpoint(osm.geom), sdot.geom)) , ST_LineLocatePoint(sdot.geom, 
								ST_ClosestPoint(st_endpoint(osm.geom), sdot.geom))) ), osm.geom), angle_degree)
					ELSE 
						public.f_within_degrees(ST_Angle(ST_LineSubstring( osm.geom, LEAST(ST_LineLocatePoint(osm.geom, 
							ST_ClosestPoint(st_startpoint(sdot.geom), osm.geom)) , ST_LineLocatePoint(osm.geom, 
							ST_ClosestPoint(st_endpoint(sdot.geom), osm.geom))), GREATEST(ST_LineLocatePoint(osm.geom, 
							ST_ClosestPoint(st_startpoint(sdot.geom), osm.geom)) , ST_LineLocatePoint(osm.geom, 
							ST_ClosestPoint(st_endpoint(sdot.geom), osm.geom))) ), sdot.geom), angle_degree) 
				END
		)
		SELECT
			osm_id,
			start_end_seg,
			sdot_objectid,
			sw_width,
			surftype,
			cross_slope,
			sw_category,
			CASE
				WHEN osm_seg IS NULL
			  		THEN 
			  			ST_Length(ST_Intersection(sdot_seg, ST_Buffer(osm_geom, buffer1,  
			  				'endcap=flat join=round')))/GREATEST(ST_Length(sdot_seg), ST_Length(osm_geom))
			  	ELSE 
			  		ST_Length(ST_Intersection(osm_seg, ST_Buffer(sdot_geom, buffer1,  
			  			'endcap=flat join=round')))/GREATEST(ST_Length(osm_seg), ST_Length(sdot_geom))
			END AS conflated_score, -- creates a ratio (0-1) based ON the buffer INTERSECTION length AND maximum length OF the two geometries
			osm_seg,
			osm_geom,
			sdot_seg,
			sdot_geom
		FROM ranked_roads
		WHERE rank = 1 
		AND
			-- applies filter to remove results with a conflation score less than the "score_min" (score minimum). This is to ensure the quality
			-- of our results as to set a minimum score we consider in moving forward with the process. Those below the score minimum we assume
			-- to be bad matchings
			CASE
				WHEN osm_seg IS NULL
			  		THEN 
			  			ST_Length(ST_Intersection(sdot_seg, ST_Buffer(osm_geom, buffer1,  
			  				'endcap=flat join=round')))/GREATEST(ST_Length(sdot_seg), ST_Length(osm_geom)) > score_min 
				ELSE
					ST_Length(ST_Intersection(osm_seg, ST_Buffer(sdot_geom, buffer1,  
						'endcap=flat join=round')))/GREATEST(ST_Length(osm_seg), ST_Length(sdot_geom)) > score_min 
			END 
	); 
		
		
	
	
	
	-- This step give us the table while filtering out overlapped segments
	-- If it overlapps, choose the one thats closer
	CREATE TEMP TABLE sdot2osm_sw_prepocessed AS (
		SELECT *
		FROM sdot2osm_sw_raw
		WHERE (CONCAT(osm_id, start_end_seg), sdot_objectid) NOT IN ( 
	    	-- case when the whole  already conflated to one osm, but then its subseg also conflated to another osm
			-- if these 2 osm overlap with at least the value of "overlap_perc_min" for the shorter one between 2 of them, 
			-- then we filter out the one that's further away from our sdot
	        SELECT 
				CONCAT(r1.osm_id, r1.start_end_seg),
				CASE
					WHEN r1.sdot_seg IS NULL 
					AND r2.sdot_seg IS NOT NULL
						THEN
							CASE
						  		WHEN ST_Distance(ST_LineInterpolatePoint(r1.osm_geom, 0.5), 
						  			ST_LineInterpolatePoint(r1.sdot_geom, 0.5)) < ST_Distance(ST_LineInterpolatePoint(r2.osm_geom, 0.5), 
						  			ST_LineInterpolatePoint(r2.sdot_seg, 0.5))
							    	THEN (r2.sdot_objectid)
							    ELSE (r1.sdot_objectid)
						 	END
					    	WHEN r1.sdot_seg IS NOT NULL 
					    	AND r2.sdot_seg IS NULL
					  			THEN
					  				CASE 
					  					WHEN ST_Distance(ST_LineInterpolatePoint(r1.osm_geom, 0.5), 
					  						ST_LineInterpolatePoint(r1.sdot_seg, 0.5)) < ST_Distance(ST_LineInterpolatePoint(r2.osm_geom, 0.5),
					  						ST_LineInterpolatePoint(r2.sdot_geom, 0.5))
							            	THEN (r2.sdot_objectid)
							        	ELSE (r1.sdot_objectid)
							  		END
									WHEN r1.sdot_seg IS NULL 
									AND r2.sdot_seg IS NULL
					  					THEN
					  						CASE 
					  							WHEN ST_Length(r1.osm_seg) > ST_Length(r2.osm_seg) 
					  								THEN
	 													CASE
	 														WHEN ST_Distance(ST_LineInterpolatePoint(r1.osm_seg, 0.5), 
	 															ST_LineInterpolatePoint(r1.sdot_geom, 0.5)) < 
	 															ST_Distance(ST_LineInterpolatePoint(r1.osm_seg, 0.5), 
	 															ST_LineInterpolatePoint(r2.sdot_geom, 0.5))
																THEN (r2.sdot_objectid)
															ELSE (r1.sdot_objectid)
	 													END
	 											ELSE 
							        				CASE
	 													WHEN ST_Distance(ST_LineInterpolatePoint(r2.osm_seg, 0.5), 
	 														ST_LineInterpolatePoint(r1.sdot_geom, 0.5)) < 
	 														ST_Distance(ST_LineInterpolatePoint(r2.osm_seg, 0.5), 
	 														ST_LineInterpolatePoint(r2.sdot_geom, 0.5))
															THEN (r2.sdot_objectid)
														ELSE (r1.sdot_objectid)
	 												END
							  				END
				END AS sdot_objectid
			FROM sdot2osm_sw_raw AS r1
			JOIN (
				SELECT *
			    FROM sdot2osm_sw_raw
			    WHERE (osm_id, start_end_seg) IN (
			    	SELECT 
			    		osm_id, 
			    		start_end_seg
			        FROM sdot2osm_sw_raw
			        GROUP BY 
			        	osm_id, 
			        	start_end_seg
			       	HAVING count(*) > 1 ) 
			) AS r2
				ON r1.osm_id = r2.osm_id 
				AND r1.start_end_seg = r2.start_end_seg 
				AND r1.sdot_objectid < r2.sdot_objectid
			WHERE 
				CASE
					WHEN r1.sdot_seg IS NULL 
					AND r2.sdot_seg IS NOT NULL
						THEN
					  		ST_Length(ST_Intersection (ST_LineSubstring(r2.osm_geom, LEAST(ST_LineLocatePoint(r2.osm_geom, 
					  			ST_ClosestPoint(st_startpoint(r2.sdot_seg), r2.osm_geom)) , ST_LineLocatePoint(r2.osm_geom, 
					  			ST_ClosestPoint(st_endpoint(r2.sdot_seg), r2.osm_geom))),GREATEST(ST_LineLocatePoint(r2.osm_geom, 
					  			ST_ClosestPoint(st_startpoint(r2.sdot_seg), r2.osm_geom)) , ST_LineLocatePoint(r2.osm_geom, 
					  			ST_ClosestPoint(st_endpoint(r2.sdot_seg), r2.osm_geom))) ), ST_Buffer(r1.osm_seg, buffer1, 
					  			'endcap=flat join=round'))) 
					  		/ LEAST(ST_Length(r1.osm_seg), ST_Length (ST_LineSubstring(r2.osm_geom,
					  			LEAST(ST_LineLocatePoint(r2.osm_geom, ST_ClosestPoint(st_startpoint(r2.sdot_seg), r2.osm_geom)), 
					  			ST_LineLocatePoint(r2.osm_geom, ST_ClosestPoint(st_endpoint(r2.sdot_seg), r2.osm_geom))),
					  			GREATEST(ST_LineLocatePoint(r2.osm_geom, ST_ClosestPoint(st_startpoint(r2.sdot_seg), r2.osm_geom)),
					  			ST_LineLocatePoint(r2.osm_geom, ST_ClosestPoint(st_endpoint(r2.sdot_seg), r2.osm_geom))) ) )) > overlap_perc_min 
					WHEN r1.sdot_seg IS NOT NULL 
					AND r2.sdot_seg IS NULL
						THEN 
				  	 		ST_Length(ST_Intersection (ST_LineSubstring(r1.osm_geom,LEAST(ST_LineLocatePoint(r1.osm_geom, 
				  	 			ST_ClosestPoint(st_startpoint(r1.sdot_seg), r1.osm_geom)) , ST_LineLocatePoint(r1.osm_geom, 
				  	 			ST_ClosestPoint(st_endpoint(r1.sdot_seg), r1.osm_geom))),GREATEST(ST_LineLocatePoint(r1.osm_geom, 
				  	 			ST_ClosestPoint(st_startpoint(r1.sdot_seg), r1.osm_geom)) , ST_LineLocatePoint(r1.osm_geom, 
				  	 			ST_ClosestPoint(st_endpoint(r1.sdot_seg), r1.osm_geom))) ), ST_Buffer(r2.osm_seg, buffer1, 
				  	 			'endcap=flat join=round')))
						  	/ LEAST(ST_Length(r2.osm_seg),ST_Length (ST_LineSubstring( r1.osm_geom, LEAST(ST_LineLocatePoint(r1.osm_geom, 
						  		ST_ClosestPoint(st_startpoint(r1.sdot_seg), r1.osm_geom)), ST_LineLocatePoint(r1.osm_geom, 
						  		ST_ClosestPoint(st_endpoint(r1.sdot_seg), r1.osm_geom))),GREATEST(ST_LineLocatePoint(r1.osm_geom, 
						  		ST_ClosestPoint(st_startpoint(r1.sdot_seg), r1.osm_geom)) , ST_LineLocatePoint(r1.osm_geom, 
						  		ST_ClosestPoint(st_endpoint(r1.sdot_seg), r1.osm_geom))) ) )) > overlap_perc_min
					WHEN r1.sdot_seg IS NULL 
					AND r2.sdot_seg IS NULL
				  		THEN 
				  	 		ST_Length(ST_Intersection(r1.osm_seg, ST_Buffer(r2.osm_seg, buffer1, 'endcap=flat join=round') ) )
				  	 	  	/ LEAST(ST_Length(r1.osm_seg), ST_Length(r2.osm_seg)) > overlap_perc_min	 	
				END

			UNION ALL
			
			-- case when the whole osm already conflated to one sdot, but then its subseg also conflated to another sdot
			-- if these 2 sdot overlap with at least the value of "overlap_perc_min" for the shorter one between 2 of them, 
			-- then we filter out the one that's further away from our osm
				SELECT 
					CASE
				    	WHEN r1.sdot_seg IS NOT NULL 
				    	AND r2.sdot_seg IS NULL 
				        	THEN 
				        		CASE
				        			WHEN ST_Distance(ST_LineInterpolatePoint(r1.osm_geom, 0.5), 
				        				ST_LineInterpolatePoint(r1.sdot_geom, 0.5)) < ST_Distance(ST_LineInterpolatePoint(r2.osm_seg, 0.5), 
				        				ST_LineInterpolatePoint(r2.sdot_geom, 0.5))
				            			THEN CONCAT(r2.osm_id, r2.start_end_seg)
				        			ELSE CONCAT(r1.osm_id, r1.start_end_seg)
				        		END
				        		WHEN r1.sdot_seg IS NULL 
				        		AND r2.sdot_seg IS NOT NULL 
				        			THEN 
				        				CASE
				        					WHEN ST_Distance(ST_LineInterpolatePoint(r1.osm_seg, 0.5), 
				        						ST_LineInterpolatePoint(r1.sdot_geom, 0.5)) < ST_Distance(ST_LineInterpolatePoint(r2.osm_geom, 
				        						0.5), ST_LineInterpolatePoint(r2.sdot_geom, 0.5))
				            					THEN CONCAT(r2.osm_id, r2.start_end_seg)
				        					ELSE CONCAT(r1.osm_id, r1.start_end_seg)
				        				END
				        				WHEN r1.sdot_seg IS NOT NULL 
				        				AND r2.sdot_seg IS NOT NULL 
				        					THEN
					  							CASE 
					  								WHEN ST_Length(r1.sdot_seg) > ST_Length(r2.sdot_seg) 
					  									THEN
	 														CASE
	 															WHEN ST_Distance(ST_LineInterpolatePoint(r1.osm_geom, 0.5), 
	 																ST_LineInterpolatePoint(r1.sdot_seg, 0.5)) < 
	 																ST_Distance(ST_LineInterpolatePoint(r2.osm_geom, 0.5), 
	 																ST_LineInterpolatePoint(r1.sdot_seg, 0.5))
																	THEN CONCAT(r2.osm_id, r2.start_end_seg)
																ELSE CONCAT(r1.osm_id, r1.start_end_seg)
	 														END
	 												ELSE 
							        					CASE
	 														WHEN ST_Distance(ST_LineInterpolatePoint(r1.osm_geom, 0.5), 
	 															ST_LineInterpolatePoint(r2.sdot_seg, 0.5)) < 
	 															ST_Distance(ST_LineInterpolatePoint(r2.osm_geom, 0.5), 
	 															ST_LineInterpolatePoint(r2.sdot_seg, 0.5))
																THEN CONCAT(r2.osm_id, r2.start_end_seg)
															ELSE CONCAT(r1.osm_id, r1.start_end_seg)
	 													END
							  					END
					END AS osm_id_seg,
				    r1.sdot_objectid
				FROM sdot2osm_sw_raw AS r1
				JOIN (
					SELECT *
			    	FROM sdot2osm_sw_raw
			        WHERE sdot_objectid IN (
			        	SELECT sdot_objectid
			            FROM sdot2osm_sw_raw
			            GROUP BY sdot_objectid
			            HAVING count(*) > 1 ) 
			    ) AS r2
					ON r1.sdot_objectid = r2.sdot_objectid 
					AND CONCAT(r1.osm_id, r1.start_end_seg) < CONCAT(r2.osm_id, r2.start_end_seg)
				WHERE
					CASE
						WHEN r1.sdot_seg IS NOT NULL 
						AND r2.sdot_seg IS NULL
							THEN 
								ST_Length(ST_Intersection(ST_LineSubstring(r2.sdot_geom,LEAST(ST_LineLocatePoint(r2.sdot_geom, 
									ST_ClosestPoint(st_startpoint(r2.osm_seg), r2.sdot_geom)), 
									ST_LineLocatePoint(r2.sdot_geom, ST_ClosestPoint(st_endpoint(r2.osm_seg), 
									r2.sdot_geom))),GREATEST(ST_LineLocatePoint(r2.sdot_geom, ST_ClosestPoint(st_startpoint(r2.osm_seg),
									r2.sdot_geom)) , ST_LineLocatePoint(r2.sdot_geom, ST_ClosestPoint(st_endpoint(r2.osm_seg),
									r2.sdot_geom))) ), ST_Buffer(r1.sdot_seg, buffer1, 'endcap=flat join=round')) )
						  		/ LEAST(ST_Length(r1.sdot_seg),ST_Length(ST_LineSubstring(r2.sdot_geom,LEAST(ST_LineLocatePoint(r2.sdot_geom, 
						  			ST_ClosestPoint(st_startpoint(r2.osm_seg), r2.sdot_geom)) , ST_LineLocatePoint(r2.sdot_geom, 
						  			ST_ClosestPoint(st_endpoint(r2.osm_seg), r2.sdot_geom))),GREATEST(ST_LineLocatePoint(r2.sdot_geom, 
						  			ST_ClosestPoint(st_startpoint(r2.osm_seg), r2.sdot_geom)) , ST_LineLocatePoint(r2.sdot_geom, 
						  			ST_ClosestPoint(st_endpoint(r2.osm_seg), r2.sdot_geom))) )))> overlap_perc_min	
			 			WHEN r1.sdot_seg IS NULL 
			 			AND r2.sdot_seg IS NOT NULL
							THEN 
								ST_Length(ST_Intersection(ST_LineSubstring(r1.sdot_geom,LEAST(ST_LineLocatePoint(r1.sdot_geom, 
									ST_ClosestPoint(st_startpoint(r1.osm_seg), r1.sdot_geom)) , ST_LineLocatePoint(r1.sdot_geom, 
									ST_ClosestPoint(st_endpoint(r1.osm_seg), r1.sdot_geom))),GREATEST(ST_LineLocatePoint(r1.sdot_geom, 
									ST_ClosestPoint(st_startpoint(r1.osm_seg), r1.sdot_geom)) , ST_LineLocatePoint(r1.sdot_geom, 
									ST_ClosestPoint(st_endpoint(r1.osm_seg), r1.sdot_geom))) ), ST_Buffer(r2.sdot_seg, buffer1,
									'endcap=flat join=round')) )
				  				/ LEAST(ST_Length(r2.sdot_seg),ST_Length(ST_LineSubstring(r1.sdot_geom,
				  					LEAST(ST_LineLocatePoint(r1.sdot_geom, ST_ClosestPoint(st_startpoint(r1.osm_seg), r1.sdot_geom)),
				  					ST_LineLocatePoint(r1.sdot_geom, ST_ClosestPoint(st_endpoint(r1.osm_seg), r1.sdot_geom))),
				  					GREATEST(ST_LineLocatePoint(r1.sdot_geom, ST_ClosestPoint(st_startpoint(r1.osm_seg), r1.sdot_geom)),
				  					ST_LineLocatePoint(r1.sdot_geom, ST_ClosestPoint(st_endpoint(r1.osm_seg), r1.sdot_geom))) ))) > overlap_perc_min
					  	WHEN r1.sdot_seg IS NOT NULL 
					  	AND r2.sdot_seg IS NOT NULL
							THEN 
								ST_Length(ST_Intersection(r1.sdot_seg, ST_Buffer(r2.sdot_seg, buffer1, 'endcap=flat join=round')) )
						  		/ LEAST(ST_Length(r1.sdot_seg), ST_Length(r2.sdot_seg)) > overlap_perc_min
					END
		)
	); 
	        
	
	
	
	-- necessary data pulled from the preprocessed data
	CREATE TEMP TABLE sidewalk AS
		SELECT 
			osm_id,
			start_end_seg,
			sdot_objectid,
			sw_width AS width,
			CASE 
				WHEN surftype ILIKE 'PCC%' 
					THEN 'concrete'
			    WHEN surftype ILIKE 'AC%' 
			    	THEN 'asphalt'
				ELSE 'paved'
			END AS sdot_surface,
			hstore(CAST('cross_slope' AS TEXT), CAST(cross_slope AS TEXT)) AS cross_slope,
			sw_category AS sdot_sw_category,
			conflated_score,
			-- checking if the segment is the original linestring geometry or if it is a subsegmentation of the original
			CASE
				WHEN osm_seg IS NOT NULL
					THEN 'no'
			    WHEN osm_seg IS NULL 
			    AND NOT ST_Equals(
			    	ST_LineSubstring( osm_geom, LEAST(ST_LineLocatePoint(osm_geom, ST_ClosestPoint(st_startpoint(sdot_seg), osm_geom)), 
			    		ST_LineLocatePoint(osm_geom, ST_ClosestPoint(st_endpoint(sdot_seg), osm_geom))), 
			    		GREATEST(ST_LineLocatePoint(osm_geom, ST_ClosestPoint(st_startpoint(sdot_seg), osm_geom)),
			    		ST_LineLocatePoint(osm_geom, ST_ClosestPoint(st_endpoint(sdot_seg), osm_geom))) ),osm_geom)
			    	THEN 'no'
			    ELSE 'yes'
			END AS original_way,
			CASE
				WHEN osm_seg IS NOT NULL
					THEN osm_seg
			    ELSE 
					ST_LineSubstring( osm_geom, LEAST(ST_LineLocatePoint(osm_geom, ST_ClosestPoint(st_startpoint(sdot_seg), osm_geom)),
						ST_LineLocatePoint(osm_geom, ST_ClosestPoint(st_endpoint(sdot_seg), osm_geom))),
						GREATEST(ST_LineLocatePoint(osm_geom, ST_ClosestPoint(st_startpoint(sdot_seg), osm_geom)),
						ST_LineLocatePoint(osm_geom, ST_ClosestPoint(st_endpoint(sdot_seg), osm_geom))))
			END AS way,
			osm_geom,
			sdot_geom
		FROM sdot2osm_sw_prepocessed;
	
	
	
	
	
	
	-- FINAL TABLE that will be exported
	CREATE TABLE automated.sidewalk_conflation_result AS (
		SELECT 
			sdot2osm.osm_id,
			sdot2osm.start_end_seg,
			sdot2osm.sdot_objectid,
			osm.highway,
			osm.surface,
			sdot2osm.sdot_surface,
			sdot2osm.width,
			osm.tags||sdot2osm.cross_slope AS tags,
			conflated_score,
			original_way,
			ST_Transform(sdot2osm.way, 4326) AS way 
		FROM sidewalk AS sdot2osm
		JOIN osm_sw_in_boundary AS osm
			ON sdot2osm.osm_id = osm.osm_id
		
		UNION
		
		SELECT  
			pre_osm.osm_id,
			pre_osm.start_end_seg,
			NULL AS sdot_objectid,
			osm.highway,
			osm.surface,
			NULL AS sdot_surface,
			NULL AS width,
			osm.tags,
			NULL AS conflated_score,
			CASE
				WHEN start_end_seg != 'null'
					THEN 'no'
				ELSE 'yes'
			END AS original_way,
				ST_Transform(pre_osm.geom, 4326) AS way
		FROM osm_sw_in_boundary AS osm
		JOIN osm_sidewalk_preprocessed AS pre_osm
			ON osm.osm_id = pre_osm.osm_id
		WHERE (pre_osm.osm_id, pre_osm.start_end_seg) NOT IN (
			SELECT DISTINCT 
				osm_id, 
				start_end_seg
			FROM sidewalk ) 
	);

END $$;
	   
	   
	   
	   
	   
-- crossing conflation
CREATE OR REPLACE PROCEDURE crossing_conflation()
LANGUAGE plpgsql AS $$

DECLARE 

	osm_buffer integer = 10;
	
	sdot_buffer integer = 20;

BEGIN
	
	-- FINAL TABLE
	CREATE TABLE automated.crossing_conflation_result AS (
		SELECT 
			osm.osm_id, 
			osm.highway,
			(osm.tags 
				|| hstore('crossing', 'marked') 
				|| hstore('crossing:signals', 'yes') 
				|| hstore('crossing:signals:sound', 'yes') 
				|| hstore('crossing:signals:button_operated', 'yes') 
				|| hstore('crossing:signals:countdown', 'yes')) AS tags,
			ST_Transform(osm.geom, 4326) AS way
		FROM (
			SELECT 
				osm.osm_id, 
				osm.highway, 
				osm.tags, 
				osm.geom
			FROM osm_crossing_in_boundary AS osm 
			JOIN sdot_accpedsig AS sdot
				ON ST_Intersects(ST_Buffer(osm.geom, osm_buffer, 'endcap=flat join=round'), ST_Buffer(sdot.geom, sdot_buffer))
		) AS osm
		
		UNION
		
		-- IF the crossing did NOT conflate:
		SELECT osm.osm_id, osm.highway, osm.tags, ST_Transform(osm.geom, 4326) AS way
		FROM osm_crossing_in_boundary AS osm
		WHERE osm.osm_id NOT IN (
			SELECT osm.osm_id
			FROM osm_crossing_in_boundary AS osm 
			JOIN sdot_accpedsig AS sdot
			ON ST_Intersects(ST_Buffer(osm.geom, osm_buffer, 'endcap=flat join=round'), ST_Buffer(sdot.geom, sdot_buffer))) );

	
END $$;

	   
	   
	   

CREATE OR REPLACE PROCEDURE metrics()
LANGUAGE plpgsql AS $$

BEGIN

	-- create a table with metrics
	-- GROUP BY sdot_objectid, the SUM the length of the conflated sdot divided by the length of the original sdot
	-- This will give us how much of the sdot got conflated
	CREATE TABLE automated.sdot2osm_metrics_sdot AS
		SELECT  
			sdot.objectid,
			COALESCE(SUM(ST_Length(sdot2osm.conflated_sdot_seg))/ST_Length(sdot.geom), 0) AS percent_conflated,
			sdot.geom, ST_UNION(sdot2osm.conflated_sdot_seg) AS sdot_conflated_subseg 
		FROM sdot_sidewalk AS sdot
		LEFT JOIN (
			SELECT 
				*,
				CASE 
					WHEN osm_seg IS NOT NULL
						THEN 
							ST_LineSubstring( sdot_geom, LEAST(ST_LineLocatePoint(sdot_geom, ST_ClosestPoint(st_startpoint(osm_seg), sdot_geom)), 
								ST_LineLocatePoint(sdot_geom, ST_ClosestPoint(st_endpoint(osm_seg), sdot_geom))), 
								GREATEST(ST_LineLocatePoint(sdot_geom, ST_ClosestPoint(st_startpoint(osm_seg), sdot_geom)), 
								ST_LineLocatePoint(sdot_geom, ST_ClosestPoint(st_endpoint(osm_seg), sdot_geom))) )
					ELSE sdot_seg
				END conflated_sdot_seg		
			FROM sdot2osm_sw_prepocessed
		) AS sdot2osm
			ON sdot.objectid = sdot2osm.sdot_objectid
		GROUP BY 
			sdot.objectid, 
			sdot2osm.sdot_objectid, 
			sdot.geom;
	
	    
	
	-- create a table with metrics
	-- GROUP BY osm_id, the SUM the length of the conflated osm divided by the length of the original osm
	-- This will give us how much of the osm got conflated
	CREATE TABLE automated.sdot2osm_metrics_osm AS
		SELECT  
			osm.osm_id,
			osm.geom, 
			COALESCE(SUM(ST_Length(sdot2osm.conflated_osm_seg))/ST_Length(osm.geom), 0) AS percent_conflated,
			ST_UNION(sdot2osm.conflated_osm_seg) AS osm_conflated_subseg
		FROM osm_sw_in_boundary AS osm
		LEFT JOIN (
			SELECT 
				*,
				CASE 
					WHEN sdot_seg IS NOT NULL
						THEN 
							ST_LineSubstring( osm_geom, LEAST(ST_LineLocatePoint(osm_geom, ST_ClosestPoint(st_startpoint(sdot_seg), osm_geom)), 
								ST_LineLocatePoint(osm_geom, ST_ClosestPoint(st_endpoint(sdot_seg), osm_geom))), 
								GREATEST(ST_LineLocatePoint(osm_geom, ST_ClosestPoint(st_startpoint(sdot_seg), osm_geom)),
								ST_LineLocatePoint(osm_geom, ST_ClosestPoint(st_endpoint(sdot_seg), osm_geom))) )
					ELSE osm_seg
				END AS conflated_osm_seg
			FROM sdot2osm_sw_prepocessed
		) AS sdot2osm
			ON osm.osm_id = sdot2osm.osm_id
		GROUP BY 
			osm.osm_id, 
			sdot2osm.osm_id, 
			osm.geom;
		
	
		
	CREATE TABLE automated.sdot2osm_metrics AS
		SELECT 
			'sdot' AS data_source,
	        sdot.objectid AS id,
	        sdot.geom,
	        COALESCE(SUM(ST_Length(sdot2osm.conflated_sdot_seg)) / ST_Length(sdot.geom), 0) AS percent_conflated,
	        ST_UNION(sdot2osm.conflated_sdot_seg) AS conflated_subseg
		FROM sdot_sidewalk AS sdot
	    LEFT JOIN (
	    	SELECT
	        	*,
	            CASE
	                WHEN osm_seg IS NOT NULL
	                	THEN
	                    	ST_LineSubstring(sdot_geom, LEAST(ST_LineLocatePoint(sdot_geom, ST_ClosestPoint(st_startpoint(osm_seg), sdot_geom)),
	                        	ST_LineLocatePoint(sdot_geom, ST_ClosestPoint(st_endpoint(osm_seg), sdot_geom))), GREATEST(ST_LineLocatePoint(sdot_geom, 
	                        	ST_ClosestPoint(st_startpoint(osm_seg), sdot_geom)), ST_LineLocatePoint(sdot_geom, ST_ClosestPoint(st_endpoint(osm_seg), 
	                        	sdot_geom))))
	                ELSE sdot_seg
	            END AS conflated_sdot_seg
	        FROM sdot2osm_sw_prepocessed
	    ) AS sdot2osm ON sdot.objectid = sdot2osm.sdot_objectid
	    GROUP BY 
	    	data_source, 
	    	id, 
	    	geom
	
	UNION ALL
	
	    SELECT
	        'osm' AS data_source,
	        osm.osm_id AS id,
	        osm.geom,
	        COALESCE(SUM(ST_Length(sdot2osm.conflated_osm_seg)) / ST_Length(osm.geom), 0) AS percent_conflated,
	        ST_UNION(sdot2osm.conflated_osm_seg) AS conflated_subseg
	    FROM osm_sw_in_boundary AS osm
	    LEFT JOIN (
	        SELECT
	            *,
	            CASE
	                WHEN sdot_seg IS NOT NULL
	                	THEN
	                    	ST_LineSubstring(osm_geom, LEAST(ST_LineLocatePoint(osm_geom, ST_ClosestPoint(st_startpoint(sdot_seg), osm_geom)), 
	                    		ST_LineLocatePoint(osm_geom, ST_ClosestPoint(st_endpoint(sdot_seg), osm_geom))), GREATEST(ST_LineLocatePoint(osm_geom, 
	                    		ST_ClosestPoint(st_startpoint(sdot_seg), osm_geom)), ST_LineLocatePoint(osm_geom, ST_ClosestPoint(st_endpoint(sdot_seg), 
	                    		osm_geom))))
	                ELSE osm_seg
	            END AS conflated_osm_seg
	        FROM sdot2osm_sw_prepocessed
	    ) AS sdot2osm ON osm.osm_id = sdot2osm.osm_id
	    GROUP BY 
	   		data_source, 
	   		id, 
	   		geom;
			
		
		
END $$;
	    


	   
-- initial procedure --
CREATE OR REPLACE PROCEDURE initial()
LANGUAGE plpgsql AS $$

BEGIN
	
	CALL data_setup();
	CALL preprocess();
	CALL sidewalk_to_sidewalk_conflation();
	CALL crossing_conflation();
	CALL metrics();
	
END $$;



-- data transfer procedure 
CREATE OR REPLACE PROCEDURE transfer()
LANGUAGE plpgsql AS $$

BEGIN
	
	CALL sw_conflation_transfer();
	CALL crossing_conflation_transfer();
	CALL metrics_transfer();
	
END $$;



-- rerun procedure
CREATE OR REPLACE PROCEDURE rerun()
LANGUAGE plpgsql AS $$

BEGIN
		
	CALL
	
END



CALL initial();
