--- INSERT DOCUMENTATION HERE ---



-- DATA SETUP --

CREATE OR REPLACE PROCEDURE data_setup()
LANGUAGE plpgsql AS $$
DECLARE

	bb box2d := st_setsrid( st_makebox2d( st_makepoint(-13616596,6053033), st_makepoint(-13615615,6053724)), 3857);

BEGIN 
	
	-- OSM sidewalk
	CREATE TEMP TABLE osm_sidewalk AS
		SELECT *, ST_Length(way) AS length
		FROM planet_osm_line AS pol 
		WHERE   highway='footway' 
			AND tags->'footway'='sidewalk' 
			AND way && bb;

	-- better naming convention
	ALTER TABLE osm_sidewalk RENAME COLUMN way TO geom;
	CREATE INDEX sw_roosevelt_geom ON osm_sidewalk USING GIST (geom);



	-- OSM crossing
	CREATE TEMP TABLE osm_crossing AS
		SELECT *
		FROM planet_osm_line AS pol 
		WHERE   highway='footway' 
			AND tags->'footway'='crossing' 
			AND way && bb;
			
	-- better naming convention
	ALTER TABLE osm_crossing RENAME COLUMN way TO geom;
	CREATE INDEX crossing_roosevelt_geom ON osm_crossing USING GIST (geom);
	
	
		
	-- SDOT sidewalk
	CREATE TEMP TABLE sdot_sidewalk AS
		SELECT  
			ogc_fid,
			objectid,
			sw_width/39.37 AS sw_width,
			surftype, primarycrossslope AS cross_slope,
			sw_category,
			(ST_Dump(wkb_geometry)).geom AS geom,
			ST_Length(wkb_geometry) AS length
		FROM sdot.sidewalks
		WHERE st_astext(wkb_geometry) != 'LINESTRING EMPTY'
			AND surftype != 'UIMPRV'
			AND sw_width != 0
			AND wkb_geometry && bb;
		
	-- better naming convention
	CREATE INDEX sdot_roosevelt_geom ON sdot_sidewalk USING GIST (geom);


	
	-- SDOT access signals
	CREATE TEMP TABLE sdot_accpedsig AS
		SELECT *
		FROM sdot.accessible_pedestrian_signals AS aps
		WHERE  wkb_geometry && bb;
	
		
	-- better naming convention
	ALTER TABLE sdot_accpedsig RENAME COLUMN wkb_geometry TO geom;



	-- check if the osm_sidewalk table has osm_sidewalk drawn as a polygon
	CREATE TEMP TABLE polygon_osm_break AS
		WITH segments AS (
		    SELECT 
		        osm_id,
		        row_number() OVER (PARTITION BY osm_id ORDER BY osm_id, (pt).path) - 1 AS segment_number,
		        (ST_MakeLine(lag((pt).geom, 1, NULL) OVER (PARTITION BY osm_id ORDER BY osm_id, (pt).path), (pt).geom)) AS geom
		    FROM (
		        SELECT 
		        	osm_id, 
		            ST_DumpPoints(geom) AS pt 
		        FROM osm_sidewalk
		        WHERE ST_StartPoint(geom) = ST_Endpoint(geom)
			) AS dumps 
		)
		SELECT *
		FROM segments
		WHERE geom IS NOT NULL;
		   
		    
END $$;





-- 
CREATE OR REPLACE PROCEDURE sp_within_degrees(_rad DOUBLE PRECISION, _thresh int, OUT result boolean) AS $$
DECLARE

    angle NUMERIC;
   
BEGIN
    
	SELECT mod(degrees(_rad)::NUMERIC, 180) INTO angle;
    IF angle > 90 THEN
        angle := angle - 180;
    END IF;
    result := abs(angle) < _thresh;
END;
$$ LANGUAGE plpgsql;
	    




--
CREATE OR REPLACE PROCEDURE sidewalk_to_sidewalk_conflation()
LANGUAGE plpgsql AS $$

BEGIN
	
	-- conflation
	CREATE TEMP TABLE sdot2osm_sw_raw AS 
		WITH ranked_roads AS (
			SELECT 
				osm.osm_id AS osm_id,
				sdot.objectid AS sdot_objectid,
				sdot.sw_width,
				sdot.surftype, 
				sdot.cross_slope,
				sdot.sw_category,
				osm.geom AS osm_geom,
				sdot.geom AS sdot_geom,
				CASE
					WHEN (sdot.length > osm.length*0.85) 
						THEN NULL
					ELSE 
						ST_LineSubstring( osm.geom, LEAST(ST_LineLocatePoint(osm.geom, ST_ClosestPoint(st_startpoint(sdot.geom), osm.geom)),
							ST_LineLocatePoint(osm.geom, ST_ClosestPoint(st_endpoint(sdot.geom), osm.geom))), GREATEST(ST_LineLocatePoint(osm.geom, 
							ST_ClosestPoint(st_startpoint(sdot.geom), osm.geom)) , ST_LineLocatePoint(osm.geom, ST_ClosestPoint(st_endpoint(sdot.geom), 
							osm.geom))))
			  	END AS osm_seg,
			  	CASE
					WHEN (sdot.length > osm.length*0.85)
					THEN 
						ST_LineSubstring( sdot.geom, LEAST(ST_LineLocatePoint(sdot.geom, ST_ClosestPoint(st_startpoint(osm.geom), sdot.geom)), 
							ST_LineLocatePoint(sdot.geom, ST_ClosestPoint(st_endpoint(osm.geom), sdot.geom))), GREATEST(ST_LineLocatePoint(sdot.geom, 
							ST_ClosestPoint(st_startpoint(osm.geom), sdot.geom)) , ST_LineLocatePoint(sdot.geom, ST_ClosestPoint(st_endpoint(osm.geom), 
							sdot.geom))))
					ELSE NULL
			  	END AS sdot_seg,
			  	ROW_NUMBER() OVER (
			  		PARTITION BY (
			  			CASE
					  		WHEN (sdot.length > osm.length*0.85) 
					  			THEN  osm.geom
					  		ELSE sdot.geom
				  		END
			  		)
			  		ORDER BY (
			  			CASE
					  		WHEN (sdot.length > osm.length*0.85)
					  			THEN 
					  				--ST_distance(ST_LineInterpolatePoint(ST_LineSubstring( sdot.geom, LEAST(ST_LineLocatePoint(sdot.geom,
					  					--ST_ClosestPoint(st_startpoint(osm.geom), sdot.geom)) , ST_LineLocatePoint(sdot.geom, 
					  					--ST_ClosestPoint(st_endpoint(osm.geom), sdot.geom))), GREATEST(ST_LineLocatePoint(sdot.geom, 
					  					--ST_ClosestPoint(st_startpoint(osm.geom), sdot.geom)) , ST_LineLocatePoint(sdot.geom, 
					  					--ST_ClosestPoint(st_endpoint(osm.geom), sdot.geom))) ), 0.5), ST_LineInterpolatePoint(osm.geom, 0.5) )
					  				ST_Area(ST_Intersection(ST_Buffer(ST_LineSubstring( sdot.geom, LEAST(ST_LineLocatePoint(sdot.geom,
					  					ST_ClosestPoint(st_startpoint(osm.geom), sdot.geom)) , ST_LineLocatePoint(sdot.geom,
					  					ST_ClosestPoint(st_endpoint(osm.geom), sdot.geom))), GREATEST(ST_LineLocatePoint(sdot.geom,
					  					ST_ClosestPoint(st_startpoint(osm.geom), sdot.geom)) , ST_LineLocatePoint(sdot.geom,
					  					ST_ClosestPoint(st_endpoint(osm.geom), sdot.geom))) ), sdot.sw_width *4, 'endcap=flat join=round'),
					  					ST_Buffer(osm.geom, 1, 'endcap=flat join=round'))) 
					  		ELSE 
					  			--ST_distance(ST_LineInterpolatePoint(ST_LineSubstring( osm.geom, LEAST(ST_LineLocatePoint(osm.geom,\
					  				--ST_ClosestPoint(st_startpoint(sdot.geom), osm.geom)) , ST_LineLocatePoint(osm.geom, 
					  				--ST_ClosestPoint(st_endpoint(sdot.geom), osm.geom))), GREATEST(ST_LineLocatePoint(osm.geom, 
					  				--ST_ClosestPoint(st_startpoint(sdot.geom), osm.geom)) , ST_LineLocatePoint(osm.geom, 
					  				--ST_ClosestPoint(st_endpoint(sdot.geom), osm.geom)))) , 0.5), ST_LineInterpolatePoint(sdot.geom, 0.5))
				  			 	ST_Area(ST_Intersection(ST_Buffer(ST_LineSubstring( osm.geom, LEAST(ST_LineLocatePoint(osm.geom, 
				  			 		ST_ClosestPoint(st_startpoint(sdot.geom), osm.geom)) , ST_LineLocatePoint(osm.geom, 
				  			 		ST_ClosestPoint(st_endpoint(sdot.geom), osm.geom))), GREATEST(ST_LineLocatePoint(osm.geom, 
				  			 		ST_ClosestPoint(st_startpoint(sdot.geom), osm.geom)) , ST_LineLocatePoint(osm.geom, 
				  			 		ST_ClosestPoint(st_endpoint(sdot.geom), osm.geom))) ), 1, 'endcap=flat join=round'),
					  				ST_Buffer(sdot.geom, sdot.sw_width * 4, 'endcap=flat join=round'))) 
					 	END 
					 ) DESC
			  	)  AS RANK
			FROM osm_sidewalk osm
			JOIN sdot_sidewalk sdot
				ON ST_Intersects(ST_Buffer(sdot.geom, sdot.sw_width * 4, 'endcap=flat join=round'), ST_Buffer(osm.geom, 1, 'endcap=flat join=round'))
			WHERE  
				CASE
					WHEN (sdot.length > osm.length*0.85)
						THEN 
							public.f_within_degrees(ST_Angle(ST_LineSubstring( sdot.geom, LEAST(ST_LineLocatePoint(sdot.geom, 
								ST_ClosestPoint(st_startpoint(osm.geom), sdot.geom)) , ST_LineLocatePoint(sdot.geom, 
								ST_ClosestPoint(st_endpoint(osm.geom), sdot.geom))), GREATEST(ST_LineLocatePoint(sdot.geom, 
								ST_ClosestPoint(st_startpoint(osm.geom), sdot.geom)) , ST_LineLocatePoint(sdot.geom, 
								ST_ClosestPoint(st_endpoint(osm.geom), sdot.geom))) ), osm.geom), 15)
					ELSE 
						-- osm.length > sdot.length
						public.f_within_degrees(ST_Angle(ST_LineSubstring( osm.geom, LEAST(ST_LineLocatePoint(osm.geom, 
							ST_ClosestPoint(st_startpoint(sdot.geom), osm.geom)) , ST_LineLocatePoint(osm.geom, 
							ST_ClosestPoint(st_endpoint(sdot.geom), osm.geom))), GREATEST(ST_LineLocatePoint(osm.geom, 
							ST_ClosestPoint(st_startpoint(sdot.geom), osm.geom)) , ST_LineLocatePoint(osm.geom, 
							ST_ClosestPoint(st_endpoint(sdot.geom), osm.geom))) ), sdot.geom), 15) 
				END
			AND osm.osm_id NOT IN (
				SELECT distinct osm_id -- polygon
				FROM polygon_osm_break 
			)
		)
		SELECT
			osm_id,
			sdot_objectid,
			sw_width,
			surftype,
			cross_slope,
			sw_category,
			CASE
				WHEN osm_seg IS NULL
			  		THEN 
			  			ST_Length(ST_Intersection(sdot_seg, ST_Buffer(osm_geom, sw_width*5,  'endcap=flat join=round')))/GREATEST(ST_Length(sdot_seg), 
			  				ST_Length(osm_geom))
			  	ELSE 
			  		ST_Length(ST_Intersection(osm_seg, ST_Buffer(sdot_geom, sw_width*5,  'endcap=flat join=round')))/GREATEST(ST_Length(osm_seg), 
			  			ST_Length(sdot_geom))
			END AS conflated_score,		  
			osm_seg,
			osm_geom,
			sdot_seg,
			sdot_geom
		FROM ranked_roads
		WHERE rank = 1 
		AND
			-- Make sure this only return segments with the intersected segments between the osm and its conflated sdot beyond some threshold
			-- for example, if the intersection between the (full) sdot and the (sub seg) osm is less than 10% of the length of any 2 of them, 
			-- it means we did not conflated well, and we want to filter it out
			CASE
				WHEN osm_seg IS NULL
			  		THEN 
			  			ST_Length(ST_Intersection(sdot_seg, ST_Buffer(osm_geom, sw_width*5, 'endcap=flat join=round')))/GREATEST(ST_Length(sdot_seg), 
			  				ST_Length(osm_geom)) > 0.1 -- TODO: need TO give it a PARAMETER later as we wanna change
			  	ELSE
			  		ST_Length(ST_Intersection(osm_seg, ST_Buffer(sdot_geom, sw_width*5, 'endcap=flat join=round')))/GREATEST(ST_Length(osm_seg), 
			  			ST_Length(sdot_geom)) > 0.1 -- TODO: need TO give it a PARAMETER later as we wanna change
			END;


		
		
		
	-- conflate the adjacent_linestring (osm) to sdot_sidewalk
	-- since 0 polygon, so no need to conflate

	
	
	
		
	-- This step give us the table while filtering out overlapped segments
	-- If it overlaps, choose the one thats closer
	CREATE TEMP TABLE sdot2osm_sw_prepocessed AS
		SELECT *
		FROM sdot2osm_sw_raw
		WHERE (osm_id, sdot_objectid) NOT IN ( 
			-- case when the whole sdot already conflated to one osm, but then its subseg also conflated to another osm
			-- if these 2 osm overlap at least 50% of the shorter one between 2 of them, then we filter out the one that's further away from our sdot
		    SELECT 
				r1.osm_id,
				CASE
					WHEN 
						ST_Distance(ST_LineInterpolatePoint(r1.osm_geom, 0.5), 
							ST_LineInterpolatePoint(r1.sdot_geom, 0.5)) < ST_Distance(ST_LineInterpolatePoint(r2.osm_geom, 0.5), 
							ST_LineInterpolatePoint(r2.sdot_seg, 0.5))
						THEN (r2.sdot_objectid)
					ELSE (r1.sdot_objectid)
				END AS sdot_objectid
			FROM sdot2osm_sw_raw AS r1
			JOIN (
				SELECT *
				FROM sdot2osm_sw_raw
				WHERE osm_id IN (
					SELECT osm_id
				    FROM sdot2osm_sw_raw
				    GROUP BY osm_id
				    HAVING count(*) > 1 ) 
			) AS r2
				ON r1.osm_id = r2.osm_id
			WHERE r1.osm_seg IS NOT NULL
			AND r2.osm_seg IS NULL
			AND 
				ST_Length(ST_Intersection(ST_Buffer(r1.sdot_geom, r1.sw_width*5, 'endcap=flat join=round'), 
					r2.sdot_seg))/LEAST(ST_Length(r1.sdot_geom), ST_Length(r2.sdot_seg)) > 0.4
			
			UNION ALL
				
				-- case when the whole osm already conflated to one sdot, but then its subseg also conflated to another sdot
				-- if these 2 sdot overlap at least 50% of the shorter one between 2 of them, then we filter out the one that's further away from our osm
				SELECT 
					CASE
						WHEN 
							ST_Distance(ST_LineInterpolatePoint(r1.osm_geom, 0.5), 
								ST_LineInterpolatePoint(r1.sdot_geom, 0.5)) < ST_Distance(ST_LineInterpolatePoint(r2.osm_seg, 0.5), 
								ST_LineInterpolatePoint(r2.sdot_geom, 0.5))
					    	THEN (r2.osm_id)
					    ELSE (r1.osm_id)
					END AS osm_id,
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
				WHERE r1.sdot_seg IS NOT NULL
				AND r2.sdot_seg IS NULL
				AND ST_Length(ST_Intersection(ST_Buffer(r1.osm_geom, r1.sw_width*5, 'endcap=flat join=round'), r2.osm_seg))/LEAST(ST_Length(r1.osm_geom), 
					ST_Length(r2.osm_seg)) > 0.4 -- TODO: need TO give it a PARAMETER later as we wanna change			
		);
	    
	    
	    
	    
	
	-- create a table with metrics
	-- GROUP BY sdot_objectid, the SUM the length of the conflated sdot divided by the length of the original sdot
	-- This will give us how much of the sdot got conflated
	CREATE TEMP TABLE sdot2osm_metrics_sdot AS
		SELECT  
			sdot.objectid,
			COALESCE(SUM(ST_Length(sdot2osm.conflated_sdot_seg))/ST_Length(sdot.geom), 0) AS percent_conflated,
			sdot.geom, ST_UNION(sdot2osm.conflated_sdot_seg) AS sdot_conflated_subseg 
		FROM sdot_sidewalk sdot
		LEFT JOIN (
			SELECT 
				*,
				CASE 
					WHEN osm_seg IS NOT NULL
						THEN 
							ST_LineSubstring( sdot_geom, LEAST(ST_LineLocatePoint(sdot_geom, ST_ClosestPoint(st_startpoint(osm_seg), sdot_geom)), 
								ST_LineLocatePoint(sdot_geom, ST_ClosestPoint(st_endpoint(osm_seg), sdot_geom))), GREATEST(ST_LineLocatePoint(sdot_geom, 
								ST_ClosestPoint(st_startpoint(osm_seg), sdot_geom)) , ST_LineLocatePoint(sdot_geom, ST_ClosestPoint(st_endpoint(osm_seg), 
								sdot_geom))) )
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
	CREATE TEMP TABLE sdot2osm_metrics_osm AS
		SELECT  
			osm.osm_id,
			COALESCE(SUM(ST_Length(sdot2osm.conflated_osm_seg))/ST_Length(osm.geom), 0) AS percent_conflated,
			osm.geom, ST_UNION(sdot2osm.conflated_osm_seg) AS osm_conflated_subseg
		FROM osm_sidewalk osm
		LEFT JOIN (
			SELECT 
				*,
				CASE 
					WHEN sdot_seg IS NOT NULL
						THEN 
							ST_LineSubstring( osm_geom, LEAST(ST_LineLocatePoint(osm_geom, ST_ClosestPoint(st_startpoint(sdot_seg), osm_geom)), 
							ST_LineLocatePoint(osm_geom, ST_ClosestPoint(st_endpoint(sdot_seg), osm_geom))), GREATEST(ST_LineLocatePoint(osm_geom, 
							ST_ClosestPoint(st_startpoint(sdot_seg), osm_geom)) , ST_LineLocatePoint(osm_geom, ST_ClosestPoint(st_endpoint(sdot_seg),
							osm_geom))) )
					ELSE osm_seg
				END conflated_osm_seg
			FROM sdot2osm_sw_prepocessed
		) AS sdot2osm
			ON osm.osm_id = sdot2osm.osm_id
		GROUP BY 
			osm.osm_id, 
			sdot2osm.osm_id, 
			osm.geom;
	    
	    
	
	
	
	CREATE TEMP TABLE sidewalk AS
		SELECT  
			osm_id,
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
			conflated_score,
			CASE
				WHEN osm_seg IS NOT NULL
					THEN 'no'
			   	ELSE 'yes'
			END AS original_way,
			CASE
				WHEN osm_seg IS NOT NULL
					THEN osm_seg
			 	ELSE osm_geom
			END AS way
		FROM sdot2osm_sw_prepocessed;
		  
	
	
	
	
	-- FINAL TABLE that will be exported
	CREATE TABLE roosevelt_automated.sidewalk_json AS
		SELECT  
			sdot2osm.osm_id,
			sdot2osm.sdot_objectid,
			osm.highway,
			osm.surface,
			sdot2osm.sdot_surface,
			CAST(sdot2osm.width AS TEXT),
			osm.tags||sdot2osm.cross_slope AS tags,
			conflated_score,
			original_way,
			sdot2osm.way
		FROM sidewalk sdot2osm
		JOIN osm_sidewalk osm
			ON sdot2osm.osm_id = osm.osm_id
		
		UNION ALL
		
		SELECT  
			osm_id,
			NULL AS "sdot_objectid",
			highway, surface, 
			NULL AS "sdot_surface", 
			width, tags, 
			NULL AS conflated_score, 
			NULL AS original_way, 
			geom AS "way"
		FROM osm_sidewalk
		WHERE osm_id NOT IN (
			SELECT DISTINCT osm_id
			FROM sidewalk );

END $$;
	   
	   
	   
	   
	   
-- crossing conflation
CREATE OR REPLACE PROCEDURE crossing_conflation()
LANGUAGE plpgsql AS $$

BEGIN
	
	-- FINAL TABLE
	CREATE TABLE roosevelt_automated.crossing_json AS 
	SELECT 
		osm.osm_id, osm.highway,
		(osm.tags ||
		hstore('crossing', 'marked') || 
		hstore('crossing:signals', 'yes') ||
		hstore('crossing:signals:sound', 'yes') ||
		hstore('crossing:signals:button_operated', 'yes') ||
		hstore('crossing:signals:countdown', 'yes')) AS tags,
		osm.geom AS way
	FROM (SELECT osm.osm_id, osm.highway, osm.tags, osm.geom
		FROM osm_crossing osm 
		JOIN sdot_accpedsig sdot
		ON ST_Intersects(osm.geom, ST_Buffer(sdot.geom, 20))
		) osm
	
	UNION
	
	-- IF the crossing did NOT conflate:
	SELECT osm.osm_id, osm.highway, osm.tags, osm.geom AS way
	FROM osm_crossing osm
	WHERE osm.osm_id NOT IN (
		SELECT osm.osm_id
		FROM osm_crossing osm 
		JOIN sdot_accpedsig sdot
		ON ST_Intersects(osm.geom, ST_Buffer(sdot.geom, 20)));
	
END $$;

	   
	   
	   
	   
-- main procedure --
CREATE OR REPLACE PROCEDURE main()
LANGUAGE plpgsql AS $$

BEGIN
	
	CALL data_setup();
	--CALL sp_within_degrees(_rad DOUBLE PRECISION, _thresh int, OUT result boolean);
	CALL sidewalk_to_sidewalk_conflation();
	CALL crossing_conflation();
	
END $$;





CALL main();

