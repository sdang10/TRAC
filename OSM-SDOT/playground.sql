CREATE OR REPLACE PROCEDURE make_sdot_table()
LANGUAGE plpgsql AS $$

DECLARE

    row_record RECORD;

BEGIN
	
    -- Create a new table with three columns: id, geom, and info (hstore).
    CREATE TABLE automated.sdot_table (
        id INTEGER,
        geom geometry,
        info hstore
    );

    -- Iterate through each row in the original table.
    FOR row_record IN (SELECT * FROM automated.sidewalks) LOOP
	    
        -- Insert into the new table with only the necessary columns.
        INSERT INTO automated.sdot_table (id, geom, info)
        VALUES (
        	row_record.objectid, 
        	(ST_Dump(row_record.wkb_geometry)).geom, 
        	hstore(
            	ARRAY[
            		'ogc_fid',
					'objectid',
					'compkey',
					'comptype',
					'segkey',
					'unitid',
					'unittype',
					'unitdesc',
					'adddttm',
					'asblt',
					'condition',
					'condition_assessment_date',
					'curbtype',
					'current_status',
					'current_status_date',
					'fillertype',
					'fillerwid',
					'install_date',
					'sw_width',
					'maintained_by',
					'matl',
					'moddttm',
					'ownership',
					'side',
					'surftype',
					'buildercd',
					'gsitypecd',
					'hansen7id',
					'attachment_1',
					'attachment_2',
					'attachment_3',
					'attachment_4',
					'attachment_5',
					'attachment_6',
					'attachment_7',
					'attachment_8',
					'attachment_9',
					'primarydistrictcd',
					'secondarydistrictcd',
					'overrideyn',
					'overridecomment',
					'srts_sidewalk_rank',
					'num_attachments',
					'primarycrossslope',
					'minimumvariablewidth',
					'sw_category',
					'maintenance_group',
					'last_verify_date',
					'color',
					'ownership_date',
					'nature_of_maint_resp',
					'maint_financial_resp',
					'variablewidthyn',
					'maintbyrdwystructyn',
					'swincompleteyn',
					'multiplesurfaceyn',
					'date_mvw_last_updated',
					'expdate',
					'shape_length',
					'wkb_geometry'
            	],
            	ARRAY[
            		row_record.ogc_fid::text, 
            		row_record.objectid::text, 
            		row_record.compkey::text, 
            		row_record.comptype::text, 
            		row_record.segkey::text, 
            		row_record.unitid::text, 
            		row_record.unittype::text,
            		row_record.unitdesc::text,
            		row_record.adddttm::text,
            		row_record.asblt::text,
            		row_record.condition::text,
            		row_record.condition_assessment_date::text,
            		row_record.curbtype::text,
            		row_record.current_status::text,
            		row_record.current_status_date::text,
            		row_record.fillertype::text,
            		row_record.fillerwid::text,
            		row_record.install_date::text,
            		row_record.sw_width::text,
            		row_record.maintained_by::text,
            		row_record.matl::text,
            		row_record.moddttm::text,
            		row_record.ownership::text,
            		row_record.side::text,
            		row_record.surftype::text,
            		row_record.buildercd::text,
            		row_record.gsitypecd::text,
            		row_record.hansen7id::text,
            		row_record.attachment_1::text,
            		row_record.attachment_2::text,
            		row_record.attachment_3::text,
            		row_record.attachment_4::text,
            		row_record.attachment_5::text,
            		row_record.attachment_6::text,
            		row_record.attachment_7::text,
            		row_record.attachment_8::text,
            		row_record.attachment_9::text,
            		row_record.primarydistrictcd::text,
            		row_record.secondarydistrictcd::text,
            		row_record.overrideyn::text,
            		row_record.overridecomment::text,
            		row_record.srts_sidewalk_rank::text,
            		row_record.num_attachments::text,
            		row_record.primarycrossslope::text,
            		row_record.minimumvariablewidth::text,
            		row_record.sw_category::text,
            		row_record.maintenance_group::text,
            		row_record.last_verify_date::text,
            		row_record.color::text,
            		row_record.ownership_date::text,
            		row_record.nature_of_maint_resp::text,
            		row_record.maint_financial_resp::text,
            		row_record.variablewidthyn::text,
            		row_record.maintbyrdwystructyn::text,
            		row_record.swincompleteyn::text,
            		row_record.multiplesurfaceyn::text,
            		row_record.date_mvw_last_updated::text,
            		row_record.expdate::text,
            		row_record.shape_length::text,
            		row_record.wkb_geometry::text
            	]
        	)
        );
    END LOOP;
END $$;


CALL make_sdot_table()
SELECT * FROM automated.sdot_table


--------------------------------------------------------------------------------------------------------------------------------------------------------------


CREATE OR REPLACE PROCEDURE make_osm_table()
LANGUAGE plpgsql AS $$

DECLARE

    row_record RECORD;

BEGIN
	
    -- Create a new table with three columns: id, geom, and info (hstore).
    CREATE TABLE automated.osm_table (
        id INTEGER,
        geom geometry,
        info hstore
    );

    -- Iterate through each row in the original table.
    FOR row_record IN (SELECT * FROM automated.planet_osm_line) LOOP
	    
        -- Insert into the new table with only the necessary columns.
        INSERT INTO automated.osm_table (id, geom, info)
        VALUES (
        	row_record.osm_id, 
        	row_record.way, 
        	hstore(
            	ARRAY[
					'osm_id',
					'access',
					'addr:housename',
					'addr:housenumber',
					'addr:interpolation',
					'admin_level',
					'aerialway',
					'aeroway',
					'amenity',
					'area',
					'barrier',
					'bicycle',
					'brand',
					'bridge',
					'boundary',
					'building',
					'construction',
					'covered',
					'culvert',
					'cutting',
					'denomination',
					'disused',
					'embankment',
					'foot',
					'generator:source',
					'harbour',
					'highway',
					'historic',
					'horse',
					'intermittent',
					'junction',
					'landuse',
					'layer',
					'leisure',
					'lock',
					'man_made',
					'military',
					'motorcar',
					'name',
					'natural',
					'office',
					'oneway',
					'operator',
					'place',
					'population',
					'power',
					'power_source',
					'public_transport',
					'railway',
					'ref',
					'religion',
					'route',
					'service',
					'shop',
					'sport',
					'surface',
					'toll',
					'tourism',
					'tower:type',
					'tracktype',
					'tunnel',
					'water',
					'waterway',
					'wetland',
					'width',
					'wood',
					'z_order',
					'way_area',
					'tags',
					'way' 
            	],
            	ARRAY[
            		row_record.osm_id::text, 
            		row_record.access::text, 
            		row_record."addr:housename"::text, 
            		row_record."addr:housenumber"::text, 
            		row_record."addr:interpolation"::text, 
            		row_record.admin_level::text, 
            		row_record.aerialway::text,
            		row_record.aeroway::text,
            		row_record.amenity::text,
            		row_record.area::text,
            		row_record.barrier::text,
            		row_record.bicycle::text,
            		row_record.brand::text,
            		row_record.bridge::text,
            		row_record.boundary::text,
            		row_record.building::text,
            		row_record.construction::text,
            		row_record.covered::text,
            		row_record.culvert::text,
            		row_record.cutting::text,
            		row_record.denomination::text,
            		row_record.disused::text,
            		row_record.embankment::text,
            		row_record.foot::text,
            		row_record."generator:source"::text,
            		row_record.harbour::text,
            		row_record.highway::text,
            		row_record.historic::text,
            		row_record.horse::text,
            		row_record.intermittent::text,
            		row_record.junction::text,
            		row_record.landuse::text,
            		row_record.layer::text,
            		row_record.leisure::text,
            		row_record.lock::text,
            		row_record.man_made::text,
            		row_record.military::text,
            		row_record.motorcar::text,
            		row_record.name::text,
            		row_record.natural::text,
            		row_record.office::text,
            		row_record.oneway::text,
            		row_record.operator::text,
            		row_record.place::text,
            		row_record.population::text,
            		row_record.power::text,
            		row_record.power_source::text,
            		row_record.public_transport::text,
            		row_record.railway::text,
            		row_record.ref::text,
            		row_record.religion::text,
            		row_record.route::text,
            		row_record.service::text,
            		row_record.shop::text,
            		row_record.sport::text,
            		row_record.surface::text,
            		row_record.toll::text,
            		row_record.tourism::text,
            		row_record."tower:type"::text,
            		row_record.tracktype::TEXT,
            		row_record.tunnel::TEXT,
            		row_record.water::TEXT,
            		row_record.waterway::TEXT,
            		row_record.wetland::TEXT,
            		row_record.width::TEXT,
            		row_record.wood::TEXT,
            		row_record.z_order::TEXT,
            		row_record.way_area::TEXT,
            		row_record.tags::TEXT,
            		row_record.way::TEXT
            	]
        	)
        );
    END LOOP;
END $$;


CALL make_osm_table()
SELECT * FROM automated.osm_table


--------------------------------------------------------------------------------------------------------------------------------------------------------------


CREATE OR REPLACE PROCEDURE osm2sdot_setup()
LANGUAGE plpgsql AS $$
DECLARE

BEGIN 

	CREATE TEMP TABLE table1 AS 
		SELECT * 
		FROM filter_by_bbox('osm_table', st_setsrid( st_makebox2d( st_makepoint(-13616215,6052850), st_makepoint(-13614841,6054964)), 3857))
		WHERE info -> 'highway' = 'footway';
		--AND info -> 'tags' = 'footway' = 'sidewalk';
	
	CREATE TEMP TABLE table2 AS 
		SELECT * 
		FROM filter_by_bbox('sdot_table', st_setsrid( st_makebox2d( st_makepoint(-13616215,6052850), st_makepoint(-13614841,6054964)), 3857));

	-- index for better spatial query performance
	-- CREATE INDEX sdot_geom ON sdot_sidewalk USING GIST (geom);

END $$;

DROP TABLE table1
DROP TABLE table2

CALL osm2sdot_setup()

SELECT * FROM table1


--------------------------------------------------------------------------------------------------------------------------------------------------------------


CREATE OR REPLACE PROCEDURE preprocess()
LANGUAGE plpgsql AS $$
DECLARE 

	_rec record;
	_first record;
	_prev record;
	_id int8;

	angle_degree integer = 15;

BEGIN
	
	
	-- 1. Check which OSM sidewalks are closed linestrings, break these OSM at vertices to sub-seg, number them by the order from 
	-- 	  start to end points
	CREATE TEMP TABLE polygon_osm_break AS
		WITH segments AS (
		    SELECT 
		        id,
		   		-- assigns number to segments of each segment from a given broken linestring
		        row_number() OVER (PARTITION BY id ORDER BY id, (pt).path) - 1 AS segment_number,
		        (ST_MakeLine(lag((pt).geom, 1, NULL) OVER (PARTITION BY id ORDER BY id, (pt).path), (pt).geom)) AS geom
		    FROM (
		        SELECT 
		        	id, 
		            ST_DumpPoints(geom) AS pt 
		        FROM table1
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
			p1.id AS id,
			p1.segment_number AS seg_a,
			p1.geom AS geom_a,
			p2.segment_number AS seg_b,
			p2.geom AS geom_b
		FROM polygon_osm_break AS p1
		JOIN polygon_osm_break AS p2
			ON p1.id = p2.id 
			AND p1.segment_number < p2.segment_number 
			AND ST_Intersects(p1.geom, p2.geom)
		-- parallel is checked for and defined here with the value "angle_degree"
		WHERE  public.f_within_degrees(ST_Angle(p1.geom, p2.geom), angle_degree);
	
	
	
	CREATE TEMP TABLE adjacent_linestrings (
		id int8 NOT NULL,
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
				id ASC, 
				seg_a ASC,
				seg_b DESC -- sort seg_b IN descending order so that first/last segment is first on new osm_id
	LOOP 
		
		
		IF _first IS DISTINCT FROM NULL OR _prev IS DISTINCT FROM NULL THEN	-- _prev or _first record exists 
			-- check if current record is same osm_id as previous
			IF _rec.id = _id 
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
			 					INSERT INTO adjacent_linestrings VALUES (_prev.id, _prev.seg_a, _prev.seg_b, 
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
		    	_id := _rec.id;
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
			    			INSERT INTO adjacent_linestrings VALUES (_first.id, _first.seg_b, _first.seg_a, 
			    				ST_Linemerge(ST_Union(_first.geom_a, _first.geom_b)));
			    		END IF;
		    	END IF;
		    	-- write final segment from previous osm_id to table
		    	INSERT INTO adjacent_linestrings VALUES (_prev.id, _prev.seg_a, _prev.seg_b, 
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
			_id := _rec.id;
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
				INSERT INTO adjacent_linestrings VALUES (_first.id, _first.seg_b, _first.seg_a, 
					ST_Linemerge(ST_Union(_first.geom_a, _first.geom_b)));
			END IF;
	END IF;
	-- write final segment from previous osm_id to table
	INSERT INTO adjacent_linestrings VALUES (_prev.id, _prev.seg_a, _prev.seg_b,
		ST_Linemerge(ST_Union(_prev.geom_a, _prev.geom_b)));



	-- 3. Finally, for those that's not adjacent and parallel to others, we also want to insert them into the adjacent_parallel table
	INSERT INTO adjacent_linestrings
		SELECT 
			id, 
			segment_number AS start_seg,
			segment_number AS end_seg, 
			geom
		FROM polygon_osm_break
		WHERE (id, segment_number) NOT IN (
			SELECT 
				id, 
				seg_a AS seg
			FROM adjacent_lines
			
			UNION ALL
			
			SELECT 
				id, 
				seg_b AS seg
			FROM adjacent_lines
		); 
	
	
	-- create a table where we union all the osm sidewalks together
	CREATE TEMP TABLE osm_sidewalk_preprocessed AS (
		SELECT 
			id, 
			CONCAT(start_seg, '-', end_seg) AS start_end_seg, 
			geom, 
			ST_Length(geom) AS length
		FROM adjacent_linestrings
		
		UNION ALL 
		
		SELECT 
			id, 
			'null' AS start_end_seg, 
			geom, 
			ST_Length(geom) AS length
		FROM table1
		WHERE NOT ST_IsClosed(geom)
	);

	CREATE INDEX osm_geom ON osm_sidewalk_preprocessed USING GIST (geom);


END	$$;

CALL preprocess()

DROP TABLE polygon_osm_break
DROP TABLE adjacent_lines
DROP TABLE adjacent_linestrings
DROP TABLE osm_sidewal

SELECT * FROM polygon_osm_break
SELECT * FROM adjacent_lines
SELECT * FROM adjacent_linestrings
SELECT * FROM osm_sidewalk_preprocessed


--------------------------------------------------------------------------------------------------------------------------------------------------------------


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
	
	-- conflation
	CREATE TEMP TABLE sw_raw AS (
		WITH ranked_roads AS (
			SELECT 
				table1.id AS table1_id,
			  	table2.id AS table2_id,
			  	table1.geom AS table1_geom,
			  	table2.geom AS table2_geom,
				CASE
					-- when the OSM geometry is greater than the SDOT geometry, then the osm_seg is defined as the portion of the linestring
					-- cut off at the closest points to the start and endpoint of the SDOT geometry. Otherwise null since it doesn't need to be changed
					WHEN (ST_Length(table1.geom) > ST_Length(table2.geom)) 
						THEN 
							ST_LineSubstring( table1.geom, LEAST(ST_LineLocatePoint(table1.geom, ST_ClosestPoint(st_startpoint(table2.geom), table1.geom)), 
								ST_LineLocatePoint(table1.geom, ST_ClosestPoint(st_endpoint(table2.geom), table1.geom))), 
								GREATEST(ST_LineLocatePoint(table1.geom, ST_ClosestPoint(st_startpoint(table2.geom), table1.geom)),
								ST_LineLocatePoint(table1.geom, ST_ClosestPoint(st_endpoint(table2.geom), table1.geom))) )
					ELSE NULL
			  	END AS table1_seg,
			  	CASE
				  	-- when the SDOT geometry is greater than the OSM geometry, then the sdot_seg is defined as the portion of the linestring
					-- cut off at the closest points to the start and endpoint of the OSM geometry. Otherwise null since it doesn't need to be changed
					WHEN (ST_Length(table1.geom) < ST_Length(table2.geom))
						THEN 
							ST_LineSubstring( table2.geom, LEAST(ST_LineLocatePoint(table2.geom, ST_ClosestPoint(st_startpoint(table1.geom),
								table2.geom)) , ST_LineLocatePoint(table2.geom, ST_ClosestPoint(st_endpoint(table1.geom), table2.geom))),
								GREATEST(ST_LineLocatePoint(table2.geom, ST_ClosestPoint(st_startpoint(table1.geom), table2.geom)),
								ST_LineLocatePoint(table2.geom, ST_ClosestPoint(st_endpoint(table1.geom), table2.geom))) )
					ELSE NULL
			  	END AS table2_seg,
			  	ROW_NUMBER() OVER (
			  		PARTITION BY (
			  			CASE
					  		WHEN (ST_Length(table1.geom) < ST_Length(table2.geom))
					  			THEN  table1.geom
					  		ELSE table2.geom
				  		END
			  		)
			  		ORDER BY
			  		-- ranks every SDOT2OSM comparison based on buffer coverage of whichever sidewalk is the smaller of SDOT and OSM. 
			  		CASE
					  	WHEN (ST_Length(table1.geom) < ST_Length(table2.geom))
					  		THEN 
					  			ST_Area(ST_Intersection(ST_Buffer(ST_LineSubstring( table2.geom, LEAST(ST_LineLocatePoint(table2.geom,
					  				ST_ClosestPoint(st_startpoint(table1.geom), table2.geom)) , ST_LineLocatePoint(table2.geom, 
					  				ST_ClosestPoint(st_endpoint(table1.geom), table2.geom))), GREATEST(ST_LineLocatePoint(table2.geom, 
					  				ST_ClosestPoint(st_startpoint(table1.geom), table2.geom)) , ST_LineLocatePoint(table2.geom, 
					  				ST_ClosestPoint(st_endpoint(table1.geom), table2.geom))) ), buffer2, 'endcap=flat join=round'),
					  				ST_Buffer(table1.geom, buffer2, 'endcap=flat join=round'))) 
					  	ELSE
					  		ST_Area(ST_Intersection(ST_Buffer(ST_LineSubstring( table1.geom, LEAST(ST_LineLocatePoint(table1.geom, 
					  			ST_ClosestPoint(st_startpoint(table2.geom), table1.geom)) , ST_LineLocatePoint(table1.geom, 
					  			ST_ClosestPoint(st_endpoint(table2.geom), table1.geom))), GREATEST(ST_LineLocatePoint(table1.geom, 
					  			ST_ClosestPoint(st_startpoint(table2.geom), table1.geom)) , ST_LineLocatePoint(table1.geom, 
					  			ST_ClosestPoint(st_endpoint(table2.geom), table1.geom))) ), buffer2, 'endcap=flat join=round'),
					  			ST_Buffer(table2.geom, buffer2, 'endcap=flat join=round'))) 
					END DESC
				)  AS RANK
			FROM table1 AS table1
			JOIN table2 AS table2
				ON ST_Intersects(ST_Buffer(table2.geom, buffer2, 'endcap=flat join=round'), ST_Buffer(table1.geom, buffer2, 
					'endcap=flat join=round'))
			WHERE  -- the the sw geometries have a similarity IN angle (parallel to each other) WITH a leniency value OF "angle_degree"
				CASE
					WHEN (ST_Length(table1.geom) < ST_Length(table2.geom))
						THEN 
							public.f_within_degrees(ST_Angle(ST_LineSubstring( table2.geom, LEAST(ST_LineLocatePoint(table2.geom, 
								ST_ClosestPoint(st_startpoint(table1.geom), table2.geom)) , ST_LineLocatePoint(table2.geom, 
								ST_ClosestPoint(st_endpoint(table1.geom), table2.geom))), GREATEST(ST_LineLocatePoint(table2.geom, 
								ST_ClosestPoint(st_startpoint(table1.geom), table2.geom)) , ST_LineLocatePoint(table2.geom, 
								ST_ClosestPoint(st_endpoint(table1.geom), table2.geom))) ), table1.geom), angle_degree)
					ELSE 
						public.f_within_degrees(ST_Angle(ST_LineSubstring( table1.geom, LEAST(ST_LineLocatePoint(table1.geom, 
							ST_ClosestPoint(st_startpoint(table2.geom), table1.geom)) , ST_LineLocatePoint(table1.geom, 
							ST_ClosestPoint(st_endpoint(table2.geom), table1.geom))), GREATEST(ST_LineLocatePoint(table1.geom, 
							ST_ClosestPoint(st_startpoint(table2.geom), table1.geom)) , ST_LineLocatePoint(table1.geom, 
							ST_ClosestPoint(st_endpoint(table2.geom), table1.geom))) ), table2.geom), angle_degree) 
				END
		)
		SELECT
			table1_id,
			table2_id,
			CASE
				WHEN table1_seg IS NULL
			  		THEN 
			  			ST_Length(ST_Intersection(table2_seg, ST_Buffer(table1_geom, buffer1,  
			  				'endcap=flat join=round')))/GREATEST(ST_Length(table2_seg), ST_Length(table1_geom))
			  	ELSE 
			  		ST_Length(ST_Intersection(table1_seg, ST_Buffer(table2_geom, buffer1,  
			  			'endcap=flat join=round')))/GREATEST(ST_Length(table1_seg), ST_Length(table2_geom))
			END AS conflated_score, -- creates a ratio (0-1) based ON the buffer INTERSECTION length AND maximum length OF the two geometries
			table1_seg,
			table1_geom,
			table2_seg,
			table2_geom
		FROM ranked_roads
		WHERE rank = 1 
		AND
			-- applies filter to remove results with a conflation score less than the "score_min" (score minimum). This is to ensure the quality
			-- of our results as to set a minimum score we consider in moving forward with the process. Those below the score minimum we assume
			-- to be bad matchings
			CASE
				WHEN table1_seg IS NULL
			  		THEN 
			  			ST_Length(ST_Intersection(table2_seg, ST_Buffer(table1_geom, buffer1,  
			  				'endcap=flat join=round')))/GREATEST(ST_Length(table2_seg), ST_Length(table1_geom)) > score_min 
				ELSE
					ST_Length(ST_Intersection(table1_seg, ST_Buffer(table2_geom, buffer1,  
						'endcap=flat join=round')))/GREATEST(ST_Length(table1_seg), ST_Length(table2_geom)) > score_min 
			END 
	); 
		


	-- This step give us the table while filtering out overlapped segments
	-- If it overlapps, choose the one thats closer
	CREATE TEMP TABLE sw_preprocessed AS (
		SELECT *
		FROM sw_raw
		WHERE (table1_id, table2_id) NOT IN ( 
	    	-- case when the whole  already conflated to one osm, but then its subseg also conflated to another osm
			-- if these 2 osm overlap with at least the value of "overlap_perc_min" for the shorter one between 2 of them, 
			-- then we filter out the one that's further away from our sdot
	        SELECT 
				r1.table1_id,
				CASE
					WHEN r1.table2_seg IS NULL 
					AND r2.table2_seg IS NOT NULL
						THEN
							CASE
						  		WHEN ST_Distance(ST_LineInterpolatePoint(r1.table1_geom, 0.5), 
						  			ST_LineInterpolatePoint(r1.table2_geom, 0.5)) < ST_Distance(ST_LineInterpolatePoint(r2.table1_geom, 0.5), 
						  			ST_LineInterpolatePoint(r2.table2_seg, 0.5))
							    	THEN (r2.table2_id)
							    ELSE (r1.table2_id)
						 	END
					    	WHEN r1.table2_seg IS NOT NULL 
					    	AND r2.table2_seg IS NULL
					  			THEN
					  				CASE 
					  					WHEN ST_Distance(ST_LineInterpolatePoint(r1.table1_geom, 0.5), 
					  						ST_LineInterpolatePoint(r1.table2_seg, 0.5)) < ST_Distance(ST_LineInterpolatePoint(r2.table1_geom, 0.5),
					  						ST_LineInterpolatePoint(r2.table2_geom, 0.5))
							            	THEN (r2.table2_id)
							        	ELSE (r1.table2_id)
							  		END
									WHEN r1.table2_seg IS NULL 
									AND r2.table2_seg IS NULL
					  					THEN
					  						CASE 
					  							WHEN ST_Length(r1.table1_seg) > ST_Length(r2.table1_seg) 
					  								THEN
	 													CASE
	 														WHEN ST_Distance(ST_LineInterpolatePoint(r1.table1_seg, 0.5), 
	 															ST_LineInterpolatePoint(r1.table2_geom, 0.5)) < 
	 															ST_Distance(ST_LineInterpolatePoint(r1.table1_seg, 0.5), 
	 															ST_LineInterpolatePoint(r2.table2_geom, 0.5))
																THEN (r2.table2_id)
															ELSE (r1.table2_id)
	 													END
	 											ELSE 
							        				CASE
	 													WHEN ST_Distance(ST_LineInterpolatePoint(r2.table1_seg, 0.5), 
	 														ST_LineInterpolatePoint(r1.table2_geom, 0.5)) < 
	 														ST_Distance(ST_LineInterpolatePoint(r2.table1_seg, 0.5), 
	 														ST_LineInterpolatePoint(r2.table2_geom, 0.5))
															THEN (r2.table2_id)
														ELSE (r1.table2_id)
	 												END
							  				END
				END AS table2_id
			FROM sw_raw AS r1
			JOIN (
				SELECT *
			    FROM sw_raw
			    WHERE table1_id IN (
			    	SELECT 
			    		table1_id
			        FROM sw_raw
			        GROUP BY 
			        	table1_id
			        	HAVING count(*) > 1 )
			) AS r2
				ON r1.table1_id = r2.table1_id 
				AND r1.table2_id < r2.table2_id
			WHERE 
				CASE
					WHEN r1.table2_seg IS NULL 
					AND r2.table2_seg IS NOT NULL
						THEN
					  		ST_Length(ST_Intersection (ST_LineSubstring(r2.table1_geom, LEAST(ST_LineLocatePoint(r2.table1_geom, 
					  			ST_ClosestPoint(st_startpoint(r2.table2_seg), r2.table1_geom)) , ST_LineLocatePoint(r2.table1_geom, 
					  			ST_ClosestPoint(st_endpoint(r2.table2_seg), r2.table1_geom))),GREATEST(ST_LineLocatePoint(r2.table1_geom, 
					  			ST_ClosestPoint(st_startpoint(r2.table2_seg), r2.table1_geom)) , ST_LineLocatePoint(r2.table1_geom, 
					  			ST_ClosestPoint(st_endpoint(r2.table2_seg), r2.table1_geom))) ), ST_Buffer(r1.table1_seg, buffer1, 
					  			'endcap=flat join=round'))) 
					  		/ LEAST(ST_Length(r1.table1_seg), ST_Length (ST_LineSubstring(r2.table1_geom,
					  			LEAST(ST_LineLocatePoint(r2.table1_geom, ST_ClosestPoint(st_startpoint(r2.table2_seg), r2.table1_geom)), 
					  			ST_LineLocatePoint(r2.table1_geom, ST_ClosestPoint(st_endpoint(r2.table2_seg), r2.table1_geom))),
					  			GREATEST(ST_LineLocatePoint(r2.table1_geom, ST_ClosestPoint(st_startpoint(r2.table2_seg), r2.table1_geom)),
					  			ST_LineLocatePoint(r2.table1_geom, ST_ClosestPoint(st_endpoint(r2.table2_seg), r2.table1_geom))) ) )) > overlap_perc_min 
					WHEN r1.table2_seg IS NOT NULL 
					AND r2.table2_seg IS NULL
						THEN 
				  	 		ST_Length(ST_Intersection (ST_LineSubstring(r1.table1_geom,LEAST(ST_LineLocatePoint(r1.table1_geom, 
				  	 			ST_ClosestPoint(st_startpoint(r1.table2_seg), r1.table1_geom)) , ST_LineLocatePoint(r1.table1_geom, 
				  	 			ST_ClosestPoint(st_endpoint(r1.table2_seg), r1.table1_geom))),GREATEST(ST_LineLocatePoint(r1.table1_geom, 
				  	 			ST_ClosestPoint(st_startpoint(r1.table2_seg), r1.table1_geom)) , ST_LineLocatePoint(r1.table1_geom, 
				  	 			ST_ClosestPoint(st_endpoint(r1.table2_seg), r1.table1_geom))) ), ST_Buffer(r2.table1_seg, buffer1, 
				  	 			'endcap=flat join=round')))
						  	/ LEAST(ST_Length(r2.table1_seg),ST_Length (ST_LineSubstring( r1.table1_geom, LEAST(ST_LineLocatePoint(r1.table1_geom, 
						  		ST_ClosestPoint(st_startpoint(r1.table2_seg), r1.table1_geom)), ST_LineLocatePoint(r1.table1_geom, 
						  		ST_ClosestPoint(st_endpoint(r1.table2_seg), r1.table1_geom))),GREATEST(ST_LineLocatePoint(r1.table1_geom, 
						  		ST_ClosestPoint(st_startpoint(r1.table2_seg), r1.table1_geom)) , ST_LineLocatePoint(r1.table1_geom, 
						  		ST_ClosestPoint(st_endpoint(r1.table2_seg), r1.table1_geom))) ) )) > overlap_perc_min
					WHEN r1.table2_seg IS NULL 
					AND r2.table2_seg IS NULL
				  		THEN 
				  	 		ST_Length(ST_Intersection(r1.table1_seg, ST_Buffer(r2.table1_seg, buffer1, 'endcap=flat join=round') ) )
				  	 	  	/ LEAST(ST_Length(r1.table1_seg), ST_Length(r2.table1_seg)) > overlap_perc_min	 	
				END

			UNION ALL
			
			-- case when the whole osm already conflated to one sdot, but then its subseg also conflated to another sdot
			-- if these 2 sdot overlap with at least the value of "overlap_perc_min" for the shorter one between 2 of them, 
			-- then we filter out the one that's further away from our osm
				SELECT 
					CASE
				    	WHEN r1.table2_seg IS NOT NULL 
				    	AND r2.table2_seg IS NULL 
				        	THEN 
				        		CASE
				        			WHEN ST_Distance(ST_LineInterpolatePoint(r1.table1_geom, 0.5), 
				        				ST_LineInterpolatePoint(r1.table2_geom, 0.5)) < ST_Distance(ST_LineInterpolatePoint(r2.table1_seg, 0.5), 
				        				ST_LineInterpolatePoint(r2.table2_geom, 0.5))
				            			THEN r2.table1_id
				        			ELSE r1.table1_id
				        		END
				        		WHEN r1.table2_seg IS NULL 
				        		AND r2.table2_seg IS NOT NULL 
				        			THEN 
				        				CASE
				        					WHEN ST_Distance(ST_LineInterpolatePoint(r1.table1_seg, 0.5), 
				        						ST_LineInterpolatePoint(r1.table2_geom, 0.5)) < ST_Distance(ST_LineInterpolatePoint(r2.table1_geom, 
				        						0.5), ST_LineInterpolatePoint(r2.table2_geom, 0.5))
				            					THEN r2.table1_id
				        					ELSE r1.table1_id
				        				END
				        				WHEN r1.table2_seg IS NOT NULL 
				        				AND r2.table2_seg IS NOT NULL 
				        					THEN
					  							CASE 
					  								WHEN ST_Length(r1.table2_seg) > ST_Length(r2.table2_seg) 
					  									THEN
	 														CASE
	 															WHEN ST_Distance(ST_LineInterpolatePoint(r1.table1_geom, 0.5), 
	 																ST_LineInterpolatePoint(r1.table2_seg, 0.5)) < 
	 																ST_Distance(ST_LineInterpolatePoint(r2.table1_geom, 0.5), 
	 																ST_LineInterpolatePoint(r1.table2_seg, 0.5))
																	THEN r2.table1_id
																ELSE r1.table1_id
	 														END
	 												ELSE 
							        					CASE
	 														WHEN ST_Distance(ST_LineInterpolatePoint(r1.table1_geom, 0.5), 
	 															ST_LineInterpolatePoint(r2.table2_seg, 0.5)) < 
	 															ST_Distance(ST_LineInterpolatePoint(r2.table1_geom, 0.5), 
	 															ST_LineInterpolatePoint(r2.table2_seg, 0.5))
																THEN r2.table1_id
															ELSE r1.table1_id
	 													END
							  					END
					END AS table1_id_seg,
				    r1.table2_id
				FROM sw_raw AS r1
				JOIN (
					SELECT *
			    	FROM sw_raw
			        WHERE table2_id IN (
			        	SELECT table2_id
			            FROM sw_raw
			            GROUP BY table2_id
			            HAVING count(*) > 1 ) 
			    ) AS r2
					ON r1.table2_id = r2.table2_id 
					AND r1.table1_id < r2.table1_id
				WHERE
					CASE
						WHEN r1.table2_seg IS NOT NULL 
						AND r2.table2_seg IS NULL
							THEN 
								ST_Length(ST_Intersection(ST_LineSubstring(r2.table2_geom,LEAST(ST_LineLocatePoint(r2.table2_geom, 
									ST_ClosestPoint(st_startpoint(r2.table1_seg), r2.table2_geom)), 
									ST_LineLocatePoint(r2.table2_geom, ST_ClosestPoint(st_endpoint(r2.table1_seg), 
									r2.table2_geom))),GREATEST(ST_LineLocatePoint(r2.table2_geom, ST_ClosestPoint(st_startpoint(r2.table1_seg),
									r2.table2_geom)) , ST_LineLocatePoint(r2.table2_geom, ST_ClosestPoint(st_endpoint(r2.table1_seg),
									r2.table2_geom))) ), ST_Buffer(r1.table2_seg, buffer1, 'endcap=flat join=round')) )
						  		/ LEAST(ST_Length(r1.table2_seg),ST_Length(ST_LineSubstring(r2.table2_geom,LEAST(ST_LineLocatePoint(r2.table2_geom, 
						  			ST_ClosestPoint(st_startpoint(r2.table1_seg), r2.table2_geom)) , ST_LineLocatePoint(r2.table2_geom, 
						  			ST_ClosestPoint(st_endpoint(r2.table1_seg), r2.table2_geom))),GREATEST(ST_LineLocatePoint(r2.table2_geom, 
						  			ST_ClosestPoint(st_startpoint(r2.table1_seg), r2.table2_geom)) , ST_LineLocatePoint(r2.table2_geom, 
						  			ST_ClosestPoint(st_endpoint(r2.table1_seg), r2.table2_geom))) )))> overlap_perc_min	
			 			WHEN r1.table2_seg IS NULL 
			 			AND r2.table2_seg IS NOT NULL
							THEN 
								ST_Length(ST_Intersection(ST_LineSubstring(r1.table2_geom,LEAST(ST_LineLocatePoint(r1.table2_geom, 
									ST_ClosestPoint(st_startpoint(r1.table1_seg), r1.table2_geom)) , ST_LineLocatePoint(r1.table2_geom, 
									ST_ClosestPoint(st_endpoint(r1.table1_seg), r1.table2_geom))),GREATEST(ST_LineLocatePoint(r1.table2_geom, 
									ST_ClosestPoint(st_startpoint(r1.table1_seg), r1.table2_geom)) , ST_LineLocatePoint(r1.table2_geom, 
									ST_ClosestPoint(st_endpoint(r1.table1_seg), r1.table2_geom))) ), ST_Buffer(r2.table2_seg, buffer1,
									'endcap=flat join=round')) )
				  				/ LEAST(ST_Length(r2.table2_seg),ST_Length(ST_LineSubstring(r1.table2_geom,
				  					LEAST(ST_LineLocatePoint(r1.table2_geom, ST_ClosestPoint(st_startpoint(r1.table1_seg), r1.table2_geom)),
				  					ST_LineLocatePoint(r1.table2_geom, ST_ClosestPoint(st_endpoint(r1.table1_seg), r1.table2_geom))),
				  					GREATEST(ST_LineLocatePoint(r1.table2_geom, ST_ClosestPoint(st_startpoint(r1.table1_seg), r1.table2_geom)),
				  					ST_LineLocatePoint(r1.table2_geom, ST_ClosestPoint(st_endpoint(r1.table1_seg), r1.table2_geom))) ))) > overlap_perc_min
					  	WHEN r1.table2_seg IS NOT NULL 
					  	AND r2.table2_seg IS NOT NULL
							THEN 
								ST_Length(ST_Intersection(r1.table2_seg, ST_Buffer(r2.table2_seg, buffer1, 'endcap=flat join=round')) )
						  		/ LEAST(ST_Length(r1.table2_seg), ST_Length(r2.table2_seg)) > overlap_perc_min
					END
		)
	); 
	

	
	INSERT INTO automated.results (
    	table1_id,
    	table1_info,
    	table2_id,
    	table2_info,
    	conflation_score,
    	table1_conflated_geometry,
    	table1_original_geometry,
    	table2_conflated_geometry,
    	table2_original_geometry
    	-- Add more columns as needed
	)
		SELECT
    		sw.table1_id,
    		t1.info AS table1_info,
    		sw.table2_id,
    		t2.info AS table2_info,
    		sw.conflated_score,
    		sw.table1_seg AS table1_conflated_geometry,
    		sw.table1_geom AS table1_original_geometry,
    		sw.table2_seg AS table2_conflated_geometry,
    		sw.table2_geom AS table2_original_geometry
    	-- Add more values or expressions for additional columns
		FROM
    		sw_preprocessed AS sw
		JOIN
    		table1 t1 ON sw.table1_id = t1.id
		JOIN
			table2 t2 ON sw.table2_id = t2.id;	
	

END $$;

CALL sidewalk_to_sidewalk_conflation()

DROP TABLE sw_raw;
DROP TABLE sw_preprocessed;

SELECT * FROM sw_raw;
SELECT * FROM sw_prepocessed;

SELECT * FROM automated.results;



