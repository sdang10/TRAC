CREATE OR REPLACE PROCEDURE transform_table()
LANGUAGE plpgsql
AS $$

DECLARE
    
	column_names text[];
    column_values text[];
    query_string text;
    row_record RECORD;
    id_column text;
    geom_column text;
	
BEGIN
	
    -- Get column names from information schema
    SELECT get_column_names('sidewalks')
    INTO column_names;

    -- Identify the positions of 'id' and 'geom' columns
    id_column := (SELECT column_name FROM information_schema.columns WHERE table_schema = 'automated' AND table_name = 'sidewalks' AND column_name = 'objectid');
    geom_column := (SELECT column_name FROM information_schema.columns WHERE table_schema = 'automated' AND table_name = 'sidewalks' AND column_name = 'wkb_geometry');

    -- Create a new table with three columns: id, geom, and info (hstore)
    EXECUTE 'CREATE TABLE automated.new_table (id INTEGER, geom geometry, info hstore)';

	-- 
	FOR row_record IN EXECUTE 'SELECT * FROM automated.sidewalks' LOOP
		-- Extract values dynamically
        EXECUTE 'SELECT $1.' || array_to_string(column_names, ', $1.') INTO column_values USING row_record;

        -- Construct and execute dynamic INSERT query
        query_string := 'INSERT INTO automated.new_table (id, geom, info) VALUES (' || id_column || ', ' || geom_column || ', hstore(ARRAY[' || quote_literal(column_names) || '], ARRAY[' || column_values || ']))';
        EXECUTE query_string USING row_record;
    END LOOP; 
END $$;









DROP FUNCTION transform_table


CREATE OR REPLACE FUNCTION transform_table(the_table_name TEXT, id_column TEXT, geom_column TEXT)
RETURNS TABLE (id INTEGER, geom geometry, info hstore)
LANGUAGE plpgsql AS $$
DECLARE
    column_names text;
    column_values text;
    specific_column_value text;
    query_string text;
    row_record RECORD;
BEGIN
    -- Validate or sanitize the input table name to prevent SQL injection
    -- Example: Add your validation logic here
    -- IF the_table_name ~ '^[a-zA-Z_][a-zA-Z0-9_]*$' THEN
    --   RAISE EXCEPTION 'Invalid table name';
    -- END IF;

    -- Get column names from information schema
    SELECT get_column_names(the_table_name) INTO column_names;

    -- Create a new table with three columns: id, geom, and info (hstore)
    EXECUTE format('CREATE TABLE automated.new_table (id INTEGER, geom geometry, info hstore)');

    -- Iterate through each row in the original table.
    FOR row_record IN EXECUTE format('SELECT * FROM automated.%I', the_table_name) LOOP
        -- Initialize column_values for each row
        column_values := '';

        -- Iterate through each column in the row
        FOR specific_column_value IN SELECT row_record.* LOOP
            -- Handle NULL values if necessary
            IF specific_column_value IS NULL THEN
                specific_column_value := 'NULL';
            ELSE
                -- Quote and escape values appropriately
                specific_column_value := quote_literal(specific_column_value);
            END IF;

            -- appending the specified values into a comma-separated list
            column_values := column_values || specific_column_value || ', ';
        END LOOP;

        -- Trim the trailing comma and space
        column_values := rtrim(column_values, ', ');

        -- Construct and execute dynamic INSERT query using values from the row_record
        query_string := 'INSERT INTO automated.new_table (id, geom, info) VALUES (' || row_record.id_column || ', ' || row_record.geom_column || ', hstore(ARRAY[' || quote_literal(column_names) || '], ARRAY[' || column_values || ']))';

        -- Execute the dynamic query
        EXECUTE query_string;
    END LOOP;

    -- Return the transformed table
    RETURN QUERY EXECUTE 'SELECT * FROM automated.new_table';
END $$;









SELECT transform_table('sidewalks', 'objectid', 'wkb_geometry');


















CALL transform_table()




CREATE OR REPLACE PROCEDURE mhm()
LANGUAGE plpgsql AS $$
DECLARE 
	column_names TEXT;
	column_names_array text[];
BEGIN 
	
	SELECT get_column_names('sidewalks')
    INTO column_names;
   
   	-- Convert the comma-separated string to an array
   	column_names_array := string_to_array(column_names, ', ');
   
   	-- Output the array
    RAISE NOTICE 'Column Names: %', column_names_array;
	
END $$;

CALL mhm();










DROP FUNCTION get_column_names

CREATE OR REPLACE FUNCTION get_column_names(the_table_name TEXT, col1_name TEXT DEFAULT NULL, col2_name TEXT DEFAULT NULL)
RETURNS text AS $$
DECLARE
    column_list text;
BEGIN
    SELECT string_agg(column_name, ', ') INTO column_list
    FROM information_schema.columns
    WHERE table_schema = 'automated' AND table_name = the_table_name
    AND (
		(col1_name IS NULL AND col2_name IS NULL)
		OR (column_name NOT IN (col1_name, col2_name))
	);

    RETURN column_list;
END;
$$ LANGUAGE plpgsql;



SELECT get_column_names('sidewalks')
SELECT get_column_names('sidewalks', 'objectid', 'unittype')












SELECT * FROM merge_columns_to_hstore('sidewalks', 'objectid', 'wkb_geometry')




SELECT * FROM pg_extension WHERE extname = 'hstore';

















CREATE OR REPLACE PROCEDURE modify_table()
LANGUAGE plpgsql AS $$

DECLARE

    row_record RECORD;

BEGIN
	
    -- Create a new table with three columns: id, geom, and info (hstore).
    CREATE TABLE automated.new_table (
        id INTEGER,
        geom geometry,
        info hstore
    );

    -- Iterate through each row in the original table.
    FOR row_record IN (SELECT * FROM automated.sidewalks) LOOP
	    
        -- Insert into the new table with only the necessary columns.
        INSERT INTO automated.new_table (id, geom, info)
        VALUES (
        	row_record.objectid, 
        	row_record.wkb_geometry, 
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



CALL modify_table()

SELECT * FROM automated.new_table


SELECT *
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_NAME = N'sidewalks'



































CREATE OR REPLACE FUNCTION filter_by_bbox(table_name text, bbox box2d)
RETURNS TABLE (id INTEGER, geom geometry, info hstore) AS $$
BEGIN
	RETURN QUERY EXECUTE
		format(
			'SELECT id, (ST_Dump(geom)).geom AS geom, info 
				FROM automated.%I
            	WHERE geom && %L', table_name, bbox)
        USING bbox;
END;
$$ LANGUAGE plpgsql;

DROP FUNCTION filter_by_bbox(text,geometry)

SELECT * FROM filter_by_bbox('new_table', st_setsrid( st_makebox2d( st_makepoint(-13616215,6052850), st_makepoint(-13614841,6054964)), 3857))




