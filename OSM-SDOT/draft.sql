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


---------------------------------------------------------------------------------------------------------------------------------------------------------------


DROP FUNCTION transform_table

CREATE OR REPLACE FUNCTION transform_table_1(the_table_name TEXT, id_column TEXT, geom_column TEXT)
RETURNS TABLE (id INTEGER, geom geometry, info hstore)
LANGUAGE plpgsql AS $$
DECLARE
    column_names text;
    column_names_array text[];
    column_values text;
    specific_column_value text;
    query_string text;
    row_record RECORD;
BEGIN
    -- Validate or sanitize the input table name to prevent SQL injection
    -- Example: Add your validation logic here
--    IF the_table_name ~ '^[a-zA-Z_][a-zA-Z0-9_]*$' 
--    	THEN RAISE EXCEPTION 'Invalid table name';
--    END IF;

    -- Get column names from information schema
    SELECT get_column_names(the_table_name) INTO column_names;
   
    -- Convert the comma-separated string to an array
   	column_names_array := string_to_array(column_names, ', ');

    -- Create a new table with three columns: id, geom, and info (hstore)
    EXECUTE format('CREATE TABLE automated.new_table (id INTEGER, geom geometry, info hstore)');

    -- Iterate through each row in the original table.
    FOR i IN 1..array_length LOOP
        -- Initialize column_values for each row
        column_values := '';

        -- Iterate through each row in the column
        FOR row_record IN EXECUTE format('SELECT * FROM automated.%I', the_table_name) LOOP
            -- Handle NULL values if necessary
            IF row_record[specific_column] IS NULL THEN
                specific_column_value := 'NULL';
            ELSE
                -- Quote and escape values appropriately
                specific_column_value := quote_literal(row_record[specific_column]);
            END IF;

            -- appending the specified values into a comma-separated list
            column_values := column_values || specific_column_value || ', ';
        END LOOP;

        -- Trim the trailing comma and space
        column_values := rtrim(column_values, ', ');

        -- Construct and execute dynamic INSERT query using values from the row_record
		query_string := 'INSERT INTO automated.new_table (id, geom, info) VALUES (
			' || row_record[id_column] || ', ' || row_record[geom_column] || ', hstore(ARRAY[' || quote_literal(column_names) || '], 
			ARRAY[' || quote_literal(column_values) || ']))';

		-- Execute the dynamic query
		EXECUTE query_string;

    END LOOP;

    -- Return the transformed table
    RETURN QUERY EXECUTE 'SELECT * FROM automated.new_table';
END $$;


SELECT transform_table('sidewalks', 'objectid', 'wkb_geometry');


---------------------------------------------------------------------------------------------------------------------------------------------------------------


DROP FUNCTION modify_geom()

CREATE OR REPLACE FUNCTION modify_geom(the_table_name TEXT)
RETURNS TABLE (id INTEGER, geom geometry, info hstore)
LANGUAGE plpgsql AS $$

BEGIN
	
	EXECUTE format('
        SELECT
            id,
            CASE
                WHEN ST_GeometryType(geom) = ''LINESTRING'' THEN
                    geom
                WHEN ST_GeometryType(geom) = ''MULTILINESTRING'' THEN
                    (ST_Dump(geom)).geom
                ELSE
                    NULL
            END AS geom,
            info
        FROM
            automated.%I', the_table_name)
    INTO STRICT id, geom, info;

    RETURN NEXT;
   
END $$;


SELECT modify_geom('new_table')


---------------------------------------------------------------------------------------------------------------------------------------------------------------


DROP FUNCTION filter_by_bbox(text,geometry)

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


SELECT * FROM filter_by_bbox('sdot_table', st_setsrid( st_makebox2d( st_makepoint(-13616215,6052850), st_makepoint(-13614841,6054964)), 3857))


---------------------------------------------------------------------------------------------------------------------------------------------------------------


CREATE OR REPLACE PROCEDURE setup(table1_name TEXT, table2_name TEXT, bb box2d DEFAULT NULL, osm_presence TEXT DEFAULT NULL)
LANGUAGE plpgsql AS $$
DECLARE

	-- make tables query
	make_table1_query TEXT;
	make_table2_query TEXT;

BEGIN 
	
	make_table1_query := 'CREATE TEMP TABLE table1 AS SELECT * FROM';
	make_table2_query := 'CREATE TEMP TABLE table2 AS SELECT * FROM';
	
	-- formatting tables to bbox, if there is one
	IF bb IS NOT NULL THEN 
		make_table1_query := make_table1_query || ' filter_by_bbox(' || table1_name || ', ' || bb || ')';
		make_table2_query := make_table2_query || ' filter_by_bbox(' || table2_name || ', ' || bb || ')';
	ELSE 
		make_table1_query := make_table1_query || ' ' || table1_name;
		make_table2_query := make_table2_query || ' ' || table2_name;
	END IF;

	IF osm_presence IS NOT NULL THEN 
		make_table1_query := make_table1_query || ' WHERE info -> ''highway'' = ''footway''';
		-- AND tags->'footway'='sidewalk' 
	END IF;

	EXECUTE make_table1_query;
	EXECUTE make_table2_query;

	-- index for better spatial query performance
	-- CREATE INDEX sdot_geom ON sdot_sidewalk USING GIST (geom);

END $$;


CALL setup('osm_table', 'sdot_table', st_setsrid( st_makebox2d( st_makepoint(-13632381,6008135), st_makepoint(-13603222,6066353)), 3857), 'yes');
