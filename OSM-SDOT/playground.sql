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
    SELECT array_agg(column_name)
    INTO column_names
    FROM information_schema.columns
    WHERE table_schema = 'automated' AND table_name = 'sidewalks' AND column_name NOT IN ('objectid', 'wkb_geometry');

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




CALL transform_table()
