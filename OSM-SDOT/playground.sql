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

