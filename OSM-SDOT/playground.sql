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
















