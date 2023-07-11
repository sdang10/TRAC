---- DATABASE SETUP ----


--- OSM ---
-- planet_osm_line
-- planet_osm_point
-- planet_osm_polygon
-- planet_osm_roads
-- spatial_ref_sys

-- we need to change 'way' to 'geom' column name

CREATE TABLE shane_data_setup.osm_lines AS (
	SELECT * 
	FROM public.planet_osm_line
)
ALTER TABLE shane_data_setup.osm_lines RENAME COLUMN way TO geom;


CREATE TABLE shane_data_setup.osm_points AS (
	SELECT * 
	FROM public.planet_osm_point
)
ALTER TABLE shane_data_setup.osm_points RENAME COLUMN way TO geom;


CREATE TABLE shane_data_setup.osm_polygons AS (
	SELECT * 
	FROM public.planet_osm_polygon
)
ALTER TABLE shane_data_setup.osm_polygons RENAME COLUMN way TO geom;


CREATE TABLE shane_data_setup.osm_roads AS (
	SELECT * 
	FROM public.planet_osm_roads
)
ALTER TABLE shane_data_setup.osm_roads RENAME COLUMN way TO geom;


--- ARNOLD ---
-- wapr_hpms_subittal 

-- we need to change multilinestring into linestring
-- put spacers between words like objectid, beginmeasure -> object_id, begin_measure  etc.

CREATE TABLE shane_data_setup.arnold_lines (
  	og_object_id INT8,
  	object_id SERIAL,
  	route_id VARCHAR(75),
  	begin_measure FLOAT8,
  	end_measure FLOAT8,
  	shape_length FLOAT8,
  	geom geometry(linestring, 3857),
  	shape geometry(multilinestringm, 3857)
);

INSERT INTO shane_data_setup.arnold_lines (og_object_id, route_id, begin_measure, end_measure, shape_length, geom, shape)
	SELECT  
		objectid, 
		routeid, 
		beginmeasure, 
		endmeasure, 
		shape_length, 
		ST_Force2D((ST_Dump(shape)).geom)::geometry(linestring, 3857),
		shape
	FROM arnold.wapr_hpms_submittal;
