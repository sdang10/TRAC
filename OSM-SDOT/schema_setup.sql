-- INSERT DOCUMENTATION HERE --



-- this function will take an input of computed angle (_rad) between any 2 linestring, and an input of a tolerence angle threshold (_thresh)
-- to calculate whether or not the computed angle between 2 linestrings are within the threshold so we know if these 2 are parallel
CREATE OR REPLACE FUNCTION public.f_within_degrees(_rad DOUBLE PRECISION, _thresh integer) RETURNS boolean AS $$
    WITH m AS (SELECT mod(degrees(_rad)::NUMERIC, 180) AS angle)
        ,a AS (SELECT CASE WHEN m.angle > 90 THEN m.angle - 180 ELSE m.angle END AS angle FROM m)
    SELECT abs(a.angle) < _thresh FROM a;
$$ LANGUAGE SQL IMMUTABLE STRICT;



CREATE TABLE automated.users (
	user_id serial PRIMARY KEY,
	username TEXT,
	email TEXT,
	password TEXT
);



CREATE TABLE automated.files (
	file_id serial PRIMARY KEY,
	user_id REFERENCES automated.users(user_id),
	file_name TEXT,
	upload_dtm TIMESTAMP WITH TIME ZONE,
	description TEXT
);



SELECT date_trunc('second', clock_timestamp() at time zone 'utc');




CREATE TABLE automated.conflations (
	conflation_id INT8 PRIMARY KEY,
	base_file_ID integer REFERENCES automated.files(file_id),
	compared_file_ID integer REFERENCES automated.files(file_id),
	process_dtm TIMESTAMP WITH TIME ZONE
);




CREATE TABLE automated.statuses (
	status_id integer PRIMARY KEY,
	status_name TEXT,
	status_description TEXT
);




CREATE TABLE automated.results (
	osm_id INT8,
	start_end_seg TEXT,
	sdot_id INT4,
	highway TEXT,
	osm_surface TEXT,
	sdot_surface TEXT,
	sw_width NUMERIC,
	tags hstore,
	conflation_score float8,
	original_way TEXT,
	way geometry,
	conflation_id integer REFERENCES automated.conflations(conflation_id),
	user_ID integer REFERENCES automated.users(user_id),
	status_id integer REFERENCES automated.statuses(status_id),
	action_dtm TIMESTAMP WITH TIME ZONE
);




CREATE TABLE automated.metrics (
	conflation_id serial REFERENCES automated.conflations(conflation_id),
	file_ID integer REFERENCES automated.files(file_id),
	segment_ID integer,
	geom geometry,
	percent_score float8,
	conflated_geoms geometry
);
	
CREATE TABLE automated.file_data(
	data_id TEXT,
	file_id REFERENCES automated.files(file_id),
	geom geometry,
	data_attributes hstore
);
