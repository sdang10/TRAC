-- INSERT DOCUMENTATION HERE --



-- this function will take an input of computed angle (_rad) between any 2 linestring, and an input of a tolerence angle threshold (_thresh)
-- to calculate whether or not the computed angle between 2 linestrings are within the threshold so we know if these 2 are parallel
CREATE OR REPLACE FUNCTION public.f_within_degrees(_rad DOUBLE PRECISION, _thresh integer) RETURNS boolean AS $$
    WITH m AS (SELECT mod(degrees(_rad)::NUMERIC, 180) AS angle)
        ,a AS (SELECT CASE WHEN m.angle > 90 THEN m.angle - 180 ELSE m.angle END AS angle FROM m)
    SELECT abs(a.angle) < _thresh FROM a;
$$ LANGUAGE SQL IMMUTABLE STRICT;



CREATE SCHEMA IF NOT EXISTS util;

ALTER SCHEMA util OWNER TO postgres;

CREATE OR REPLACE FUNCTION util.f_now_utc() RETURNS timestamp AS $$
    SELECT date_trunc('second', clock_timestamp() at time zone 'utc');
$$ LANGUAGE SQL STRICT;



CREATE TABLE automated.users (
	user_id serial PRIMARY KEY,
	username TEXT,
	email TEXT,
	user_password TEXT
);

INSERT INTO automated.users VALUES 
(0, "system", "machine", "password");



CREATE TABLE automated.files (
	file_id serial PRIMARY KEY,
	user_id NOT NULL REFERENCES automated.users(user_id),
	file_name TEXT,
	description TEXT,
	upload_dtm TIMESTAMP WITH TIME ZONE DEFAULT util.now_utc()
);

INSERT INTO automated.files VALUES
(0, 0, "OSM", "osm data", util.now_utc()),
(1, 0, "SDOT", "sdot data", util.now_utc());



CREATE TABLE automated.conflations (
	conflation_id INT8 PRIMARY KEY,
	base_file_id integer NOT NULL REFERENCES automated.files(file_id),
	compared_file_id integer NOT NULL REFERENCES automated.files(file_id),
	process_dtm TIMESTAMP WITH TIME ZONE DEFAULT util.now_utc()
);



CREATE TABLE automated.statuses (
	status_id integer PRIMARY KEY,
	status_name TEXT,
	status_description TEXT
);



INSERT INTO automated.statuses VALUES 
(0, "system_approved", "approved by the system automatically"),
(1, "approved", "approved by manual review"),
(10, "system_rejected", "rejected by the system automatically"),
(11, "reject_base", "rejected for base line work"),
(12, "reject_compared", "rejected for compared data line work"),
(13, "reject_other", "rejected for other reason");



CREATE TABLE automated.results (
	result_id serial PRIMARY KEY,
	osm_id INT8 NOT NULL,
	start_end_seg TEXT,
	sdot_id INT4 NOT NULL,
	highway TEXT,
	osm_surface TEXT,
	sdot_surface TEXT,
	sw_width NUMERIC,
	tags hstore,
	conflation_score float8,
	original_way TEXT,
	way geometry,
	conflation_id integer REFERENCES automated.conflations(conflation_id),
	default_status_id integer REFERENCES automated.statuses(status_id)
);



CREATE TABLE automated.results_review (
	results_review_id serial PRIMARY KEY,
	user_id integer NOT NULL REFERENCES automated.users(user_id),
	status_id integer REFERENCES automated.statuses(status_id),
	note TEXT,
	action_dtm TIMESTAMP WITH TIME ZONE,
	result_id integer NOT NULL REFERENCES automated.results(result_id)
);




CREATE TABLE automated.metrics (
	conflation_id serial REFERENCES automated.conflations(conflation_id),
	file_id integer REFERENCES automated.files(file_id),
	segment_id integer,
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
