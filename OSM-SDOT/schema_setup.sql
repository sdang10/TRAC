-- INSERT DOCUMENTATION HERE --


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
	(0, "system", "system", "password");



CREATE TABLE automated.files (
	file_id serial PRIMARY KEY,
	user_id integer NOT NULL REFERENCES automated.users(user_id),
	file_name TEXT NOT NULL,
	description TEXT NOT NULL,
	unique_identifier_column_name TEXT NOT NULL,
	geom_column_name TEXT NOT NULL,
	upload_dtm TIMESTAMP WITH TIME ZONE DEFAULT util.f_now_utc()
);

INSERT INTO automated.files VALUES
	(0, 0, "OSM", "osm data", "osm_id", "way", util.now_utc()),
	(1, 0, "SDOT", "sdot data", "objectid", "wkb_geometry", util.now_utc());



CREATE TABLE automated.file_data (
	file_id serial REFERENCES automated.files(file_id),
	geom geometry,
	data_attributes hstore
);



CREATE TABLE automated.conflations (
	conflation_id INT8 PRIMARY KEY,
	base_file_id integer NOT NULL REFERENCES automated.files(file_id),
	compared_file_id integer NOT NULL REFERENCES automated.files(file_id),
	process_dtm TIMESTAMP WITH TIME ZONE DEFAULT util.f_now_utc()
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
	conflation_id integer REFERENCES automated.conflations(conflation_id),
	table1_id INT8 NOT NULL,
	table1_info hstore,
	table2_id INT4 NOT NULL,
	table2_info hstore,
	conflation_score float8,
	table1_conflated_geometry geometry,
	table1_original_geometry geometry,
	table2_conflated_geometry geometry,
	table2_original_geometry geometry,
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
