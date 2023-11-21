-- INSERT DOCUMENTATION HERE --



-- this function will take an input of computed angle (_rad) between any 2 linestring, and an input of a tolerence angle threshold (_thresh)
-- to calculate whether or not the computed angle between 2 linestrings are within the threshold so we know if these 2 are parallel
CREATE OR REPLACE FUNCTION public.f_within_degrees(_rad DOUBLE PRECISION, _thresh bigint) RETURNS boolean AS $$
    WITH m AS (SELECT mod(degrees(_rad)::NUMERIC, 180) AS angle)
        ,a AS (SELECT CASE WHEN m.angle > 90 THEN m.angle - 180 ELSE m.angle END AS angle FROM m)
    SELECT abs(a.angle) < _thresh FROM a;
$$ LANGUAGE SQL IMMUTABLE STRICT;



CREATE TABLE automated.users (
	ID INT8 PRIMARY KEY,
	username TEXT,
	email TEXT,
	password TEXT
);



CREATE TABLE automated.files (
	ID INT8 PRIMARY KEY,
	FOREIGN KEY(ID) REFERENCES automated.users(ID),
	name TEXT,
	upload_dtm TIMESTAMP WITH TIME ZONE,
	description TEXT
);



CREATE TABLE automated.conflations (
	ID INT8 PRIMARY KEY,
	base_file_ID INT8,
	FOREIGN KEY(base_file_ID) REFERENCES automated.files(ID),
	compared_file_ID INT8,
	FOREIGN KEY(compared_file_ID) REFERENCES automated.files(ID),
	process_dtm TIMESTAMP WITH TIME ZONE
);



CREATE TABLE automated.results (
	OSM_ID INT8,
	start_end_seg TEXT,
	SDOT_ID INT4,
	highway TEXT,
	OSM_surface TEXT,
	SDOT_surface TEXT,
	width NUMERIC,
	tags hstore,
	conflation_score float8,
	original_way TEXT,
	way geometry,
	conflation_ID INT8,
	FOREIGN KEY(conflation_ID) REFERENCES automated.conflations(ID),
	user_ID INT8,
	FOREIGN KEY(user_ID) REFERENCES automated.users(ID),
	status INT8,
	action_dtm TIMESTAMP WITH TIME ZONE
);



CREATE TABLE automated.metrics (
	conflation_ID INT8,
	FOREIGN KEY(conflation_ID) REFERENCES automated.conflations(ID),
	file_ID INT8,
	FOREIGN KEY(file_ID) REFERENCES automated.files(ID),
	segment_ID INT8,
	geom geometry,
	percent_score float8,
	conflated_geoms geometry
);
	
