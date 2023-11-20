-- INSERT DOCUMENTATION HERE --



-- this function will take an input of computed angle (_rad) between any 2 linestring, and an input of a tolerence angle threshold (_thresh)
-- to calculate whether or not the computed angle between 2 linestrings are within the threshold so we know if these 2 are parallel
CREATE OR REPLACE FUNCTION public.f_within_degrees(_rad DOUBLE PRECISION, _thresh bigint) RETURNS boolean AS $$
    WITH m AS (SELECT mod(degrees(_rad)::NUMERIC, 180) AS angle)
        ,a AS (SELECT CASE WHEN m.angle > 90 THEN m.angle - 180 ELSE m.angle END AS angle FROM m)
    SELECT abs(a.angle) < _thresh FROM a;
$$ LANGUAGE SQL IMMUTABLE STRICT;



CREATE TABLE automated.users AS
	ID INT8 PRIMARY KEY;
	name TEXT;
	email TEXT;
	password TEXT;



CREATE TABLE automated.files AS
	ID INT8 PRIMARY KEY;
	name TEXT;
	upload_dtm TIMESTAMP WITH TIME ZONE;
	description TEXT;
	FOREIGN KEY(user_ID) REFERENCES users(ID);



CREATE TABLE automated.conflations AS
	ID INT8 PRIMARY KEY;
	FOREIGN KEY(base_file_ID) REFERENCES files(ID);
	FOREIGN KEY(compared_file_ID) REFERENCES files(ID);
	process_dtm TIMESTAMP WITH TIME ZONE;



CREATE TABLE automated.results AS
	OSM_ID INT8;
	start_end_seg TEXT;
	SDOT_ID INT4;
	highway TEXT;
	OSM_surface TEXT;
	SDOT_surface TEXT;
	width NUMERIC;
	tags hstore;
	conflation_score float8;
	original_way TEXT;
	way geometry;
	FOREIGN KEY(conflation_ID) REFERENCES conflations(ID);
	FOREIGN KEY(user_ID) REFERENCES users(ID);
	status INT8;
	action_dtm TIMESTAMP WITH TIME ZONE;



CREATE TABLE automated.metrics AS
	FOREIGN KEY(conflation_ID) REFERENCES conflations(ID);
	FOREIGN KEY(file_ID) REFERENCES files(ID);
	FOREIGN KEY(file_name) REFERENCES files(name);
	segment_ID INT8;
	geom geometry;
	percent_score float8;
	conflated_geoms geometry;
	

