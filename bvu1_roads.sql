---- PROCESS ----


CREATE TABLE shane.bvu1_osm_roads AS (
	SELECT *
	FROM shane.osm_lines
	WHERE highway IN ('motorway', 'trunk', 'primary', 'secondary', 'tertiary', 'road', 'busway', 'unclassified') 
		AND geom && st_setsrid( st_makebox2d( st_makepoint(-13603442,6043723), st_makepoint(-13602226,6044848)), 3857) );

	

-- checkpoint --
	-- for roads that were unintentionally left out
	-- NOTE: later we might want to include 'residential' AND 'service' roads as they are still roads, but they just don't conflate to 
	-- datasets (arnold) for now as they are not main roads
SELECT * 
FROM shane.osm_lines
WHERE osm_id NOT IN (
	SELECT osm_id
	FROM shane.bvu1_osm_roads
) AND geom && st_setsrid( st_makebox2d( st_makepoint(-13603442,6043723), st_makepoint(-13602226,6044848)), 3857)
AND highway != 'footway';
		


-- create lane number column
CREATE TABLE shane.bvu1_osm_roads_add_lanes AS (
	SELECT osm_id, name, CAST(tags -> 'lanes' AS int) lanes, geom
	FROM shane.bvu1_osm_roads
	WHERE tags ? 'lanes' );



-- For those that do not have the lanes, if the road seg (with no lanes info) has the end/start point
-- intersecting with another end/start point of the road seg (with lanes info), and they are parallel to each other,
-- inherit the lanes info from the existing shane.bvu1_osm_roads_add_lanes table 

-- WHILE LOOP TO RUN MORE THAN ONCE, CURRENTLY COUNT = 2
INSERT INTO shane.bvu1_osm_roads_add_lanes(osm_id, name, lanes, geom)
WITH ranked_road AS (
	SELECT 
		r2.osm_id AS osm_id, 
		r2.name AS osm_road_name, 
		r1.lanes AS lanes, 
		r2.geom AS osm_geom,
		ROW_NUMBER() OVER (
			PARTITION BY r2.osm_id
		   	ORDER BY r1.lanes DESC
		) AS RANK 
	FROM shane.bvu1_osm_roads_add_lanes r1
	JOIN (
		SELECT osm_id, name, highway, geom
		FROM shane.bvu1_osm_roads
		WHERE tags -> 'lanes' IS NULL) AS r2
		ON ST_Intersects(st_startpoint(r1.geom), st_startpoint(r2.geom))
		OR ST_Intersects(st_startpoint(r1.geom), st_endpoint(r2.geom))
		OR ST_Intersects(st_endpoint(r1.geom), st_startpoint(r2.geom))
		OR ST_Intersects(st_endpoint(r1.geom), st_endpoint(r2.geom))
	WHERE r1.osm_id != r2.osm_id
	AND ( -- osm road should be PARALLEL TO the roads that ARE IN the 
		ABS(DEGREES(ST_Angle(r1.geom, r2.geom))) BETWEEN 0 AND 10 -- 0 
		OR ABS(DEGREES(ST_Angle(r1.geom, r2.geom))) BETWEEN 170 AND 190 -- 180
		OR ABS(DEGREES(ST_Angle(r1.geom, r2.geom))) BETWEEN 350 AND 360  ) -- 360 
	AND r2.osm_id NOT IN(
		SELECT osm_id FROM shane.bvu1_osm_roads_add_lanes
	)		    
)	    
SELECT 
	osm_id, 
	osm_road_name, 
	lanes, 
	osm_geom
FROM ranked_road
WHERE RANK = 1


	
	
-- this is the table we are checking if the osm road are the same as the arnold road
	-- check if they are parallel and the arnold intersects the buffer from the osm road (the buffer size depend on the lanes)
CREATE TABLE shane.bvu1_conflation_road_case AS 
WITH ranked_roads AS (
	SELECT 
	arnold.og_object_id AS arnold_object_id, 
	arnold.shape AS arnold_shape, 
	osm.osm_id AS osm_id, 
	osm.name AS osm_road_name,  
	osm.geom AS osm_geom,
	ST_LineSubstring( arnold.geom, LEAST(ST_LineLocatePoint(arnold.geom, ST_ClosestPoint(st_startpoint(osm.geom), arnold.geom)), 
		ST_LineLocatePoint(arnold.geom, ST_ClosestPoint(st_endpoint(osm.geom), arnold.geom))), GREATEST(ST_LineLocatePoint(arnold.geom,
		ST_ClosestPoint(st_startpoint(osm.geom), arnold.geom)) , ST_LineLocatePoint(arnold.geom, ST_ClosestPoint(st_endpoint(osm.geom), 
		arnold.geom))) ) AS seg_geom,
	ROW_NUMBER() OVER (
		PARTITION BY osm.geom
		ORDER BY ST_distance( ST_LineSubstring( arnold.geom, LEAST(ST_LineLocatePoint(arnold.geom, ST_ClosestPoint(st_startpoint(osm.geom),
			arnold.geom)) , ST_LineLocatePoint(arnold.geom, ST_ClosestPoint(st_endpoint(osm.geom), arnold.geom))),
		  	GREATEST(ST_LineLocatePoint(arnold.geom, ST_ClosestPoint(st_startpoint(osm.geom), arnold.geom)) , ST_LineLocatePoint(arnold.geom,
		  	ST_ClosestPoint(st_endpoint(osm.geom), arnold.geom))) ), osm.geom )
	) AS RANK
	FROM shane.bvu1_osm_roads_add_lanes AS osm
	RIGHT JOIN shane.bvu1_arnold_lines AS arnold
	ON ST_Intersects(ST_buffer(osm.geom, (osm.lanes+1)*4), arnold.geom)
	WHERE (  ABS(DEGREES(ST_Angle(ST_LineSubstring( arnold.geom, LEAST(ST_LineLocatePoint(arnold.geom, 
		ST_ClosestPoint(st_startpoint(osm.geom), arnold.geom)) , ST_LineLocatePoint(arnold.geom, ST_ClosestPoint(st_endpoint(osm.geom), arnold.geom))),
		GREATEST(ST_LineLocatePoint(arnold.geom, ST_ClosestPoint(st_startpoint(osm.geom), arnold.geom)) , ST_LineLocatePoint(arnold.geom, 
		ST_ClosestPoint(st_endpoint(osm.geom), arnold.geom))) ), osm.geom))) BETWEEN 0 AND 15 -- 0 
	OR ABS(DEGREES(ST_Angle(ST_LineSubstring( arnold.geom,
		LEAST(ST_LineLocatePoint(arnold.geom, ST_ClosestPoint(st_startpoint(osm.geom), arnold.geom)) , ST_LineLocatePoint(arnold.geom, 
		ST_ClosestPoint(st_endpoint(osm.geom), arnold.geom))), GREATEST(ST_LineLocatePoint(arnold.geom, ST_ClosestPoint(st_startpoint(osm.geom),
		arnold.geom)) , ST_LineLocatePoint(arnold.geom, ST_ClosestPoint(st_endpoint(osm.geom), arnold.geom))) ), osm.geom))) BETWEEN 165 AND 195 -- 180
	OR ABS(DEGREES(ST_Angle(ST_LineSubstring( arnold.geom, LEAST(ST_LineLocatePoint(arnold.geom, ST_ClosestPoint(st_startpoint(osm.geom), arnold.geom)),
		ST_LineLocatePoint(arnold.geom, ST_ClosestPoint(st_endpoint(osm.geom), arnold.geom))), 
		GREATEST(ST_LineLocatePoint(arnold.geom, ST_ClosestPoint(st_startpoint(osm.geom), arnold.geom)), 
		ST_LineLocatePoint(arnold.geom, ST_ClosestPoint(st_endpoint(osm.geom), arnold.geom))) ), osm.geom))) BETWEEN 345 AND 360 ) -- 360
)
SELECT
	osm_id,
	osm_road_name,
	osm_geom,
	arnold_object_id,
	seg_geom AS arnold_geom,
	arnold_shape
FROM ranked_roads
WHERE rank = 1;







		 
INSERT INTO shane.bvu1_conflation_road_case(osm_id, osm_road_name, osm_geom, arnold_object_id, arnold_geom)
SELECT DISTINCT 
	arnold_osm.objectid, 
	arnold_osm.og_objectid, 
	lanes.osm_id, 
	lanes.name,
	lanes.geom
FROM shane.bvu1_osm_roads_add_lanes lanes
JOIN shane.bvu1_conflation_road_case arnold_osm
	ON lanes.name = arnold_osm.name 
AND ( ST_Intersects(st_startpoint(lanes.geom), st_startpoint(arnold_osm.osm_geom))
	OR ST_Intersects(st_startpoint(lanes.geom), st_endpoint(arnold_osm.osm_geom))
	OR ST_Intersects(st_endpoint(lanes.geom), st_startpoint(arnold_osm.osm_geom))
	OR ST_Intersects(st_endpoint(lanes.geom), st_endpoint(arnold_osm.osm_geom))
)
WHERE lanes.osm_id NOT IN (
	SELECT osm_id
	FROM shane.bvu1_conflation_road_case
)	
GROUP BY arnold_osm.objectid, arnold_osm.og_objectid, lanes.osm_id, lanes.name, lanes.highway, lanes.geom

	








CREATE TABLE <schema>.arnold_osm_conflated AS
	SELECT objectid, og_objectid, osm_id, osm_geom FROM shane.bvu1_conflation_road_case


