---- PROCESS ----


CREATE TABLE shane_ch1_roads.osm_roads AS (
	SELECT *
	FROM shane_data_setup.osm_lines
	WHERE highway IN ('motorway', 'trunk', 'primary', 'secondary', 'tertiary', 'road', 'busway', 'unclassified', 'residential')
	AND geom && st_setsrid( st_makebox2d( st_makepoint(-13617414,6042417), st_makepoint(-13614697,6045266)), 3857)
); -- count: 518

	
-- checkpoint --
	-- for roads that were unintentionally left out
	-- NOTE: later we might want to include 'service' and other roads as they are still roads, but they just don't conflate to 
	-- datasets (arnold) for now as they are not main roads
SELECT * 
FROM shane_data_setup.osm_lines
WHERE osm_id NOT IN (
	SELECT osm_id
	FROM shane_ch1_roads.osm_roads
) AND geom && st_setsrid( st_makebox2d( st_makepoint(-13617414,6042417), st_makepoint(-13614697,6045266)), 3857)
AND highway != 'footway';
		

-- arnold lines in ch1 --
CREATE TABLE shane_ch1_roads.arnold_lines AS (
	SELECT *
 	FROM shane_data_setup.arnold_lines
	WHERE geom && st_setsrid( st_makebox2d( st_makepoint(-13617414,6042417), st_makepoint(-13614697,6045266)), 3857)
); -- count: 148



-- create lane number column
CREATE TABLE shane_ch1_roads.osm_roads_add_lanes AS (
	SELECT osm_id, name AS osm_road_name, CAST(tags -> 'lanes' AS int) lanes, geom
	FROM shane_ch1_roads.osm_roads
	WHERE tags ? 'lanes' 
); -- count: 515


-- checkpoint --
	-- what roads have no lanes?
SELECT * FROM shane_ch1_roads.osm_roads
WHERE osm_id NOT IN (
	SELECT osm_id FROM shane_ch1_roads.osm_roads_add_lanes
);




-- For those that do not have the lanes, if the road seg (with no lanes info) has the end/start point
-- intersecting with another end/start point of the road seg (with lanes info), and they are parallel to each other,
-- inherit the lanes info from the existing shane_ch1_roads._osm_roads_add_lanes table 

-- WHILE LOOP TO RUN MORE THAN ONCE, CURRENTLY COUNT = 2
INSERT INTO shane_ch1_roads.osm_roads_add_lanes(osm_id, osm_road_name, lanes, geom)
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
	FROM shane_ch1_roads.osm_roads_add_lanes r1
	JOIN (
		SELECT osm_id, name, highway, geom
		FROM shane_ch1_roads.osm_roads
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
		SELECT osm_id FROM shane_ch1_roads.osm_roads_add_lanes
	)		    
)	    
SELECT 
	osm_id, 
	osm_road_name, 
	lanes, 
	osm_geom
FROM ranked_road
WHERE RANK = 1; -- count: 3 total after looping twice


-- checkpoint --
	-- what's in? --
SELECT * FROM shane_ch1_roads.osm_roads_add_lanes
	-- what's not in? --
SELECT * FROM shane_ch1_roads.osm_roads
WHERE osm_id NOT IN (
	SELECT osm_id FROM shane_ch1_roads.osm_roads_add_lanes
);


	
	
-- conflates osm roads to arnold roads based on parallel and buffer intersection (the buffer is scaled by the lane count from osm)
CREATE TABLE shane_ch1_roads.conflation_road_case AS 
WITH ranked_roads AS (
	SELECT 
	arnold.route_id AS arnold_route_id, 
	arnold.shape AS arnold_shape,
	osm.osm_id AS osm_id, 
	osm.lanes AS lanes, 
	osm.osm_road_name AS osm_road_name,  
	osm.geom AS osm_geom,
	ST_LineSubstring( arnold.geom, LEAST(ST_LineLocatePoint(arnold.geom, ST_ClosestPoint(st_startpoint(osm.geom), arnold.geom)), 
		ST_LineLocatePoint(arnold.geom, ST_ClosestPoint(st_endpoint(osm.geom), arnold.geom))), GREATEST(ST_LineLocatePoint(arnold.geom,
		ST_ClosestPoint(st_startpoint(osm.geom), arnold.geom)) , ST_LineLocatePoint(arnold.geom, ST_ClosestPoint(st_endpoint(osm.geom), 
		arnold.geom))) ) AS seg_geom,
	ROW_NUMBER() OVER (
            PARTITION BY osm.geom
            ORDER BY 
                ST_Area(ST_Intersection(ST_Buffer(osm.geom, lanes * 4), ST_Buffer(arnold.geom, 1))) DESC
        ) AS RANK,
    ST_Intersection(ST_Buffer(osm.geom, lanes * 4 ), ST_Buffer(arnold.geom, 1)) AS intersection_geom,
    ST_Area(ST_Intersection(ST_Buffer(osm.geom, lanes * 4 ), ST_Buffer(arnold.geom, 1))) AS overlap_area
	FROM shane_ch1_roads.osm_roads_add_lanes AS osm
	RIGHT JOIN shane_ch1_roads.arnold_lines AS arnold
	ON ST_Intersects(ST_buffer(osm.geom, (osm.lanes) * 2), arnold.geom)
	WHERE (  ABS(DEGREES(ST_Angle(ST_LineSubstring( arnold.geom, LEAST(ST_LineLocatePoint(arnold.geom, 
		ST_ClosestPoint(st_startpoint(osm.geom), arnold.geom)) , ST_LineLocatePoint(arnold.geom, ST_ClosestPoint(st_endpoint(osm.geom), arnold.geom))),
		GREATEST(ST_LineLocatePoint(arnold.geom, ST_ClosestPoint(st_startpoint(osm.geom), arnold.geom)) , ST_LineLocatePoint(arnold.geom, 
		ST_ClosestPoint(st_endpoint(osm.geom), arnold.geom))) ), osm.geom))) BETWEEN 0 AND 10 -- 0 
	OR ABS(DEGREES(ST_Angle(ST_LineSubstring( arnold.geom,
		LEAST(ST_LineLocatePoint(arnold.geom, ST_ClosestPoint(st_startpoint(osm.geom), arnold.geom)) , ST_LineLocatePoint(arnold.geom, 
		ST_ClosestPoint(st_endpoint(osm.geom), arnold.geom))), GREATEST(ST_LineLocatePoint(arnold.geom, ST_ClosestPoint(st_startpoint(osm.geom),
		arnold.geom)) , ST_LineLocatePoint(arnold.geom, ST_ClosestPoint(st_endpoint(osm.geom), arnold.geom))) ), osm.geom))) BETWEEN 170 AND 190 -- 180
	OR ABS(DEGREES(ST_Angle(ST_LineSubstring( arnold.geom, LEAST(ST_LineLocatePoint(arnold.geom, ST_ClosestPoint(st_startpoint(osm.geom), arnold.geom)),
		ST_LineLocatePoint(arnold.geom, ST_ClosestPoint(st_endpoint(osm.geom), arnold.geom))), 
		GREATEST(ST_LineLocatePoint(arnold.geom, ST_ClosestPoint(st_startpoint(osm.geom), arnold.geom)), 
		ST_LineLocatePoint(arnold.geom, ST_ClosestPoint(st_endpoint(osm.geom), arnold.geom))) ), osm.geom))) BETWEEN 350 AND 360 ) -- 360
)
SELECT
	osm_id,
	osm_road_name,
	lanes,
	osm_geom,
	arnold_route_id,
	RANK,
	seg_geom AS arnold_geom,
	arnold_shape AS arnold_shape,
	intersection_geom,
	overlap_area
FROM ranked_roads
WHERE 
	ST_Length(seg_geom) > 4
AND RANK = 1
OR (
	RANK = 2
	AND EXISTS (
    	SELECT 1
    	FROM ranked_roads AS ia2
    	WHERE ia2.osm_id = ranked_roads.osm_id
     	AND ia2.rank = 1
     	AND ST_Area(ST_Intersection(ST_Buffer(ia2.seg_geom, 4), ST_Buffer(ranked_roads.seg_geom, 4))) < 100
    ) AND ST_Length(seg_geom) > 4
); -- count: 449



  
-- checkpoint --
	-- what's in? --
SELECT * FROM shane_ch1_roads.conflation_road_case
	-- what's not in? --
SELECT * FROM shane_ch1_roads.osm_roads
WHERE osm_id NOT IN (
	SELECT osm_id FROM shane_ch1_roads.conflation_road_case crc 
);

SELECT * FROM shane_ch1_roads.arnold_lines 




-- if a osm road that has defined lanes interesects with a conflated osm road and they share the same name, the non-conflated road 
-- gets conflated inheriting the conflation of the conflated road
--INSERT INTO shane_ch1_roads.conflation_road_case (
--	osm_id, 
--	osm_road_name,
--	lanes,
--	osm_geom, 
--	arnold_route_id, 
--	arnold_geom
--	arnold_shape)
--SELECT DISTINCT ON (lanes.osm_id, road_case.arnold_route_id)
--	lanes.osm_id AS osm_id, 
--	lanes.osm_road_name AS osm_road_name,
--	lanes.lanes AS lanes,
--	lanes.geom AS osm_geom,
--	road_case.arnold_route_id AS arnold_route_id,
--	road_case.arnold_geom AS arnold_geom,
--	road_case.arnold_shape AS arnold_shape
--FROM shane_ch1_roads.osm_roads_add_lanes lanes
--JOIN shane_ch1_roads.conflation_road_case road_case
--	ON lanes.osm_road_name = road_case.osm_road_name 
--AND ( ST_Intersects(st_startpoint(lanes.geom), st_startpoint(road_case.osm_geom))
--	OR ST_Intersects(st_startpoint(lanes.geom), st_endpoint(road_case.osm_geom))
--	OR ST_Intersects(st_endpoint(lanes.geom), st_startpoint(road_case.osm_geom))
--	OR ST_Intersects(st_endpoint(lanes.geom), st_endpoint(road_case.osm_geom))
--)
--WHERE lanes.osm_id NOT IN (
--	SELECT osm_id
--	FROM shane_ch1_roads.conflation_road_case
--); -- count: 15

	
-- check point -- 
SELECT * FROM shane_ch1_roads.conflation_road_case -- count: 449
