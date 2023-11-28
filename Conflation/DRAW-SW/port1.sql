----- OSM Roads to Sidewalk -----

---- Portland 1 ----


-- creating a table of osm roads that have sidewalk info
CREATE TABLE shane_port1.osm_roads_with_sidewalk AS 
	SELECT *
	FROM planet_osm_line
	WHERE tags -> 'sidewalk' IN ('left', 'right', 'both')
	AND way && st_setsrid( st_makebox2d( st_makepoint(-13643161,5704871), st_makepoint(-13642214,5705842)), 3857); -- count: 63


-- checkpoint --
	-- what's in? --
SELECT * FROM shane_port1.osm_roads_with_sidewalk;
	-- what's NOT in? --
SELECT * FROM planet_osm_line
	WHERE way && st_setsrid( st_makebox2d( st_makepoint(-13643161,5704871), st_makepoint(-13642214,5705842)), 3857);



-- creating a lanes column --
CREATE TABLE shane_port1.osm_roads_with_sidewalk_add_lanes AS
	SELECT 
		osm_id,
		highway,
		name,
		tags,
		CAST(tags -> 'lanes' AS int) AS lanes,
		way
	FROM shane_port1.osm_roads_with_sidewalk; -- count: 63


-- checkpoint --
	-- what's in? --
SELECT * FROM shane_port1.osm_roads_with_sidewalk_add_lanes;
	-- what's NOT in? --
SELECT * FROM shane_port1.osm_roads_with_sidewalk
WHERE  osm_id NOT IN (
	SELECT osm_id FROM shane_port1.osm_roads_with_sidewalk_add_lanes
);



-- changing the road geometries into centerlines
CREATE TABLE shane_port1.osm_roads_centerlines AS
	SELECT 
		r1.osm_id,
		r1.highway,
		r1.name,
		CASE 
			WHEN r1.tags->'sidewalk'=r2.tags->'sidewalk'
			THEN r1.tags 
				|| hstore('sidewalk', 'both') 
				|| hstore('lanes', CAST(r1.lanes + r2.lanes AS TEXT))
		END AS tags,
		r1.lanes + r2.lanes AS lanes,
		ST_Makeline(ST_Startpoint(r1.way), ST_endpoint(r1.way)) AS way
	FROM shane_port1.osm_roads_with_sidewalk_add_lanes r1
	JOIN shane_port1.osm_roads_with_sidewalk_add_lanes r2
	ON ST_Intersects(ST_Startpoint(r1.way), ST_endpoint(r2.way)) 
		AND ST_Intersects(ST_endpoint(r1.way), ST_startpoint(r2.way))
	WHERE r1.osm_id < r2.osm_id
	UNION ALL
	SELECT 
		osm_id, 
		highway, 
		name, 
		tags, 
		lanes, 
		way
	FROM shane_port1.osm_roads_with_sidewalk_add_lanes
	WHERE osm_id NOT IN (
		SELECT r1.osm_id
		FROM shane_port1.osm_roads_with_sidewalk_add_lanes r1
		JOIN shane_port1.osm_roads_with_sidewalk_add_lanes r2
		ON ST_Intersects(ST_Startpoint(r1.way), ST_endpoint(r2.way)) 
		AND ST_Intersects(ST_endpoint(r1.way), ST_startpoint(r2.way))
	); -- count: 58


-- checkpoint --
	-- what's in? --
SELECT * FROM shane_port1.osm_roads_centerlines;
	-- what's NOT in? --
SELECT * FROM shane_port1.osm_roads_with_sidewalk_add_lanes
WHERE  osm_id NOT IN (
	SELECT osm_id FROM shane_port1.osm_roads_centerlines
);



-- Create the new table with the same structure as shane_port1.osm_roads_centerlines
CREATE TABLE shane_port1.osm_roads_centerlines_add_norm_lanes AS
SELECT * FROM shane_port1.osm_roads_centerlines; -- count: 58

-- Add the norm_lanes column to the new table
ALTER TABLE shane_port1.osm_roads_centerlines_add_norm_lanes
ADD norm_lanes INT4;

-- Update the norm_lanes values based on normalization logic
UPDATE shane_port1.osm_roads_centerlines_add_norm_lanes
	SET norm_lanes = subquery.updated_lanes
	FROM (
	    SELECT l1.name, l1.highway, FLOOR(AVG(l1.lanes)) AS updated_lanes
	    FROM shane_port1.osm_roads_centerlines_add_norm_lanes AS l1
	    GROUP BY l1.name, l1.highway
	) AS subquery
	WHERE (shane_port1.osm_roads_centerlines_add_norm_lanes.name = subquery.name OR shane_port1.osm_roads_centerlines_add_norm_lanes.name IS NULL) 
		AND shane_port1.osm_roads_centerlines_add_norm_lanes.highway = subquery.highway; -- count: 58


-- checkpoint --
	-- what's in? --
SELECT * FROM shane_port1.osm_roads_centerlines_add_norm_lanes;
	-- what's NOT in? --
SELECT * FROM shane_port1.osm_roads_centerlines
WHERE  osm_id NOT IN (
	SELECT osm_id FROM shane_port1.osm_roads_centerlines_add_norm_lanes
);



-- drawing the sidewalks for the roads
CREATE TABLE shane_port1.sidewalks_from_road AS
	SELECT  
		osm_id,
		CASE 
			WHEN tags->'sidewalk' IN ('left','both')
			THEN ST_OffsetCurve(way, ((LEAST(lanes, norm_lanes)*12)+6)/3.281, 'quad_segs=4 join=mitre mitre_limit=2.2')
		END AS left_sidewalk,
		CASE 
			WHEN tags->'sidewalk' IN ('right','both')
			THEN ST_OffsetCurve(way, -((LEAST(lanes,norm_lanes)*12)+6)/3.281, 'quad_segs=4 join=mitre mitre_limit=2.2')
		END AS right_sidewalk,
		tags,
	    way
	FROM shane_port1.osm_roads_centerlines_add_norm_lanes; -- count: 58

	
-- checkpoint --
	-- what's in? --
SELECT * FROM shane_port1.sidewalks_from_road;
	-- what's NOT in? --
SELECT * FROM shane_port1.osm_roads_centerlines
WHERE  osm_id NOT IN (
	SELECT osm_id FROM shane_port1.sidewalks_from_road
);



-- creating a table for all sidewalks created as distinct rows
CREATE TABLE shane_port1.sidewalk_list AS 
	SELECT 
		osm_id,  
		tags, 
		left_sidewalk AS geom,
		way
	FROM shane_port1.sidewalks_from_road
	WHERE left_sidewalk IS NOT NULL
	UNION ALL
	SELECT 
		osm_id, 
		tags, 
		right_sidewalk AS geom,
		way
	FROM shane_port1.sidewalks_from_road
	WHERE right_sidewalk IS NOT NULL; -- count: 101


-- checkpoint --
	-- what's in? --
SELECT * FROM shane_port1.sidewalk_list;
	
	
	
-- lane data for all original osm MAIN roads --
CREATE TABLE shane_port1.osm_roads AS
	SELECT DISTINCT 
		og.osm_id, 
		og.highway, 
		og.name, 
		og.tags,
	    CASE
	        WHEN og.tags ? 'lanes'
	        THEN CAST(og.tags -> 'lanes' AS int)
	        WHEN og.tags ? 'lanes' = FALSE 
	        	AND (og.tags ? 'lanes:forward' 
	        	OR og.tags ? 'lanes:backward' 
	        	OR og.tags ? 'lanes:both_ways')
	        THEN COALESCE(CAST(og.tags -> 'lanes:forward' AS int), 0) 
	        	+ COALESCE(CAST(og.tags -> 'lanes:backward' AS int), 0) 
	        	+ COALESCE(CAST(og.tags -> 'lanes:both_ways' AS int), 0)
	        WHEN og.tags ? 'lanes' = FALSE AND 
	            (og.tags ? 'lanes:forward' = FALSE AND
	             og.tags ? 'lanes:backward' = FALSE AND
	             og.tags ? 'lanes:both_ways' = FALSE)
	            THEN 2
	    END AS lanes,
	    og.way,
	    NULL::bigint AS norm_lanes
	FROM planet_osm_line AS og
	JOIN shane_port1.osm_roads_centerlines as centerlines
	    ON ST_Intersects(og.way, centerlines.way)
	      --AND (ST_Intersects(st_startpoint(og.way), centerlines.way) IS FALSE AND ST_Intersects(st_endpoint(og.way), centerlines.way) IS FALSE)
	WHERE og.osm_id NOT IN (SELECT osm_id FROM shane_port1.osm_roads_with_sidewalk) 
		AND((og.highway = 'service' 
		AND centerlines.way IS NOT NULL 
		AND ST_Intersects(st_startpoint(og.way), centerlines.way) IS FALSE 
		AND ST_Intersects(st_endpoint(og.way), centerlines.way) IS FALSE) 
		OR (og.highway NOT IN ('proposed', 'footway', 'cycleway', 'bridleway', 'path', 'steps', 'escalator', 'service', 'track'))) 
		AND og.way && st_setsrid(st_makebox2d(st_makepoint(-13643161,5704871), st_makepoint(-13642214,5705842)), 3857)
	UNION ALL
	SELECT *, NULL::bigint AS norm_lanes
	FROM shane_port1.osm_roads_centerlines; -- count: 72


-- checkpoint --
	-- what's in? --
SELECT * FROM shane_port1.osm_roads;
	-- what's NOT in? --
SELECT * FROM planet_osm_line
WHERE way && st_setsrid( st_makebox2d( st_makepoint(-13643161,5704871), st_makepoint(-13642214,5705842)), 3857)
	AND osm_id NOT IN (
	SELECT osm_id FROM shane_port1.sidewalks_from_road
);



-- update norm_lanes of original osm_roads
UPDATE shane_port1.osm_roads
	SET norm_lanes = subquery.updated_lanes
	FROM (
	    SELECT l1.name, l1.highway, FLOOR(AVG(l1.lanes)) AS updated_lanes
	    FROM shane_port1.osm_roads l1
	    GROUP BY l1.name, l1.highway
	) AS subquery
	WHERE (shane_port1.osm_roads.name = subquery.name OR shane_port1.osm_roads.name IS NULL)
		  AND shane_port1.osm_roads.highway = subquery.highway; -- count: 72
	
	
-- checkpoint --
	-- what's in? --
SELECT * FROM shane_port1.osm_roads;



-- cut segments of the sidewalk that overlaps with the original osm road geometry buffer (buffer defined by lanes and norm lanes)
CREATE TABLE shane_port1.trimmed_sidewalks AS
	SELECT 
		sw.osm_id, 
		sw.tags, 
		sw.geom,
		COALESCE(ST_Difference(sw.geom, ST_Buffer((ST_Union(ST_Intersection(sw.geom, ST_Buffer(road.way, 
			((LEAST(road.lanes,road.norm_lanes)*12)+6)/3.281 )))), 1, 'endcap=square join=round')), sw.geom) AS trimmed_sw,
		sw.way
	FROM shane_port1.sidewalk_list AS sw
	LEFT JOIN shane_port1.osm_roads AS road
	ON ST_Intersects(sw.geom, ST_Buffer(road.way, 2 ))
	GROUP BY 
		sw.osm_id,
		sw.tags,
		sw.geom,
		sw.way
	HAVING NOT ST_IsEmpty(
	    COALESCE(ST_Difference(sw.geom, ST_Buffer((ST_Union(ST_Intersection(sw.geom, ST_Buffer(road.way, 
	    	((LEAST(road.lanes,road.norm_lanes)*12)+6)/3.281 )))), 1, 'endcap=square join=round')), sw.geom));
	    

-- checkpoint --
	-- what's in? --
SELECT * FROM shane_port1.trimmed_sidewalks;



    
    
    
    
    
    
    
    
    
    
    
    
    
--- DEFINE the intersection as the point where there are at least 3 road sub-seg ---

-- First, break the road to smaller segments at every intersections
CREATE TABLE shane_port1.osm_roads_broken_by_intersection AS
	WITH intersection_points AS (
		SELECT DISTINCT 
	  		m1.osm_id, 
	  		(ST_Intersection(m1.way, m2.way)) AS geom
	  	FROM shane_port1.osm_roads AS m1
	  	JOIN shane_port1.osm_roads AS m2 
	  		ON ST_Intersects(m1.way, m2.way) 
	  		AND m1.osm_id <> m2.osm_id 
	)
	SELECT
		a.osm_id, 
		(ST_Dump(ST_Split(a.way, ST_Union(b.geom)))).geom AS way
	FROM shane_port1.osm_roads AS a
	JOIN intersection_points AS b 
		ON a.osm_id = b.osm_id
	GROUP BY
		a.osm_id,
	  	a.way; -- count: 89


-- checkpoint --
	-- what's in? --
SELECT * FROM shane_port1.osm_roads_broken_by_intersection;
	 
	 
	 
-- Intersection points
CREATE TABLE shane_port1.osm_road_intersections AS
	SELECT DISTINCT (ST_Intersection(m1.way, m2.way)) AS point
	FROM shane_port1.osm_roads AS m1
	JOIN shane_port1.osm_roads AS m2 
		ON ST_Intersects(m1.way, m2.way) 
		AND m1.osm_id <> m2.osm_id; -- count: 63
	

-- checkpoint --
	-- what's in? --
SELECT * FROM shane_port1.osm_road_intersections;








-- show all the intersections and the sidewalks
SELECT 
	NULL::geometry AS sidewalk, 
	point AS point, 
	ST_UNION(subseg.way) AS road
FROM shane_port1.osm_road_intersections AS point
JOIN shane_port1.osm_roads_broken_by_intersection AS subseg
ON ST_Intersects(subseg.way, point.point)
GROUP BY point.point
HAVING COUNT(subseg.osm_id) >= 3
UNION ALL  
SELECT 
	COALESCE(ST_Difference(sidewalk.geom, ST_Buffer((ST_Union(ST_Intersection(sidewalk.geom, ST_Buffer(road.way, 
		((LEAST(road.lanes,road.norm_lanes)*12)+8)/3.281 )))), 1, 'endcap=square join=round')), sidewalk.geom) AS sidewalk, 
	NULL::geometry AS point, 
	NULL::geometry AS road
FROM shane_port1.sidewalk_list as sidewalk
LEFT JOIN shane_port1.osm_roads AS road
ON ST_Intersects(sidewalk.geom, ST_Buffer(road.way, 2 ))
GROUP BY sidewalk.geom
HAVING NOT ST_IsEmpty(
    COALESCE(ST_Difference(sidewalk.geom, ST_Buffer((ST_Union(ST_Intersection(sidewalk.geom, ST_Buffer(road.way, 
    	((LEAST(road.lanes,road.norm_lanes)*12)+8)/3.281 )))), 1, 'endcap=square join=round')), sidewalk.geom))
UNION ALL 
SELECT 
	NULL::geometry AS sidewalk, 
	wkb_geometry AS point, 
	NULL::geometry AS road
FROM portland.curb_ramps
WHERE wkb_geometry && st_setsrid( st_makebox2d( st_makepoint(-13643161,5704871), st_makepoint(-13642214,5705842)), 3857);


-- next step: how to draw crossings?
