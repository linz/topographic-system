This approach to detecting changes between 2 of the same datasets over time is done via SQL.

This following is information to show some examples of the type of SQL used.

**Check for new rows added**

SELECT b.topo_id::text, 'airport' as table_name, b.feature_type, 'added' AS change_type
     FROM release64.airport AS b
      LEFT JOIN release62.airport AS a ON a.topo_id = b.topo_id
      WHERE a.topo_id IS NULL; 


 SELECT b.topo_id::text, 'water' as table_name, b.feature_type, 'added' AS change_type
     FROM release64.water AS b
      LEFT JOIN release62.water AS a ON a.topo_id = b.topo_id
      WHERE a.topo_id IS NULL; 

**Check for deleted rows**

SELECT a.topo_id::text, a.t50_fid, 'airport' as table_name, a.feature_type, 'removed' AS change_type
            FROM release62.airport AS a
            LEFT JOIN release64.airport AS b ON a.topo_id = b.topo_id
            WHERE b.topo_id IS NULL; 

SELECT a.topo_id::text, a.t50_fid, 'BUILDING' as table_name, a.feature_type, 'removed' AS change_type
            FROM release62.building AS a
            LEFT JOIN release64.building AS b ON a.topo_id = b.topo_id
            WHERE b.topo_id IS NULL; 

**Check changes / updates to specific fields in the table**

 SELECT a.topo_id::text, a.bridge_use, B.bridge_use,'bridge_line' as table_name, a.feature_type, 'updated' AS change_type
 FROM release62.bridge_line AS a
 JOIN release64.bridge_line AS b ON a.topo_id = b.topo_id 
 WHERE a.t50_fid IS DISTINCT FROM b.t50_fid OR a.topo_id 
 IS DISTINCT FROM b.topo_id OR a.bridge_use IS DISTINCT FROM b.bridge_use 
 OR a.bridge_use2 IS DISTINCT FROM b.bridge_use2 
 OR a.construction_type IS DISTINCT FROM b.construction_type 
 OR a.status IS DISTINCT FROM b.status 
 OR a.name IS DISTINCT FROM b.name;

**Check changes / updates just to Geometry**

 SELECT a.topo_id::text, a.t50_fid,b.t50_fid, ST_AsText(a.geometry) as geom_A, 
 ST_AsText(b.geometry) as geom_B,'airport' as table_name, 
 a.feature_type, 'updated' AS change_type            
 FROM release62.airport AS a
 JOIN release64.airport AS b ON a.topo_id = b.topo_id           
 WHERE a.geometry IS DISTINCT FROM b.geometry 
