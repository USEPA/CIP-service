DO $$DECLARE 
   a VARCHAR;b VARCHAR;
BEGIN
   SELECT p.oid::regproc,pg_get_function_identity_arguments(p.oid)
   INTO a,b FROM pg_catalog.pg_proc p LEFT JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace
   WHERE p.oid::regproc::text = 'cipsrv_nhdplus_m.load_topo_catchment';
   IF b IS NOT NULL THEN 
   EXECUTE FORMAT('DROP FUNCTION IF EXISTS %s(%s)',a,b);ELSE
   IF a IS NOT NULL THEN EXECUTE FORMAT('DROP FUNCTION IF EXISTS %s',a);END IF;END IF;
END$$;

CREATE OR REPLACE FUNCTION cipsrv_nhdplus_m.load_topo_catchment(
    IN  p_grid_srid               INTEGER
   ,IN  p_catchment_count         INTEGER DEFAULT NULL
   ,OUT out_total_areasqkm        NUMERIC
   ,OUT out_geometry              GEOMETRY
   ,OUT out_return_code           INTEGER
   ,OUT out_status_message        VARCHAR
)
VOLATILE
AS $BODY$ 
DECLARE
   rec               RECORD;
   ary_edges         INTEGER[];
   ary_rings         INTEGER[];
   int_edge_count    INTEGER;
   sdo_ring          GEOMETRY;
   int_sanity        INTEGER;
   ary_polygons      GEOMETRY[];
   ary_holes         GEOMETRY[];
   boo_running       BOOLEAN;
   
BEGIN

   ----------------------------------------------------------------------------
   -- Step 10
   -- Create temporary edge table
   ----------------------------------------------------------------------------
   IF cipsrv_engine.temp_table_exists('tmp_delineation_edges')
   THEN
      TRUNCATE TABLE tmp_delineation_edges;
      
   ELSE
      CREATE TEMPORARY TABLE tmp_delineation_edges(
          edge_id        INTEGER 
         ,interior_side  VARCHAR(1)
         ,start_node_id  INTEGER
         ,end_node_id    INTEGER
         ,shape          GEOMETRY 
         ,touch_count    INTEGER
      );

      CREATE UNIQUE INDEX tmp_delineation_edges_pk
      ON tmp_delineation_edges(edge_id);
      
      CREATE INDEX tmp_delineation_edges_01i
      ON tmp_delineation_edges(start_node_id);
            
      CREATE INDEX tmp_delineation_edges_02i
      ON tmp_delineation_edges(end_node_id);

   END IF;
   
   ----------------------------------------------------------------------------
   -- Step 20
   -- Create temporary ring table
   ----------------------------------------------------------------------------
   IF cipsrv_engine.temp_table_exists('tmp_delineation_rings')
   THEN
      TRUNCATE TABLE tmp_delineation_rings;
      
   ELSE
      CREATE TEMPORARY TABLE tmp_delineation_rings(
          ring_id        INTEGER
         ,ring_type      VARCHAR(1)
         ,shape          GEOMETRY
      );

      CREATE UNIQUE INDEX tmp_delineation_rings_pk
      ON tmp_delineation_rings(ring_id);

   END IF;
   
   ----------------------------------------------------------------------------
   -- Step 30
   -- Create temporary ring table
   ----------------------------------------------------------------------------
   IF cipsrv_engine.temp_table_exists('tmp_delineation_faces')
   THEN
      TRUNCATE TABLE tmp_delineation_faces;
      
   ELSE
      CREATE TEMPORARY TABLE tmp_delineation_faces(
          face_id        INTEGER
      );

      CREATE UNIQUE INDEX tmp_delineation_faces_pk
      ON tmp_delineation_faces(face_id);

   END IF;

   ----------------------------------------------------------------------------
   -- Step 40
   -- Load the edges into temporary table
   ----------------------------------------------------------------------------   
   IF p_grid_srid = 5070
   THEN
      INSERT INTO tmp_delineation_faces(
         face_id
      )
      SELECT 
      a.element_id 
      FROM 
      cipsrv_nhdplustopo_m_catchment_fabric_5070.relation a 
      JOIN (   
         SELECT 
         aa.topo_geom 
         FROM 
         cipsrv_nhdplustopo_m.catchment_5070_topo aa 
         JOIN
         tmp_catchments bb
         ON
         aa.nhdplusid = bb.nhdplusid
      ) b 
      ON 
      a.layer_id = (b.topo_geom).layer_id AND 
      a.topogeo_id = (b.topo_geom).id 
      WHERE 
      a.element_type = 3;

      INSERT INTO tmp_delineation_edges(
          edge_id
         ,interior_side
         ,start_node_id
         ,end_node_id
         ,shape
         ,touch_count
      )
      SELECT
       a1.edge_id
      ,'L'
      ,a1.start_node
      ,a1.end_node
      ,a1.geom
      ,0
      FROM
      cipsrv_nhdplustopo_m_catchment_fabric_5070.edge_data a1 
      WHERE 
      EXISTS (SELECT 1 FROM tmp_delineation_faces f1 where f1.face_id = a1.left_face) AND 
      NOT EXISTS (SELECT 1 FROM tmp_delineation_faces f2 WHERE f2.face_id = a1.right_face) 
      UNION ALL SELECT 
       a2.edge_id
      ,'R'
      ,a2.start_node
      ,a2.end_node
      ,a2.geom
      ,0
      FROM 
      cipsrv_nhdplustopo_m_catchment_fabric_5070.edge_data a2 
      WHERE 
      NOT EXISTS (SELECT 1 FROM tmp_delineation_faces f3 where f3.face_id = a2.left_face) AND 
      EXISTS (SELECT 1 FROM tmp_delineation_faces f4 WHERE f4.face_id = a2.right_face);

   ELSIF p_grid_srid = 3338
   THEN
      INSERT INTO tmp_delineation_faces(
         face_id
      )
      SELECT 
      a.element_id 
      FROM 
      cipsrv_nhdplustopo_m_catchment_fabric_3338.relation a 
      JOIN (   
         SELECT 
         aa.topo_geom 
         FROM 
         cipsrv_nhdplustopo_m.catchment_3338_topo aa 
         JOIN
         tmp_catchments bb
         ON
         aa.nhdplusid = bb.nhdplusid
      ) b 
      ON 
      a.layer_id = (b.topo_geom).layer_id AND 
      a.topogeo_id = (b.topo_geom).id 
      WHERE 
      a.element_type = 3;

      INSERT INTO tmp_delineation_edges(
          edge_id
         ,interior_side
         ,start_node_id
         ,end_node_id
         ,shape
         ,touch_count
      )
      SELECT
       a1.edge_id
      ,'L'
      ,a1.start_node
      ,a1.end_node
      ,a1.geom
      ,0
      FROM
      cipsrv_nhdplustopo_m_catchment_fabric_3338.edge_data a1 
      WHERE 
      EXISTS (SELECT 1 FROM tmp_delineation_faces f1 where f1.face_id = a1.left_face) AND 
      NOT EXISTS (SELECT 1 FROM tmp_delineation_faces f2 WHERE f2.face_id = a1.right_face) 
      UNION ALL SELECT 
       a2.edge_id
      ,'R'
      ,a2.start_node
      ,a2.end_node
      ,a2.geom
      ,0
      FROM 
      cipsrv_nhdplustopo_m_catchment_fabric_3338.edge_data a2 
      WHERE 
      NOT EXISTS (SELECT 1 FROM tmp_delineation_faces f3 where f3.face_id = a2.left_face) AND 
      EXISTS (SELECT 1 FROM tmp_delineation_faces f4 WHERE f4.face_id = a2.right_face);

   ELSIF p_grid_srid = 26904
   THEN
      INSERT INTO tmp_delineation_faces(
         face_id
      )
      SELECT 
      a.element_id 
      FROM 
      cipsrv_nhdplustopo_m_catchment_fabric_3338.relation a 
      JOIN (   
         SELECT 
         aa.topo_geom 
         FROM 
         cipsrv_nhdplustopo_m.catchment_26904_topo aa 
         JOIN
         tmp_catchments bb
         ON
         aa.nhdplusid = bb.nhdplusid
      ) b 
      ON 
      a.layer_id = (b.topo_geom).layer_id AND 
      a.topogeo_id = (b.topo_geom).id 
      WHERE 
      a.element_type = 3;
      
      INSERT INTO tmp_delineation_edges(
          edge_id
         ,interior_side
         ,start_node_id
         ,end_node_id
         ,shape
         ,touch_count
      )
      SELECT
       a1.edge_id
      ,'L'
      ,a1.start_node
      ,a1.end_node
      ,a1.geom
      ,0
      FROM
      cipsrv_nhdplustopo_m_catchment_fabric_26904.edge_data a1 
      WHERE 
      EXISTS (SELECT 1 FROM tmp_delineation_faces f1 where f1.face_id = a1.left_face) AND 
      NOT EXISTS (SELECT 1 FROM tmp_delineation_faces f2 WHERE f2.face_id = a1.right_face) 
      UNION ALL SELECT 
       a2.edge_id
      ,'R'
      ,a2.start_node
      ,a2.end_node
      ,a2.geom
      ,0
      FROM 
      cipsrv_nhdplustopo_m_catchment_fabric_26904.edge_data a2 
      WHERE 
      NOT EXISTS (SELECT 1 FROM tmp_delineation_faces f3 where f3.face_id = a2.left_face) AND 
      EXISTS (SELECT 1 FROM tmp_delineation_faces f4 WHERE f4.face_id = a2.right_face);
      
   ELSIF p_grid_srid = 32161
   THEN
      INSERT INTO tmp_delineation_faces(
         face_id
      )
      SELECT 
      a.element_id 
      FROM 
      cipsrv_nhdplustopo_m_catchment_fabric_32161.relation a 
      JOIN (   
         SELECT 
         aa.topo_geom 
         FROM 
         cipsrv_nhdplustopo_m.catchment_32161_topo aa 
         JOIN
         tmp_catchments bb
         ON
         aa.nhdplusid = bb.nhdplusid
      ) b 
      ON 
      a.layer_id = (b.topo_geom).layer_id AND 
      a.topogeo_id = (b.topo_geom).id 
      WHERE 
      a.element_type = 3;
      
      INSERT INTO tmp_delineation_edges(
          edge_id
         ,interior_side
         ,start_node_id
         ,end_node_id
         ,shape
         ,touch_count
      )
      SELECT
       a1.edge_id
      ,'L'
      ,a1.start_node
      ,a1.end_node
      ,a1.geom
      ,0
      FROM
      cipsrv_nhdplustopo_m_catchment_fabric_32161.edge_data a1 
      WHERE 
      EXISTS (SELECT 1 FROM tmp_delineation_faces f1 where f1.face_id = a1.left_face) AND 
      NOT EXISTS (SELECT 1 FROM tmp_delineation_faces f2 WHERE f2.face_id = a1.right_face) 
      UNION ALL SELECT 
       a2.edge_id
      ,'R'
      ,a2.start_node
      ,a2.end_node
      ,a2.geom
      ,0
      FROM 
      cipsrv_nhdplustopo_m_catchment_fabric_32161.edge_data a2 
      WHERE 
      NOT EXISTS (SELECT 1 FROM tmp_delineation_faces f3 where f3.face_id = a2.left_face) AND 
      EXISTS (SELECT 1 FROM tmp_delineation_faces f4 WHERE f4.face_id = a2.right_face);
   
   ELSIF p_grid_srid = 32655
   THEN
      INSERT INTO tmp_delineation_faces(
         face_id
      )
      SELECT 
      a.element_id 
      FROM 
      cipsrv_nhdplustopo_m_catchment_fabric_32655.relation a 
      JOIN (   
         SELECT 
         aa.topo_geom 
         FROM 
         cipsrv_nhdplustopo_m.catchment_32655_topo aa 
         JOIN
         tmp_catchments bb
         ON
         aa.nhdplusid = bb.nhdplusid
      ) b 
      ON 
      a.layer_id = (b.topo_geom).layer_id AND 
      a.topogeo_id = (b.topo_geom).id 
      WHERE 
      a.element_type = 3;
      
      INSERT INTO tmp_delineation_edges(
          edge_id
         ,interior_side
         ,start_node_id
         ,end_node_id
         ,shape
         ,touch_count
      )
      SELECT
       a1.edge_id
      ,'L'
      ,a1.start_node
      ,a1.end_node
      ,a1.geom
      ,0
      FROM
      cipsrv_nhdplustopo_m_catchment_fabric_32655.edge_data a1 
      WHERE 
      EXISTS (SELECT 1 FROM tmp_delineation_faces f1 where f1.face_id = a1.left_face) AND 
      NOT EXISTS (SELECT 1 FROM tmp_delineation_faces f2 WHERE f2.face_id = a1.right_face) 
      UNION ALL SELECT 
       a2.edge_id
      ,'R'
      ,a2.start_node
      ,a2.end_node
      ,a2.geom
      ,0
      FROM 
      cipsrv_nhdplustopo_m_catchment_fabric_32655.edge_data a2 
      WHERE 
      NOT EXISTS (SELECT 1 FROM tmp_delineation_faces f3 where f3.face_id = a2.left_face) AND 
      EXISTS (SELECT 1 FROM tmp_delineation_faces f4 WHERE f4.face_id = a2.right_face);
   
   ELSIF p_grid_srid = 32702
   THEN
      INSERT INTO tmp_delineation_faces(
         face_id
      )
      SELECT 
      a.element_id 
      FROM 
      cipsrv_nhdplustopo_m_catchment_fabric_32702.relation a 
      JOIN (   
         SELECT 
         aa.topo_geom 
         FROM 
         cipsrv_nhdplustopo_m.catchment_32702_topo aa 
         JOIN
         tmp_catchments bb
         ON
         aa.nhdplusid = bb.nhdplusid
      ) b 
      ON 
      a.layer_id = (b.topo_geom).layer_id AND 
      a.topogeo_id = (b.topo_geom).id 
      WHERE 
      a.element_type = 3;
   
      INSERT INTO tmp_delineation_edges(
          edge_id
         ,interior_side
         ,start_node_id
         ,end_node_id
         ,shape
         ,touch_count
      )
      SELECT
       a1.edge_id
      ,'L'
      ,a1.start_node
      ,a1.end_node
      ,a1.geom
      ,0
      FROM
      cipsrv_nhdplustopo_m_catchment_fabric_32702.edge_data a1 
      WHERE 
      EXISTS (SELECT 1 FROM tmp_delineation_faces f1 where f1.face_id = a1.left_face) AND 
      NOT EXISTS (SELECT 1 FROM tmp_delineation_faces f2 WHERE f2.face_id = a1.right_face) 
      UNION ALL SELECT 
       a2.edge_id
      ,'R'
      ,a2.start_node
      ,a2.end_node
      ,a2.geom
      ,0
      FROM 
      cipsrv_nhdplustopo_m_catchment_fabric_32702.edge_data a2 
      WHERE 
      NOT EXISTS (SELECT 1 FROM tmp_delineation_faces f3 where f3.face_id = a2.left_face) AND 
      EXISTS (SELECT 1 FROM tmp_delineation_faces f4 WHERE f4.face_id = a2.right_face);
   
   ELSE
      RAISE EXCEPTION 'err';

   END IF;

   ----------------------------------------------------------------------------
   -- Step 50
   -- Pull out any single edge rings
   ----------------------------------------------------------------------------
   WITH insertedges AS (
      INSERT INTO tmp_delineation_rings(
          ring_id
         ,ring_type
         ,shape
      )
      SELECT
       a.edge_id
      ,CASE
       WHEN   (ST_IsPolygonCCW(ST_MakePolygon(a.shape)) AND a.interior_side = 'L')
       OR (NOT ST_IsPolygonCCW(ST_MakePolygon(a.shape)) AND a.interior_side = 'R')
       THEN
         'E'
       ELSE
         'I'
       END AS ring_type
      ,a.shape
      FROM
      tmp_delineation_edges a
      WHERE
      a.start_node_id = a.end_node_id
      RETURNING ring_id
   )
   SELECT
   array_agg(ring_id)
   INTO ary_edges
   FROM
   insertedges;
   
   DELETE FROM tmp_delineation_edges
   WHERE edge_id = ANY(ary_edges);
   
   ----------------------------------------------------------------------------
   -- Step 60
   -- Recursively pull out rings
   ----------------------------------------------------------------------------
   SELECT 
   COUNT(*)
   INTO int_edge_count
   FROM
   tmp_delineation_edges a; 
   
   int_sanity := 1;
   boo_running := TRUE;
   WHILE boo_running
   LOOP
      boo_running := nhdplus_delineation.edges2rings();
      
      int_sanity := int_sanity + 1;
      
      IF int_sanity > 1000
      THEN
         RAISE EXCEPTION 'sanity check';
         
      END IF;
      
   END LOOP;
   
   ----------------------------------------------------------------------------
   -- Step 70
   -- Organize the polygons outer and inner rings
   ----------------------------------------------------------------------------
   FOR rec IN 
      SELECT
       a.ring_id 
      ,a.shape
      FROM
      tmp_delineation_rings a
      WHERE
      a.ring_type = 'E'
      ORDER BY
      ST_Area(ST_MakePolygon(a.shape)) ASC
   LOOP
      ary_holes := NULL;
      
      SELECT
       array_agg(b.ring_id)
      ,array_agg(b.shape)
      INTO 
       ary_rings
      ,ary_holes
      FROM
      tmp_delineation_rings b
      WHERE
          b.ring_type = 'I'
      AND ST_Intersects(
          ST_MakePolygon(rec.shape)
         ,b.shape
      );
   
      IF ary_rings IS NULL
      OR array_length(ary_rings,1) = 0
      THEN
         sdo_ring := ST_MakePolygon(rec.shape);
         
      ELSE
         sdo_ring := ST_MakePolygon(rec.shape,ary_holes);
         
         DELETE FROM tmp_delineation_rings
         WHERE ring_id = ANY(ary_rings);
         
      END IF;
   
      ary_polygons := array_append(ary_polygons,sdo_ring);
   
   END LOOP;
      
   ----------------------------------------------------------------------------
   -- Step 80
   -- Generate the final results
   ----------------------------------------------------------------------------
   IF array_length(ary_polygons,1) = 1
   THEN
      out_geometry := ST_ForcePolygonCCW(ary_polygons[1]);
      
   ELSE
      out_geometry := ST_ForcePolygonCCW(ST_Collect(ary_polygons));
      
   END IF;

   ----------------------------------------------------------------------------
   -- Step 90
   -- Calculate the area
   ----------------------------------------------------------------------------
   out_total_areasqkm := ST_Area(ST_Transform(out_geometry,4326)::GEOGRAPHY) * 0.0000010;

   ----------------------------------------------------------------------------
   -- Step 100
   -- Insert results as nad83
   ----------------------------------------------------------------------------
   INSERT INTO tmp_catchments(
       nhdplusid
      ,sourcefc
      ,gridcode
      ,areasqkm
      ,shape
   ) VALUES (
       -9999999
      ,'AGGR'
      ,-9999
      ,out_total_areasqkm
      ,ST_Transform(out_geometry,4269)
   );
   
   out_return_code := 0;
   
   RETURN; 
   
END;
$BODY$
LANGUAGE plpgsql;

ALTER FUNCTION cipsrv_nhdplus_m.load_topo_catchment(
    INTEGER
   ,INTEGER
) OWNER TO cipsrv;

GRANT EXECUTE ON FUNCTION cipsrv_nhdplus_m.load_topo_catchment(
    INTEGER
   ,INTEGER
) TO PUBLIC;

