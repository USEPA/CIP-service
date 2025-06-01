DROP TABLE IF EXISTS cipsrv_nhdplustopo_m.catchment_32161_topo CASCADE;

SELECT topology.DropTopology('cipsrv_nhdplustopo_m_catchment_fabric_32161');

DROP SCHEMA IF EXISTS cipsrv_nhdplustopo_m_catchment_fabric_32161 CASCADE;

CREATE TABLE IF NOT EXISTS cipsrv_nhdplustopo_m.catchment_32161_topo(
    objectid           INTEGER    NOT NULL
   ,catchmentstatecode VARCHAR(2) NOT NULL
   ,nhdplusid          BIGINT     NOT NULL
   ,areasqkm           NUMERIC    NOT NULL
   ,state_count        INTEGER
   ,CONSTRAINT catchment_32161_topo_pk  PRIMARY KEY(catchmentstatecode,nhdplusid)
   ,CONSTRAINT catchment_32161_topo_u01 UNIQUE(objectid)
);

ALTER TABLE IF EXISTS cipsrv_nhdplustopo_m.catchment_32161_topo
OWNER to cipsrv;

GRANT SELECT ON TABLE cipsrv_nhdplustopo_m.catchment_32161_topo TO PUBLIC;

CREATE INDEX IF NOT EXISTS catchment_32161_topo_i01
ON cipsrv_nhdplustopo_m.catchment_32161_topo(catchmentstatecode);

CREATE INDEX IF NOT EXISTS catchment_32161_topo_i02
ON cipsrv_nhdplustopo_m.catchment_32161_topo(nhdplusid);

SELECT topology.CreateTopology(
    toponame := 'cipsrv_nhdplustopo_m_catchment_fabric_32161'
   ,srid     := 32161
   ,prec     := 0.001
);

SELECT topology.AddTopoGeometryColumn(
    'cipsrv_nhdplustopo_m_catchment_fabric_32161'
   ,'cipsrv_nhdplustopo_m'
   ,'catchment_32161_topo'
   ,'topo_geom'
   ,'POLYGON'
);

GRANT USAGE ON SCHEMA cipsrv_nhdplustopo_m_catchment_fabric_32161           TO PUBLIC;
GRANT SELECT ON TABLE cipsrv_nhdplustopo_m_catchment_fabric_32161.edge_data TO PUBLIC;
GRANT SELECT ON TABLE cipsrv_nhdplustopo_m_catchment_fabric_32161.face      TO PUBLIC;
GRANT SELECT ON TABLE cipsrv_nhdplustopo_m_catchment_fabric_32161.relation  TO PUBLIC;
GRANT SELECT ON TABLE cipsrv_nhdplustopo_m_catchment_fabric_32161.node      TO PUBLIC;

CREATE OR REPLACE PROCEDURE cipsrv_nhdplustopo_m.unravel_topology_32161(
    IN  p_bad_edge_id      INTEGER
   ,OUT out_avoid_objectid INTEGER[]
)
LANGUAGE 'plpgsql'
AS $BODY$
DECLARE
   int_left_face      INTEGER;
   int_right_face     INTEGER;
   int_topogeo_id     INTEGER;
   int_objectid       INTEGER;
   
   ary_topogeo_id     INTEGER[];
   out_avoid_objectid INTEGER[];
   
BEGIN

   SELECT
    a.left_face
   ,a.right_face
   INTO
    int_left_face
   ,int_right_face
   FROM
   cipsrv_nhdplustopo_m_catchment_fabric_32161.edge_data a
   WHERE
   a.edge_id = p_bad_edge_id;
   
   IF int_left_face IS NULL
   THEN
      RAISE WARNING 'edge not found';
      RETURN;
      
   END IF;
   
   IF int_left_face > 0
   THEN
      SELECT
      a.topogeo_id
      INTO 
      int_topogeo_id
      FROM
      cipsrv_nhdplustopo_m_catchment_fabric_32161.relation a
      WHERE
      a.element_id = int_left_face;
      
      SELECT
      a.objectid
      INTO 
      int_objectid
      FROM
      cipsrv_nhdplustopo_m.catchment_32161_topo a
      WHERE
      (a.topo_geom).id = int_topogeo_id;

      IF int_topogeo_id IS NOT NULL
      THEN
         ary_topogeo_id := array_append(ary_topogeo_id,int_topogeo_id);
      
      END IF;
      
      IF int_objectid IS NOT NULL
      THEN
         out_avoid_objectid   := array_append(out_avoid_objectid,int_objectid);    
         
      END IF;
      
   END IF;
   
   IF int_right_face > 0
   THEN
      SELECT
      a.topogeo_id
      INTO 
      int_topogeo_id
      FROM
      cipsrv_nhdplustopo_m_catchment_fabric_32161.relation a
      WHERE
      a.element_id = int_right_face;
      
      SELECT
      a.objectid
      INTO 
      int_objectid
      FROM
      cipsrv_nhdplustopo_m.catchment_32161_topo a
      WHERE
      (a.topo_geom).id = int_topogeo_id;

      IF int_topogeo_id IS NOT NULL
      THEN
         ary_topogeo_id := array_append(ary_topogeo_id,int_topogeo_id);
      
      END IF;
      
      IF int_objectid IS NOT NULL
      THEN
         out_avoid_objectid   := array_append(out_avoid_objectid,int_objectid);    
         
      END IF;
      
   END IF;

   FOR i IN 1 .. array_length(ary_topogeo_id,1)
   LOOP
      
      RAISE WARNING ' deleting objectid %',int_objectid;
      DELETE FROM cipsrv_nhdplustopo_m.catchment_32161_topo a
      WHERE a.objectid = out_avoid_objectid[i];
    
      RAISE WARNING ' deleting relation %',int_topogeo_id;
      DELETE FROM cipsrv_nhdplustopo_m_catchment_fabric_32161.relation a
      WHERE a.topogeo_id = ary_topogeo_id[i];
      
   END LOOP;

   RAISE WARNING ' deleting edge %',p_bad_edge_id;
   PERFORM ST_RemEdgeNewFace('cipsrv_nhdplustopo_m_catchment_fabric_32161',p_bad_edge_id);
   COMMIT;
   
END 
$BODY$;

CREATE OR REPLACE PROCEDURE cipsrv_nhdplustopo_m.load_32161(
    p_state   VARCHAR
   ,p_chunk   INTEGER  DEFAULT 10000
   ,p_commit  INTEGER  DEFAULT 1000
   ,p_analyze INTEGER  DEFAULT 5000
   ,p_focus   GEOMETRY DEFAULT NULL
)
LANGUAGE 'plpgsql'
AS $BODY$
DECLARE
   rec              RECORD;
   rec2             RECORD;
   int_cmt          INTEGER;
   int_anl          INTEGER;
   bad_edge         INTEGER;
   int_chunk        INTEGER;
   tmp_objs         INTEGER[];
   avoid_objs       INTEGER[];  
   boo_check        BOOLEAN;
   int_cnt          INTEGER;
   
BEGIN

   int_chunk := p_chunk;
   int_cmt := 0;
   int_anl := 0;
   
   <<outer>>
   FOR i IN 1 .. 100000
   LOOP
      boo_check := FALSE;
      
      <<inner>>
      FOR rec IN 
      SELECT
      DISTINCT a.objectid
      FROM
      cipsrv_epageofab_m.catchment_fabric_32161_3 a
      JOIN
      cipsrv_nhdplustopo_m_catchment_fabric_32161.edge_data b
      ON ST_INTERSECTS(a.shape,b.geom)
      WHERE
          a.catchmentstatecode = p_state
      AND NOT EXISTS (SELECT 1 FROM cipsrv_nhdplustopo_m.catchment_32161_topo c WHERE c.objectid = a.objectid)
      AND ( b.left_face != 0 OR b.right_face != 0  ) 
      AND a.objectid != ALL(COALESCE(avoid_objs,ARRAY[]::INT[]))
      AND (
         p_focus IS NULL
         OR
         ST_INTERSECTS(
             a.shape
            ,ST_TRANSFORM(p_focus,32161)
         )
      )
      LIMIT int_chunk
      LOOP
         boo_check := TRUE;
         
         BEGIN
            INSERT INTO cipsrv_nhdplustopo_m.catchment_32161_topo(
               objectid, catchmentstatecode, nhdplusid, areasqkm, state_count, topo_geom)
            SELECT
             aa.objectid
            ,aa.catchmentstatecode
            ,aa.nhdplusid
            ,aa.areasqkm
            ,aa.state_count
            ,topology.toTopoGeom(
                aa.shape
               ,'cipsrv_nhdplustopo_m_catchment_fabric_32161'
               ,1
               ,0.001
             )
            FROM
            cipsrv_epageofab_m.catchment_fabric_32161_3 aa
            WHERE 
            aa.objectid = rec.objectid;
            
            int_cmt := int_cmt + 1;
            int_anl := int_anl + 1;
            
         EXCEPTION
            WHEN OTHERS 
            THEN
               ROLLBACK;
               
               IF SQLERRM = 'index returned tuples in wrong order'
               THEN
                  RAISE WARNING '   bouncing on tuple ordering for %',rec.objectid;
                  
                  BEGIN
                     INSERT INTO cipsrv_nhdplustopo_m.catchment_32161_topo(
                        objectid, catchmentstatecode, nhdplusid, areasqkm, state_count, topo_geom)
                     SELECT
                      aa.objectid
                     ,aa.catchmentstatecode
                     ,aa.nhdplusid
                     ,aa.areasqkm
                     ,aa.state_count
                     ,topology.toTopoGeom(
                         ST_UNION(aa.shape,aa.shape)
                        ,'cipsrv_nhdplustopo_m_catchment_fabric_32161'
                        ,1
                        ,0.001
                      )
                     FROM
                     cipsrv_epageofab_m.catchment_fabric_32161_3 aa
                     WHERE
                     aa.objectid = rec.objectid;
                     
                     int_cmt := int_cmt + 1;
                     int_anl := int_anl + 1;
                  
                  EXCEPTION
                     WHEN OTHERS 
                     THEN
                        ROLLBACK;
                        
                        RAISE WARNING '   unable to bounce, will skip %',rec.objectid;
                        avoid_objs := ARRAY_APPEND(avoid_objs,rec.objectid);
                        
                        int_cmt := int_cmt + 1;
                        int_anl := int_anl + 1;
                     
                  END;
                  
               ELSIF SQLERRM = 'Second line start point too far from first line end point'
               THEN
                  RAISE WARNING '   point distance issue with objectid %, will avoid', rec.objectid;
                  avoid_objs := ARRAY_APPEND(avoid_objs,rec.objectid);
                  
                  int_cmt := int_cmt + 1;
                  int_anl := int_anl + 1;
                  
               ELSE
                  RAISE WARNING '%',SQLERRM;
                  bad_edge := (SPLIT_PART(SQLERRM,' ',-1))::INTEGER;
                  RAISE WARNING 'bad edge: % for objectid %',bad_edge,rec.objectid;
                  CALL cipsrv_epageofab_m.unravel_topology(bad_edge,tmp_objs);
                  avoid_objs := ARRAY_CAT(avoid_objs,tmp_objs);
                  avoid_objs := ARRAY_APPEND(avoid_objs,rec.objectid);
               
               END IF;
               
         END;
         
         IF int_anl >= p_analyze
         THEN
            COMMIT;
            RAISE WARNING '   committing and analyzing %', int_anl;
            EXECUTE 'ANALYZE cipsrv_nhdplustopo_m_catchment_fabric_32161.edge_data';
            EXECUTE 'ANALYZE cipsrv_nhdplustopo_m_catchment_fabric_32161.face';
            int_cmt := 0;
            int_anl := 0;
            
         ELSE         
            IF int_cmt >= p_commit
            THEN
               COMMIT;
               RAISE WARNING '   commiting %',int_cmt;
               int_cmt := 0;
               
            END IF;
            
         END IF;        
         
      END LOOP;
       
      IF NOT boo_check
      THEN
         INSERT INTO cipsrv_nhdplustopo_m.catchment_32161_topo(
            objectid, catchmentstatecode, nhdplusid, areasqkm, state_count, topo_geom)
         SELECT
          aa.objectid
         ,aa.catchmentstatecode
         ,aa.nhdplusid
         ,aa.areasqkm
         ,aa.state_count
         ,topology.toTopoGeom(
             aa.shape
            ,'cipsrv_nhdplustopo_m_catchment_fabric_32161'
            ,1
            ,0.001
          )
         FROM
         cipsrv_epageofab_m.catchment_fabric_32161_3 aa
         WHERE
             aa.catchmentstatecode = p_state
         AND NOT EXISTS (SELECT 1 FROM cipsrv_nhdplustopo_m.catchment_32161_topo cc WHERE cc.objectid = aa.objectid)
         AND aa.objectid != ALL(COALESCE(avoid_objs,ARRAY[]::INT[]))
         AND (
            p_focus IS NULL
            OR
            ST_INTERSECTS(
                aa.shape
               ,ST_TRANSFORM(p_focus,32161)
            )
         )
         LIMIT 1;
         
         GET DIAGNOSTICS int_cnt = ROW_COUNT;
         RAISE WARNING '   adding % new seed',int_cnt;
         
         IF int_cnt = 0
         THEN
            RAISE WARNING '   % seems done',p_state;
            EXIT outer;
            
         END IF;
         
      END IF;
      
      COMMIT;

   END LOOP;

   COMMIT;   
   
END 
$BODY$;

DO $$DECLARE   
BEGIN
   CALL cipsrv_nhdplustopo_m.load_32161('PR',p_commit := 100);
   CALL cipsrv_nhdplustopo_m.load_32161('VI',p_commit := 100);
   
END$$;

