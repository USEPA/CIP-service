DO $$DECLARE 
   a VARCHAR;b VARCHAR;
BEGIN
   SELECT p.oid::regproc,pg_get_function_identity_arguments(p.oid)
   INTO a,b FROM pg_catalog.pg_proc p LEFT JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace
   WHERE p.oid::regproc::text = 'cipsrv_pgrest.point_catreach_index';
   IF b IS NOT NULL THEN EXECUTE FORMAT('DROP FUNCTION IF EXISTS %s(%s)',a,b);END IF;
END$$;

CREATE OR REPLACE FUNCTION cipsrv_pgrest.point_catreach_index(
   JSONB
) RETURNS JSONB
VOLATILE
AS
$BODY$ 
DECLARE
   rec                               RECORD;
   json_input                        JSONB := $1;
   sdo_point                         GEOMETRY;
   boo_return_snap_path              BOOLEAN;
   int_return_code                   INTEGER;
   str_status_message                VARCHAR;
   str_known_region                  VARCHAR;
   str_nhdplus_version               VARCHAR;
   int_srid                          INTEGER;
   int_nhdplusid                     BIGINT;
   int_hydroseq                      BIGINT;
   int_fcode                         INTEGER;
   str_istribal                      VARCHAR;
   boo_isnavigable                   BOOLEAN;
   boo_iscoastal                     BOOLEAN;
   boo_isocean                       BOOLEAN;
   num_areasqkm                      NUMERIC;
   str_permanent_identifier          VARCHAR;
   str_reachcode                     VARCHAR;
   num_fmeasure                      NUMERIC;
   num_tmeasure                      NUMERIC;
   num_lengthkm                      NUMERIC;
   num_snap_measure                  NUMERIC;
   num_snap_distancekm               NUMERIC;
   sdo_flowline                      GEOMETRY;
   sdo_snap_point                    GEOMETRY;
   sdo_snap_path                     GEOMETRY;
   jsonb_snap_point                  JSONB;
   jsonb_snap_path                   JSONB;
   
BEGIN
   
   --------------------------------------------------------------------------
   -- Step 10
   -- Check over incoming parameters
   --------------------------------------------------------------------------
   IF JSONB_PATH_EXISTS(json_input,'$.point')
   AND json_input->'point' IS NOT NULL
   AND 
   THEN
      sdo_point := cipsrv_engine.json2geometry(json_input->'point');
      
   ELSE
      RETURN JSONB_BUILD_OBJECT(
          'return_code',    -10
         ,'status_message', 'input point is required.'
      );
   
   END IF;
   
   IF sdo_point IS NULL
   THEN
      RETURN JSONB_BUILD_OBJECT(
          'return_code',    -10
         ,'status_message', 'valid input point is required.'
      );
   
   ELSIF ST_GeometryType(sdo_point) != 'ST_Point'
   THEN
      RETURN JSONB_BUILD_OBJECT(
          'return_code',    -20
         ,'status_message', 'geometry must be single point.'
      );
   
   END IF;
   
   IF JSONB_PATH_EXISTS(json_input,'$.nhdplus_version')
   AND json_input->>'nhdplus_version' IS NOT NULL
   THEN
      str_nhdplus_version := json_input->>'nhdplus_version';
      
   ELSE
      RETURN JSONB_BUILD_OBJECT(
          'return_code', -10
         ,'status_message', 'nhdplus_version required.'
      );
      
   END IF;
   
   IF JSONB_PATH_EXISTS(json_input,'$.return_snap_path')
   AND json_input->>'return_snap_path' IS NOT NULL
   THEN
      boo_return_snap_path := (json_input->>'return_snap_path')::BOOLEAN;
      
   ELSE
      boo_return_snap_path := FALSE;
      
   END IF;
   
   IF JSONB_PATH_EXISTS(json_input,'$.known_region')
   AND json_input->>'known_region' IS NOT NULL
   THEN
      str_known_region := json_input->>'known_region';
      
   END IF;
   
   --------------------------------------------------------------------------
   -- Step 20
   -- Determine proper location grid
   --------------------------------------------------------------------------
   IF str_nhdplus_version = 'nhdplus_m'
   THEN
      rec := cipsrv_nhdplus_m.determine_grid_srid(
          p_geometry       := sdo_point
         ,p_known_region   := str_known_region
      );
      int_srid           := rec.out_srid;
      int_return_code    := rec.out_return_code;
      str_status_message := rec.out_status_message;
      
   ELSIF str_nhdplus_version = 'nhdplus_h'
   THEN
      rec := cipsrv_nhdplus_h.determine_grid_srid(
          p_geometry       := sdo_point
         ,p_known_region   := str_known_region
      );
      int_srid           := rec.out_srid;
      int_return_code    := rec.out_return_code;
      str_status_message := rec.out_status_message;
      
   ELSE
      RETURN JSONB_BUILD_OBJECT(
          'return_code', -10
         ,'status_message', 'invalid nhdplus_version.'
      );
   
   END IF;
   
   IF int_return_code != 0
   THEN
      RETURN JSONB_BUILD_OBJECT(
          'return_code',    int_return_code
         ,'status_message', str_status_message
      );
   
   END IF;
   
   sdo_point := ST_Transform(sdo_point,int_srid);
   
   --------------------------------------------------------------------------
   -- Step 30
   -- Call the engine
   --------------------------------------------------------------------------
   IF str_nhdplus_version = 'nhdplus_m'
   THEN
      IF int_srid = 5070
      THEN
         SELECT 
          a.nhdplusid
         ,a.hydroseq
         ,a.fcode
         ,a.istribal
         ,a.isnavigable
         ,a.iscoastal
         ,a.isocean
         ,a.areasqkm
         INTO
          int_nhdplusid
         ,int_hydroseq
         ,int_fcode
         ,str_istribal
         ,boo_isnavigable
         ,boo_iscoastal
         ,boo_isocean
         ,num_areasqkm
         FROM 
         cipsrv_nhdplus_m.catchment_5070 a
         WHERE
         ST_Intersects(
             a.shape
            ,sdo_point
         )
         LIMIT 1;
         
      ELSIF int_srid = 3338
      THEN
         SELECT 
          a.nhdplusid
         ,a.hydroseq
         ,a.fcode
         ,a.istribal
         ,a.isnavigable
         ,a.iscoastal
         ,a.isocean
         ,a.areasqkm
         INTO
          int_nhdplusid
         ,int_hydroseq
         ,int_fcode
         ,str_istribal
         ,boo_isnavigable
         ,boo_iscoastal
         ,boo_isocean
         ,num_areasqkm
         FROM 
         cipsrv_nhdplus_m.catchment_3338 a
         WHERE
         ST_Intersects(
             a.shape
            ,sdo_point
         )
         LIMIT 1;
         
      ELSIF int_srid = 26904
      THEN
         SELECT
          a.nhdplusid
         ,a.hydroseq
         ,a.fcode
         ,a.istribal
         ,a.isnavigable
         ,a.iscoastal
         ,a.isocean
         ,a.areasqkm
         INTO
          int_nhdplusid
         ,int_hydroseq
         ,int_fcode
         ,str_istribal
         ,boo_isnavigable
         ,boo_iscoastal
         ,boo_isocean
         ,num_areasqkm
         FROM 
         cipsrv_nhdplus_m.catchment_26904 a
         WHERE
         ST_Intersects(
             a.shape
            ,sdo_point
         )
         LIMIT 1;
         
      ELSIF int_srid = 32161
      THEN
         SELECT 
          a.nhdplusid
         ,a.hydroseq
         ,a.fcode
         ,a.istribal
         ,a.isnavigable
         ,a.iscoastal
         ,a.isocean
         ,a.areasqkm
         INTO
          int_nhdplusid
         ,int_hydroseq
         ,int_fcode
         ,str_istribal
         ,boo_isnavigable
         ,boo_iscoastal
         ,boo_isocean
         ,num_areasqkm
         FROM 
         cipsrv_nhdplus_m.catchment_32161 a
         WHERE
         ST_Intersects(
             a.shape
            ,sdo_point
         )
         LIMIT 1;
         
      ELSIF int_srid = 32655
      THEN
         SELECT 
          a.nhdplusid
         ,a.hydroseq
         ,a.fcode
         ,a.istribal
         ,a.isnavigable
         ,a.iscoastal
         ,a.isocean
         ,a.areasqkm
         INTO
          int_nhdplusid
         ,int_hydroseq
         ,int_fcode
         ,str_istribal
         ,boo_isnavigable
         ,boo_iscoastal
         ,boo_isocean
         ,num_areasqkm
         FROM 
         cipsrv_nhdplus_m.catchment_32655 a
         WHERE
         ST_Intersects(
             a.shape
            ,sdo_point
         )
         LIMIT 1;
         
      ELSIF int_srid = 32702
      THEN
         SELECT 
          a.nhdplusid
         ,a.hydroseq
         ,a.fcode
         ,a.istribal
         ,a.isnavigable
         ,a.iscoastal
         ,a.isocean
         ,a.areasqkm
         INTO
          int_nhdplusid
         ,int_hydroseq
         ,int_fcode
         ,str_istribal
         ,boo_isnavigable
         ,boo_iscoastal
         ,boo_isocean
         ,num_areasqkm
         FROM 
         cipsrv_nhdplus_m.catchment_32702 a
         WHERE
         ST_Intersects(
             a.shape
            ,sdo_point
         )
         LIMIT 1;
         
      ELSE
         RAISE EXCEPTION 'err %',int_srid;
      
      END IF;
   
   ELSIF str_nhdplus_version = 'nhdplus_h'
   THEN
      IF int_srid = 5070
      THEN
         SELECT 
          a.nhdplusid
         ,a.hydroseq
         ,a.fcode
         ,a.istribal
         ,a.isnavigable
         ,a.iscoastal
         ,a.isocean
         ,a.areasqkm
         INTO
          int_nhdplusid
         ,int_hydroseq
         ,int_fcode
         ,str_istribal
         ,boo_isnavigable
         ,boo_iscoastal
         ,boo_isocean
         ,num_areasqkm
         FROM 
         cipsrv_nhdplus_h.catchment_5070 a
         WHERE
         ST_Intersects(
             a.shape
            ,sdo_point
         )
         LIMIT 1;
         
      ELSIF int_srid = 3338
      THEN
         SELECT 
          a.nhdplusid
         ,a.hydroseq
         ,a.fcode
         ,a.istribal
         ,a.isnavigable
         ,a.iscoastal
         ,a.isocean
         ,a.areasqkm
         INTO
          int_nhdplusid
         ,int_hydroseq
         ,int_fcode
         ,str_istribal
         ,boo_isnavigable
         ,boo_iscoastal
         ,boo_isocean
         ,num_areasqkm
         FROM 
         cipsrv_nhdplus_h.catchment_3338 a
         WHERE
         ST_Intersects(
             a.shape
            ,sdo_point
         )
         LIMIT 1;
         
      ELSIF int_srid = 26904
      THEN
         SELECT 
          a.nhdplusid
         ,a.hydroseq
         ,a.fcode
         ,a.istribal
         ,a.isnavigable
         ,a.iscoastal
         ,a.isocean
         ,a.areasqkm
         INTO
          int_nhdplusid
         ,int_hydroseq
         ,int_fcode
         ,str_istribal
         ,boo_isnavigable
         ,boo_iscoastal
         ,boo_isocean
         ,num_areasqkm
         FROM 
         cipsrv_nhdplus_h.catchment_26904 a
         WHERE
         ST_Intersects(
             a.shape
            ,sdo_point
         )
         LIMIT 1;
         
      ELSIF int_srid = 32161
      THEN
         SELECT 
          a.nhdplusid
         ,a.hydroseq
         ,a.fcode
         ,a.istribal
         ,a.isnavigable
         ,a.iscoastal
         ,a.isocean
         ,a.areasqkm
         INTO
          int_nhdplusid
         ,int_hydroseq
         ,int_fcode
         ,str_istribal
         ,boo_isnavigable
         ,boo_iscoastal
         ,boo_isocean
         ,num_areasqkm
         FROM 
         cipsrv_nhdplus_h.catchment_32161 a
         WHERE
         ST_Intersects(
             a.shape
            ,sdo_point
         )
         LIMIT 1;
         
      ELSIF int_srid = 32655
      THEN
         SELECT 
          a.nhdplusid
         ,a.hydroseq
         ,a.fcode
         ,a.istribal
         ,a.isnavigable
         ,a.iscoastal
         ,a.isocean
         ,a.areasqkm
         INTO
          int_nhdplusid
         ,int_hydroseq
         ,int_fcode
         ,str_istribal
         ,boo_isnavigable
         ,boo_iscoastal
         ,boo_isocean
         ,num_areasqkm
         FROM 
         cipsrv_nhdplus_h.catchment_32655 a
         WHERE
         ST_Intersects(
             a.shape
            ,sdo_point
         )
         LIMIT 1;
         
      ELSIF int_srid = 32702
      THEN
         SELECT 
          a.nhdplusid
         ,a.hydroseq
         ,a.fcode
         ,a.istribal
         ,a.isnavigable
         ,a.iscoastal
         ,a.isocean
         ,a.areasqkm
         INTO
          int_nhdplusid
         ,int_hydroseq
         ,int_fcode
         ,str_istribal
         ,boo_isnavigable
         ,boo_iscoastal
         ,boo_isocean
         ,num_areasqkm
         FROM 
         cipsrv_nhdplus_h.catchment_32702 a
         WHERE
         ST_Intersects(
             a.shape
            ,sdo_point
         )
         LIMIT 1;
         
      ELSE
         RAISE EXCEPTION 'err %',int_srid;
      
      END IF;
      
   END IF;
   
   --------------------------------------------------------------------------
   -- Step 40
   -- Search for the flowline reach measure
   --------------------------------------------------------------------------
   IF boo_isnavigable 
   OR boo_iscoastal
   THEN
      IF str_nhdplus_version = 'nhdplus_m'
      THEN
         SELECT
          a.permanent_identifier
         ,a.reachcode
         ,a.fmeasure
         ,a.tmeasure
         ,a.lengthkm
         ,ST_Transform(a.shape,int_srid)
         INTO
          str_permanent_identifier
         ,str_reachcode
         ,num_fmeasure
         ,num_tmeasure
         ,num_lengthkm
         ,sdo_flowline
         FROM
         cipsrv_nhdplus_m.nhdflowline a
         WHERE
         a.nhdplusid = int_nhdplusid;
         
      ELSIF str_nhdplus_version = 'nhdplus_h'
      THEN
         SELECT
          a.permanent_identifier
         ,a.reachcode
         ,a.fmeasure
         ,a.tmeasure
         ,a.lengthkm
         ,ST_Transform(a.shape,int_srid)
         INTO
          str_permanent_identifier
         ,str_reachcode
         ,num_fmeasure
         ,num_tmeasure
         ,num_lengthkm
         ,sdo_flowline
         FROM
         cipsrv_nhdplus_h.nhdflowline a
         WHERE
         a.nhdplusid = int_nhdplusid;
         
      ELSE
         RAISE EXCEPTION 'err %',str_nhdplus_version;
   
      END IF;
      
      sdo_flowline := ST_Transform(sdo_flowline,int_srid);
      
      num_snap_measure := ROUND(
          ST_InterpolatePoint(
             sdo_flowline
            ,sdo_point
          )::NUMERIC
         ,5
      );
      
      IF num_snap_measure IS NOT NULL
      THEN
         sdo_snap_point := ST_Force2D(
            ST_GeometryN(
                ST_LocateAlong(
                  sdo_flowline
                 ,num_snap_measure
                )
               ,1
            )
         );
         
         num_snap_distancekm := ST_Distance(
             ST_Transform(sdo_point,4326)::GEOGRAPHY
            ,ST_Transform(sdo_snap_point,4326)::GEOGRAPHY
         ) / 1000;
         
         IF boo_return_snap_path
         AND num_snap_distancekm > 0.00005
         THEN
            sdo_snap_path := ST_MakeLine(
                sdo_point
               ,sdo_snap_point
            );
      
         END IF;
         
      END IF;
   
   END IF;
   
   --------------------------------------------------------------------------
   -- Step 50
   -- Return what we got
   --------------------------------------------------------------------------
   IF sdo_snap_point IS NOT NULL
   THEN
      jsonb_snap_point := JSONB_BUILD_OBJECT(
          'type'      ,'Feature'
         ,'geometry'  ,ST_AsGeoJSON(ST_Transform(sdo_snap_point,4326))::JSONB
         ,'obj_type'  ,'snap_point_properties'
         ,'properties',JSONB_BUILD_OBJECT(
             'nhdplusid'  ,int_nhdplusid
            ,'reachcode'  ,str_reachcode
            ,'measure'    ,num_snap_measure
         )
      );
      
   END IF;
   
   IF sdo_snap_path IS NOT NULL
   THEN
      jsonb_snap_path := JSONB_BUILD_OBJECT(
          'type'      ,'Feature'
         ,'geometry'  ,ST_AsGeoJSON(ST_Transform(sdo_snap_path,4326))::JSONB
         ,'obj_type'  ,'snap_path_properties'
         ,'properties',JSONB_BUILD_OBJECT(
            'lengthkm', num_snap_distancekm
         )
      );
      
   END IF;
   
   RETURN JSONB_BUILD_OBJECT(
       'nhdplusid'           ,int_nhdplusid
      ,'hydroseq'            ,int_hydroseq
      ,'permanent_identifier',str_permanent_identifier
      ,'reachcode'           ,str_reachcode
      ,'fcode'               ,int_fcode
      ,'isnavigable'         ,boo_isnavigable
      ,'snap_measure'        ,num_snap_measure
      ,'snap_distancekm'     ,num_snap_distancekm
      ,'snap_point'          ,jsonb_snap_point
      ,'snap_path'           ,jsonb_snap_path
      ,'return_code'         ,int_return_code
      ,'status_message'      ,str_status_message
   );

END;
$BODY$
LANGUAGE plpgsql;

ALTER FUNCTION cipsrv_pgrest.point_catreach_index(
   JSONB
) OWNER TO cipsrv_pgrest;

GRANT EXECUTE ON FUNCTION cipsrv_pgrest.point_catreach_index(
   JSONB
) TO PUBLIC;

