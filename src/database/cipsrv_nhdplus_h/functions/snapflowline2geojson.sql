DO $$DECLARE 
   a VARCHAR;b VARCHAR;
BEGIN
   SELECT p.oid::regproc,pg_get_function_identity_arguments(p.oid)
   INTO a,b FROM pg_catalog.pg_proc p LEFT JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace
   WHERE p.oid::regproc::text = 'cipsrv_nhdplus_h.snapflowline2geojson';
   IF b IS NOT NULL THEN 
   EXECUTE FORMAT('DROP FUNCTION IF EXISTS %s(%s)',a,b);ELSE
   IF a IS NOT NULL THEN EXECUTE FORMAT('DROP FUNCTION IF EXISTS %s',a);END IF;END IF;
END$$;

CREATE OR REPLACE FUNCTION cipsrv_nhdplus_h.snapflowline2geojson(
    IN  p_input                        cipsrv_nhdplus_h.snapflowline
) RETURNS JSONB
IMMUTABLE
AS $BODY$ 
DECLARE
BEGIN

   RETURN JSONB_BUILD_OBJECT(
       'type'                ,'Feature'
      ,'geometry'            ,ST_AsGeoJSON(ST_Transform(p_input.shape,4326))::JSONB
      ,'properties'          ,JSONB_BUILD_OBJECT(
          'permanent_identifier'       ,p_input.permanent_identifier
         ,'fdate'                      ,p_input.fdate
         ,'resolution'                 ,p_input.resolution
         ,'gnis_id'                    ,p_input.gnis_id
         ,'gnis_name'                  ,p_input.gnis_name
         ,'lengthkm'                   ,p_input.lengthkm
         ,'reachcode'                  ,p_input.reachcode
         ,'flowdir'                    ,p_input.flowdir
         ,'wbarea_permanent_identifier',p_input.wbarea_permanent_identifier
         ,'ftype'                      ,p_input.ftype
         ,'fcode'                      ,p_input.fcode
         ,'mainpath'                   ,p_input.mainpath
         ,'innetwork'                  ,p_input.innetwork
         ,'visibilityfilter'           ,p_input.visibilityfilter
         ,'nhdplusid'                  ,p_input.nhdplusid
         ,'vpuid'                      ,p_input.vpuid
         ,'enabled'                    ,p_input.enabled
         ,'fmeasure'                   ,p_input.fmeasure
         ,'tmeasure'                   ,p_input.tmeasure
         ,'hydroseq'                   ,p_input.hydroseq
         ,'snap_measure'               ,p_input.snap_measure
         ,'snap_distancekm'            ,p_input.snap_distancekm
      )
   );

END;
$BODY$
LANGUAGE plpgsql;

ALTER FUNCTION cipsrv_nhdplus_h.snapflowline2geojson(
    cipsrv_nhdplus_h.snapflowline
) OWNER TO cipsrv;

GRANT EXECUTE ON FUNCTION cipsrv_nhdplus_h.snapflowline2geojson(
    cipsrv_nhdplus_h.snapflowline
) TO PUBLIC;
