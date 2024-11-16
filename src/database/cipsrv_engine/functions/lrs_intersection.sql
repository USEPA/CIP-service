DO $$DECLARE 
   a VARCHAR;b VARCHAR;
BEGIN
   SELECT p.oid::regproc,pg_get_function_identity_arguments(p.oid)
   INTO a,b FROM pg_catalog.pg_proc p LEFT JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace
   WHERE p.oid::regproc::text = 'cipsrv_engine.lrs_intersection';
   IF b IS NOT NULL THEN 
   EXECUTE FORMAT('DROP FUNCTION IF EXISTS %s(%s)',a,b);ELSE
   IF a IS NOT NULL THEN EXECUTE FORMAT('DROP FUNCTION IF EXISTS %s',a);END IF;END IF;
END$$;

CREATE OR REPLACE FUNCTION cipsrv_engine.lrs_intersection(
    IN  p_geometry1          GEOMETRY
   ,IN  p_geometry2          GEOMETRY
) RETURNS GEOMETRY 
AS
$BODY$ 
DECLARE
   sdo_intersection GEOMETRY;
   sdo_initial      GEOMETRY;
   sdo_newinter     GEOMETRY;
   
BEGIN

   ----------------------------------------------------------------------------
   -- Step 10
   -- Check over incoming parameters
   ----------------------------------------------------------------------------
   IF ST_GeometryType(p_geometry1) <> 'ST_LineString'
   OR ST_M(ST_StartPoint(p_geometry1)) IS NULL
   THEN
      RAISE EXCEPTION 'geometry 1 must be single LRS linestring';
      
   END IF;
   
   IF ST_GeometryType(p_geometry2) NOT IN ('ST_Polygon','ST_MultiPolygon')
   THEN
      RAISE EXCEPTION 'geometry 2 must be a polygon or multipolygon';
      
   END IF;
   
   ----------------------------------------------------------------------------
   -- Step 20
   -- Do the intersection
   ----------------------------------------------------------------------------
   sdo_intersection := ST_Intersection(
       p_geometry1
      ,p_geometry2
   );
   
   ----------------------------------------------------------------------------
   -- Step 30
   -- Now see what we got
   ----------------------------------------------------------------------------
   IF ST_GeometryType(sdo_intersection) IS NULL
   THEN
      RETURN NULL;
      
   ELSIF ST_GeometryType(sdo_intersection) = 'ST_MultiPoint'
   THEN
      RETURN NULL;
      
   ELSIF ST_GeometryType(sdo_intersection) IN (
       'ST_LineString'
      ,'ST_GeometryCollection'
      ,'ST_MultiLineString'
   )
   THEN
      NULL;  -- Do nothing
      
   ELSE
      RAISE EXCEPTION 
          'intersection returned component gtype %'
         , ST_GeometryType(sdo_intersection);
   
   END IF;
   
   ----------------------------------------------------------------------------
   -- Step 40
   -- Pick out the linestrings
   ----------------------------------------------------------------------------
   FOR i IN 1 .. ST_NumGeometries(sdo_intersection)
   LOOP
      sdo_initial := ST_GeometryN(sdo_intersection,i);
      
      IF ST_GeometryType(sdo_initial) = 'ST_LineString'
      THEN
         sdo_initial := dz_lrs.overlay_measures(
             p_geometry1 := sdo_initial
            ,p_geometry2 := p_geometry1
         );

         IF sdo_newinter IS NULL
         THEN
            sdo_newinter := sdo_initial;
            
         ELSE
            sdo_newinter := dz_lrs.safe_concatenate_geom_segments(
                sdo_newinter
               ,sdo_initial
            );
            
         END IF;
         
      END IF;
   
   END LOOP;
   
   --------------------------------------------------------------------------
   -- Step 50
   -- Final check and then return the results
   --------------------------------------------------------------------------
   IF ST_GeometryType(sdo_newinter) NOT IN ('ST_LineString','ST_MultiLineString')
   THEN
      RAISE EXCEPTION 'unable to process geometry';
      
   END IF;

   --------------------------------------------------------------------------
   -- Step 60
   -- Return what we got
   --------------------------------------------------------------------------
   RETURN sdo_newinter;
   
END;
$BODY$
LANGUAGE plpgsql;

ALTER FUNCTION cipsrv_engine.lrs_intersection(
    GEOMETRY
   ,GEOMETRY
) OWNER TO cipsrv;

GRANT EXECUTE ON FUNCTION cipsrv_engine.lrs_intersection(
    GEOMETRY
   ,GEOMETRY
) TO PUBLIC;
