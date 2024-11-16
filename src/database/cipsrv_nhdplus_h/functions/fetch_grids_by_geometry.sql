DO $$DECLARE 
   a VARCHAR;b VARCHAR;
BEGIN
   SELECT p.oid::regproc,pg_get_function_identity_arguments(p.oid)
   INTO a,b FROM pg_catalog.pg_proc p LEFT JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace
   WHERE p.oid::regproc::text = 'cipsrv_nhdplus_h.fetch_grids_by_geometry';
   IF b IS NOT NULL THEN 
   EXECUTE FORMAT('DROP FUNCTION IF EXISTS %s(%s)',a,b);ELSE
   IF a IS NOT NULL THEN EXECUTE FORMAT('DROP FUNCTION IF EXISTS %s',a);END IF;END IF;
END$$;

CREATE OR REPLACE FUNCTION cipsrv_nhdplus_h.fetch_grids_by_geometry(
    IN  p_FDR_input                 GEOMETRY
   ,IN  p_FAC_input                 GEOMETRY
   ,IN  p_known_region              VARCHAR DEFAULT NULL
   ,IN  p_FDR_nodata                NUMERIC DEFAULT NULL
   ,IN  p_FAC_nodata                NUMERIC DEFAULT NULL
   ,IN  p_crop                      BOOLEAN DEFAULT TRUE
   ,OUT out_FDR                     RASTER
   ,OUT out_FAC                     RASTER
   ,OUT out_return_code             INTEGER
   ,OUT out_status_message          VARCHAR
)
STABLE
AS
$BODY$ 
DECLARE
   rec                RECORD;
   int_raster_srid    INTEGER;
   int_original_srid  INTEGER;
   sdo_test_input     GEOMETRY;
   sdo_fdr_selector   GEOMETRY := p_FDR_input;
   sdo_fac_selector   GEOMETRY := p_FAC_input;
   int_fdr_nodata     INTEGER  := p_FDR_nodata;
   int_fac_nodata     INTEGER  := p_FAC_nodata;
   
BEGIN

   ----------------------------------------------------------------------------
   -- Step 10
   -- Check over incoming parameters
   ----------------------------------------------------------------------------
   IF  sdo_fdr_selector IS NULL
   AND sdo_fac_selector IS NULL
   THEN
      RAISE EXCEPTION 'input selections cannot both be null';
      
   ELSIF sdo_fdr_selector IS NULL
   THEN
      sdo_test_input := sdo_fac_selector;
      
   ELSE
      sdo_test_input := sdo_fdr_selector;
   
   END IF;
   
   IF  sdo_fdr_selector IS NOT NULL 
   AND ST_GeometryType(sdo_fdr_selector) NOT IN ('ST_Polygon','ST_MultiPolygon')
   THEN
      RAISE EXCEPTION 'fdr geometry selector must be polygon';
      
   END IF;
   
   IF  sdo_fac_selector IS NOT NULL 
   AND ST_GeometryType(sdo_fac_selector) NOT IN ('ST_Polygon','ST_MultiPolygon')
   THEN
      RAISE EXCEPTION 'fac geometry selector must be polygon';
      
   END IF;
   
   IF int_fdr_nodata IS NULL
   THEN
      int_fdr_nodata := 255;
      
   END IF;
   
   IF int_fac_nodata IS NULL
   THEN
      int_fac_nodata := 2147483647;
      
   END IF;
   
   IF int_fdr_nodata < 0
   THEN
      RAISE EXCEPTION 'fdr nodata value must be unsigned 8 bit integer';
   
   END IF;   
   
   --------------------------------------------------------------------------
   -- Step 20
   -- Determine the grid projection 
   --------------------------------------------------------------------------
   rec := cipsrv_nhdplus_h.determine_grid_srid(
       p_geometry       := p_split_point
      ,p_known_region   := p_known_region
   );
   int_raster_srid    := rec.out_srid;
   out_return_code    := rec.out_return_code;
   out_status_message := rec.out_status_message;
   
   IF out_return_code != 0
   THEN
      RETURN;
      
   END IF;
   
   --------------------------------------------------------------------------
   -- Step 30
   -- Project initial point
   --------------------------------------------------------------------------
   int_original_srid := ST_Srid(sdo_test_input);

   IF  int_original_srid <> int_raster_srid
   THEN
      IF sdo_fdr_selector IS NOT NULL
      THEN
         sdo_fdr_selector := ST_Transform(sdo_fdr_selector,int_raster_srid);
      
      END IF;
      
      IF sdo_fac_selector IS NOT NULL
      THEN
         sdo_fac_selector := ST_Transform(sdo_fac_selector,int_raster_srid);
      
      END IF;
      
   END IF;
   
   --------------------------------------------------------------------------
   -- Step 40
   -- Fetch the FDR raster if requested
   --------------------------------------------------------------------------
   IF sdo_fdr_selector IS NOT NULL
   THEN
      IF int_raster_srid = 5070
      THEN
         SELECT 
         ST_Clip(
             ST_Union(a.rast)
            ,sdo_fdr_selector
            ,p_FDR_nodata
            ,p_crop
         )
         INTO out_FDR
         FROM
         cipsrv_nhdplusgrid_m.fdr_5070_rdt a
         WHERE
         ST_INTERSECTS(a.rast,sdo_fdr_selector);

      ELSIF int_raster_srid = 3338
      THEN
         SELECT 
         ST_Clip(
             ST_Union(a.rast)
            ,sdo_fdr_selector
            ,p_FDR_nodata
            ,p_crop
         )
         INTO out_FDR
         FROM
         cipsrv_nhdplusgrid_m.fdr_3338_rdt a
         WHERE
         ST_INTERSECTS(a.rast,sdo_fdr_selector);

      ELSIF int_raster_srid = 26904
      THEN
         SELECT 
         ST_Clip(
             ST_Union(a.rast)
            ,sdo_fdr_selector
            ,p_FDR_nodata
            ,p_crop
         )
         INTO out_FDR
         FROM
         cipsrv_nhdplusgrid_m.fdr_26904_rdt a
         WHERE
         ST_INTERSECTS(a.rast,sdo_fdr_selector);

      ELSIF int_raster_srid = 32161
      THEN
         SELECT 
         ST_Clip(
             ST_Union(a.rast)
            ,sdo_fdr_selector
            ,p_FDR_nodata
            ,p_crop
         )
         INTO out_FDR
         FROM
         cipsrv_nhdplusgrid_m.fdr_32161_rdt a
         WHERE
         ST_INTERSECTS(a.rast,sdo_fdr_selector);

      ELSIF int_raster_srid = 32655
      THEN
         SELECT 
         ST_Clip(
             ST_Union(a.rast)
            ,sdo_fdr_selector
            ,p_FDR_nodata
            ,p_crop
         )
         INTO out_FDR
         FROM
         cipsrv_nhdplusgrid_m.fdr_32655_rdt a
         WHERE
         ST_INTERSECTS(a.rast,sdo_fdr_selector);

      ELSIF int_raster_srid = 32702
      THEN
         SELECT 
         ST_Clip(
             ST_Union(a.rast)
            ,sdo_fdr_selector
            ,p_FDR_nodata
            ,p_crop
         )
         INTO out_FDR
         FROM
         cipsrv_nhdplusgrid_m.fdr_32702_rdt a
         WHERE
         ST_INTERSECTS(a.rast,sdo_fdr_selector);

      ELSE
         RAISE EXCEPTION 'err';
         
      END IF;
   
   END IF;
   
   --------------------------------------------------------------------------
   -- Step 50
   -- Fetch the FAC raster if requested
   --------------------------------------------------------------------------
   IF sdo_fac_selector IS NOT NULL
   THEN
      IF int_raster_srid = 5070
      THEN
         SELECT 
         ST_Clip(
             ST_Union(a.rast)
            ,sdo_fac_selector
            ,p_FAC_nodata
            ,p_crop
         )
         INTO out_FAC
         FROM
         cipsrv_nhdplusgrid_m.fac_5070_rdt a
         WHERE
         ST_INTERSECTS(a.rast,sdo_fac_selector);

      ELSIF int_raster_srid = 3338
      THEN
         SELECT 
         ST_Clip(
             ST_Union(a.rast)
            ,sdo_fac_selector
            ,p_FAC_nodata
            ,p_crop
         )
         INTO out_FAC
         FROM
         cipsrv_nhdplusgrid_m.fac_3338_rdt a
         WHERE
         ST_INTERSECTS(a.rast,sdo_fac_selector); 

      ELSIF int_raster_srid = 26904
      THEN
         SELECT 
         ST_Clip(
             ST_Union(a.rast)
            ,sdo_fac_selector
            ,p_FAC_nodata
            ,p_crop
         )
         INTO out_FAC
         FROM
         cipsrv_nhdplusgrid_m.fac_26904_rdt a
         WHERE
         ST_INTERSECTS(a.rast,sdo_fac_selector); 

      ELSIF int_raster_srid = 32161
      THEN
         SELECT 
         ST_Clip(
             ST_Union(a.rast)
            ,sdo_fac_selector
            ,p_FAC_nodata
            ,p_crop
         )
         INTO out_FAC
         FROM
         cipsrv_nhdplusgrid_m.fac_32161_rdt a
         WHERE
         ST_INTERSECTS(a.rast,sdo_fac_selector);

      ELSIF int_raster_srid = 32655
      THEN
         SELECT 
         ST_Clip(
             ST_Union(a.rast)
            ,sdo_fac_selector
            ,p_FAC_nodata
            ,p_crop
         )
         INTO out_FAC
         FROM
         cipsrv_nhdplusgrid_m.fac_32655_rdt a
         WHERE
         ST_INTERSECTS(a.rast,sdo_fac_selector);

      ELSIF int_raster_srid = 32702
      THEN
         SELECT 
         ST_Clip(
             ST_Union(a.rast)
            ,sdo_fac_selector
            ,p_FAC_nodata
            ,p_crop
         )
         INTO out_FAC
         FROM
         cipsrv_nhdplusgrid_m.fac_32702_rdt a
         WHERE
         ST_INTERSECTS(a.rast,sdo_fac_selector);

      ELSE
         RAISE EXCEPTION 'err';
         
      END IF;
   
   END IF;
   
END;
$BODY$
LANGUAGE plpgsql;

ALTER FUNCTION cipsrv_nhdplus_h.fetch_grids_by_geometry(
    GEOMETRY
   ,GEOMETRY
   ,VARCHAR
   ,NUMERIC
   ,NUMERIC
   ,BOOLEAN
) OWNER TO cipsrv;

GRANT EXECUTE ON FUNCTION cipsrv_nhdplus_h.fetch_grids_by_geometry(
    GEOMETRY
   ,GEOMETRY
   ,VARCHAR
   ,NUMERIC
   ,NUMERIC
   ,BOOLEAN
) TO PUBLIC;
