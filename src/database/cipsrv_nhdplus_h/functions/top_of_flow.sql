DO $$DECLARE 
   a VARCHAR;b VARCHAR;
BEGIN
   SELECT p.oid::regproc,pg_get_function_identity_arguments(p.oid)
   INTO a,b FROM pg_catalog.pg_proc p LEFT JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace
   WHERE p.oid::regproc::text = 'cipsrv_nhdplus_h.top_of_flow';
   IF b IS NOT NULL THEN 
   EXECUTE FORMAT('DROP FUNCTION IF EXISTS %s(%s)',a,b);ELSE
   IF a IS NOT NULL THEN EXECUTE FORMAT('DROP FUNCTION IF EXISTS %s',a);END IF;END IF;
END$$;

CREATE OR REPLACE FUNCTION cipsrv_nhdplus_h.top_of_flow(
    IN  p_nhdplusid                 BIGINT
   ,IN  p_permanent_identifier      VARCHAR
   ,IN  p_reachcode                 VARCHAR
   ,IN  p_known_region              VARCHAR
   ,IN  p_2d_flag                   BOOLEAN  DEFAULT TRUE
   ,IN  p_polygon_mask              GEOMETRY DEFAULT NULL
) RETURNS GEOMETRY
STABLE
AS
$BODY$ 
DECLARE
   rec                RECORD;
   sdo_results        GEOMETRY;
   sdo_polygon_mask   GEOMETRY := p_polygon_mask;
   sdo_flowline       GEOMETRY;
   sdo_temp           GEOMETRY;
   num_highest        NUMERIC;
   num_top_measure    NUMERIC;
   int_raster_srid    INTEGER;
   int_original_srid  INTEGER;
   boo_2d_flag        BOOLEAN := p_2d_flag;
   
BEGIN

   ----------------------------------------------------------------------------
   -- Step 10
   -- Check over incoming parameters
   ----------------------------------------------------------------------------
   IF boo_2d_flag IS NULL
   THEN
      boo_2d_flag := TRUE;
      
   END IF;
   
   ----------------------------------------------------------------------------
   -- Step 20
   -- Grab the flowline if no mask
   ----------------------------------------------------------------------------
   IF sdo_polygon_mask IS NULL
   THEN
      IF p_nhdplusid IS NOT NULL
      THEN
         SELECT
          a.shape
         ,a.tomeas AS tmeasure
         ,ST_PointN(a.shape,1)
         INTO
          sdo_flowline
         ,num_top_measure
         ,sdo_results
         FROM
         cipsrv_nhdplus_h.networknhdflowline a
         WHERE
         a.nhdplusid = p_nhdplusid;
      
      ELSIF p_permanent_identifier IS NOT NULL
      THEN
         SELECT
          a.shape
         ,a.tomeas AS tmeasure
         ,ST_PointN(a.shape,1)
         INTO
          sdo_flowline
         ,num_top_measure
         ,sdo_results
         FROM
         cipsrv_nhdplus_h.networknhdflowline a
         WHERE
         a.permanent_identifier = p_permanent_identifier;
      
      ELSIF p_reachcode IS NOT NULL
      THEN
         SELECT
          a.shape
         ,a.tomeas AS tmeasure
         ,ST_PointN(a.shape,1)
         INTO
          sdo_flowline
         ,num_top_measure
         ,sdo_results
         FROM
         cipsrv_nhdplus_h.networknhdflowline a
         WHERE
             a.reachcode = p_reachcode
         AND a.tomeas = 100;
         
      ELSE
         RAISE EXCEPTION 'nhdplusid, permanent_identifier or reachcode required';
         
      END IF;
      
      rec := cipsrv_nhdplus_h.determine_grid_srid(
          p_geometry       := sdo_results
         ,p_known_region   := p_known_region
      );
      int_raster_srid    := rec.out_srid;

      sdo_results := ST_Transform(sdo_results,int_raster_srid);
         
   END IF;
   
   ----------------------------------------------------------------------------
   -- Step 30
   -- Determine the projection if p_polygon_mask provided
   ----------------------------------------------------------------------------
   IF sdo_polygon_mask IS NOT NULL
   THEN
      int_original_srid := ST_SRID(sdo_polygon_mask);
      
      IF int_original_srid = 5070
      THEN
         int_raster_srid := 5070;
         
      ELSIF int_original_srid = 3338
      THEN
         int_raster_srid := 3338;
         
      ELSIF int_original_srid = 32161
      THEN
         int_raster_srid := 32161;
         
      ELSIF int_original_srid = 26904
      THEN
         int_raster_srid := 26904;
         
      ELSIF int_original_srid = 32655
      THEN
         int_raster_srid := 32655;
         
      ELSIF int_original_srid = 32702
      THEN
         int_raster_srid := 32702;
         
      ELSE
         rec := cipsrv_nhdplus_h.determine_grid_srid(
             p_geometry       := ST_Centroid(sdo_polygon_mask)
            ,p_known_region   := p_known_region
         );
         int_raster_srid    := rec.out_srid;

         IF rec.out_return_code <> 0
         THEN
            RAISE EXCEPTION '%: %', rec.out_return_code,rec.out_status_message;
            
         END IF;
         
         sdo_polygon_mask := ST_Transform(sdo_polygon_mask,int_raster_srid);
         
      END IF;
      
   END IF;
   
   ----------------------------------------------------------------------------
   -- Step 40
   -- Grab flowline including polygon mask
   ----------------------------------------------------------------------------
   IF sdo_polygon_mask IS NOT NULL
   THEN
      IF p_nhdplusid IS NOT NULL
      THEN
         IF int_raster_srid = 5070
         THEN 
            SELECT
            a.shape
            INTO
            sdo_flowline
            FROM
            cipsrv_nhdplus_h.nhdflowline_5070 a
            WHERE
                a.nhdplusid = p_nhdplusid
            AND ST_Intersects(a.shape,sdo_polygon_mask);
            
         ELSIF int_raster_srid = 3338
         THEN 
            SELECT
            a.shape
            INTO
            sdo_flowline
            FROM
            cipsrv_nhdplus_h.nhdflowline_3338 a
            WHERE
                a.nhdplusid = p_nhdplusid
            AND ST_Intersects(a.shape,sdo_polygon_mask);
            
         ELSIF int_raster_srid = 26904
         THEN
            SELECT
            a.shape
            INTO
            sdo_flowline
            FROM
            cipsrv_nhdplus_h.nhdflowline_26904 a
            WHERE
                a.nhdplusid = p_nhdplusid
            AND ST_Intersects(a.shape,sdo_polygon_mask);
            
         ELSIF int_raster_srid = 32161
         THEN
            SELECT
            a.shape
            INTO
            sdo_flowline
            FROM
            cipsrv_nhdplus_h.nhdflowline_32161 a
            WHERE
                a.nhdplusid = p_nhdplusid
            AND ST_Intersects(a.shape,sdo_polygon_mask);
            
         ELSIF int_raster_srid = 32655
         THEN
            SELECT
            a.shape
            INTO
            sdo_flowline
            FROM
            cipsrv_nhdplus_h.nhdflowline_32655 a
            WHERE
                a.nhdplusid = p_nhdplusid
            AND ST_Intersects(a.shape,sdo_polygon_mask);
            
         ELSIF int_raster_srid = 32702
         THEN
            SELECT
            a.shape
            INTO
            sdo_flowline
            FROM
            cipsrv_nhdplus_h.nhdflowline_32702 a
            WHERE
                a.nhdplusid = p_nhdplusid
            AND ST_Intersects(a.shape,sdo_polygon_mask);
            
         ELSE
             RAISE EXCEPTION 'err';
             
         END IF;
         
      ELSIF p_permanent_identifier IS NOT NULL
      THEN
         IF int_raster_srid = 5070
         THEN 
            SELECT
            a.shape
            INTO
            sdo_flowline
            FROM
            cipsrv_nhdplus_h.nhdflowline_5070 a
            WHERE
                a.permanent_identifier = p_permanent_identifier
            AND ST_Intersects(a.shape,sdo_polygon_mask);
            
         ELSIF int_raster_srid = 3338
         THEN 
            SELECT
            a.shape
            INTO
            sdo_flowline
            FROM
            cipsrv_nhdplus_h.nhdflowline_3338 a
            WHERE
                a.permanent_identifier = p_permanent_identifier
            AND ST_Intersects(a.shape,sdo_polygon_mask);
            
         ELSIF int_raster_srid = 26904
         THEN
            SELECT
            a.shape
            INTO
            sdo_flowline
            FROM
            cipsrv_nhdplus_h.nhdflowline_26904 a
            WHERE
                a.permanent_identifier = p_permanent_identifier
            AND ST_Intersects(a.shape,sdo_polygon_mask);
            
         ELSIF int_raster_srid = 32161
         THEN
            SELECT
            a.shape
            INTO
            sdo_flowline
            FROM
            cipsrv_nhdplus_h.nhdflowline_32161 a
            WHERE
                a.permanent_identifier = p_permanent_identifier
            AND ST_Intersects(a.shape,sdo_polygon_mask);
            
         ELSIF int_raster_srid = 32655
         THEN
            SELECT
            a.shape
            INTO
            sdo_flowline
            FROM
            cipsrv_nhdplus_h.nhdflowline_32655 a
            WHERE
                a.permanent_identifier = p_permanent_identifier
            AND ST_Intersects(a.shape,sdo_polygon_mask);
            
         ELSIF int_raster_srid = 32702
         THEN
            SELECT
            a.shape
            INTO
            sdo_flowline
            FROM
            cipsrv_nhdplus_h.nhdflowline_32702 a
            WHERE
                a.permanent_identifier = p_permanent_identifier
            AND ST_Intersects(a.shape,sdo_polygon_mask);
            
         ELSE
             RAISE EXCEPTION 'err';
             
         END IF;
         
      ELSIF p_reachcode IS NOT NULL
      THEN
         IF int_raster_srid = 5070
         THEN 
            SELECT
            a.shape
            INTO
            sdo_flowline
            FROM (
               SELECT
               aa.shape
               FROM
               cipsrv_nhdplus_h.nhdflowline_5070 aa
               WHERE
                   aa.reachcode = p_reachcode
               AND ST_Intersects(aa.shape,sdo_polygon_mask)
               ORDER BY
               aa.tmeasure DESC
            ) a
            LIMIT 1;
            
         ELSIF int_raster_srid = 3338
         THEN 
            SELECT
            a.shape
            INTO
            sdo_flowline
            FROM (
               SELECT
               aa.shape
               FROM
               cipsrv_nhdplus_h.nhdflowline_3338 aa
               WHERE
                   aa.reachcode = p_reachcode
               AND ST_Intersects(aa.shape,sdo_polygon_mask)
               ORDER BY
               aa.tmeasure DESC
            ) a
            LIMIT 1;
            
         ELSIF int_raster_srid = 26904
         THEN
            SELECT
            a.shape
            INTO
            sdo_flowline
            FROM (
               SELECT
               aa.shape
               FROM
               cipsrv_nhdplus_h.nhdflowline_26904 aa
               WHERE
                   aa.reachcode = p_reachcode
               AND ST_Intersects(aa.shape,sdo_polygon_mask)
               ORDER BY
               aa.tmeasure DESC
            ) a
            LIMIT 1;
            
         ELSIF int_raster_srid = 32161
         THEN
            SELECT
            a.shape
            INTO
            sdo_flowline
            FROM (
               SELECT
               aa.shape
               FROM
               cipsrv_nhdplus_h.nhdflowline_32161 aa
               WHERE
                   aa.reachcode = p_reachcode
               AND ST_Intersects(aa.shape,sdo_polygon_mask)
               ORDER BY
               aa.tmeasure DESC
            ) a
            LIMIT 1;
            
         ELSIF int_raster_srid = 32655
         THEN
            SELECT
            a.shape
            INTO
            sdo_flowline
            FROM (
               SELECT
               aa.shape
               FROM
               cipsrv_nhdplus_h.nhdflowline_32655 aa
               WHERE
                   aa.reachcode = p_reachcode
               AND ST_Intersects(aa.shape,sdo_polygon_mask)
               ORDER BY
               aa.tmeasure DESC
            ) a
            LIMIT 1;
            
         ELSIF int_raster_srid = 32702
         THEN
            SELECT
            a.shape
            INTO
            sdo_flowline
            FROM (
               SELECT
               aa.shape
               FROM
               cipsrv_nhdplus_h.nhdflowline_32702 aa
               WHERE
                   aa.reachcode = p_reachcode
               AND ST_Intersects(aa.shape,sdo_polygon_mask)
               ORDER BY
               aa.tmeasure DESC
            ) a
            LIMIT 1;
            
         ELSE
             RAISE EXCEPTION 'err';
             
         END IF;

      ELSE
         RAISE EXCEPTION 'nhdplusid, permanent_identifier or reachcode required';
         
      END IF;
      --raise warning '%', st_astext(st_transform(sdo_flowline,4269));

      IF sdo_flowline IS NULL
      THEN
         RAISE EXCEPTION 'no flowline returned that intersects polygon mask';
         
      END IF;
            
      sdo_flowline := cipsrv_engine.lrs_intersection(
          p_geometry1 := sdo_flowline
         ,p_geometry2 := sdo_polygon_mask
      );
      
      IF sdo_flowline IS NULL
      THEN
         sdo_results := NULL;
         
      ELSIF ST_NumGeometries(sdo_flowline) = 1
      THEN
         sdo_results := ST_StartPoint(ST_GeometryN(sdo_flowline,1));
         
      ELSE
         num_highest := -1;
         
         FOR i IN 1 .. ST_NumGeometries(sdo_flowline)
         LOOP
            sdo_temp := ST_GeometryN(sdo_flowline,i);
            IF ST_M(ST_StartPoint(sdo_temp)) > num_highest
            THEN
               num_highest := ST_M(ST_StartPoint(sdo_temp));
               
            END IF;
            
         END LOOP;
         
         FOR i IN 1 .. ST_NumGeometries(sdo_flowline)
         LOOP
            sdo_temp := ST_GeometryN(sdo_flowline,i);
            IF ST_M(ST_StartPoint(sdo_temp)) = num_highest
            THEN
               sdo_results := ST_StartPoint(sdo_temp);
               EXIT;
               
            END IF;
         
         END LOOP;
         
      END IF;
      
   END IF;
   
   ----------------------------------------------------------------------------
   -- Step 50
   -- Bail if nothing found
   ----------------------------------------------------------------------------
   IF sdo_results IS NULL
   THEN
      RETURN NULL;
      
   END IF;
   
   ----------------------------------------------------------------------------
   -- Step 60
   -- Swap back cs if needed
   ----------------------------------------------------------------------------
   IF int_original_srid <> int_raster_srid
   THEN
      sdo_results := ST_Transform(sdo_results,int_original_srid);
      
   END IF;
   
   ----------------------------------------------------------------------------
   -- Step 70
   -- Return what we got
   ----------------------------------------------------------------------------
   IF boo_2d_flag
   THEN
      RETURN ST_Force2D(sdo_results);
      
   ELSE
      RETURN sdo_results;
   
   END IF;
   
END;
$BODY$
LANGUAGE plpgsql;

ALTER FUNCTION cipsrv_nhdplus_h.top_of_flow(
    BIGINT
   ,VARCHAR
   ,VARCHAR
   ,VARCHAR
   ,BOOLEAN
   ,GEOMETRY
) OWNER TO cipsrv;

GRANT EXECUTE ON FUNCTION cipsrv_nhdplus_h.top_of_flow(
    BIGINT
   ,VARCHAR
   ,VARCHAR
   ,VARCHAR
   ,BOOLEAN
   ,GEOMETRY
) TO PUBLIC;
