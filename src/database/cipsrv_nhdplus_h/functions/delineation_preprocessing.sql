DO $$DECLARE 
   a VARCHAR;b VARCHAR;
BEGIN
   SELECT p.oid::regproc,pg_get_function_identity_arguments(p.oid)
   INTO a,b FROM pg_catalog.pg_proc p LEFT JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace
   WHERE p.oid::regproc::text = 'cipsrv_nhdplus_h.delineation_preprocessing';
   IF b IS NOT NULL THEN 
   EXECUTE FORMAT('DROP FUNCTION IF EXISTS %s(%s)',a,b);ELSE
   IF a IS NOT NULL THEN EXECUTE FORMAT('DROP FUNCTION IF EXISTS %s',a);END IF;END IF;
END$$;

CREATE OR REPLACE FUNCTION cipsrv_nhdplus_h.delineation_preprocessing(
    IN  p_aggregation_flag            BOOLEAN
   ,IN  p_known_grid                  INTEGER
   ,IN  p_return_delineation_geometry BOOLEAN
   ,IN  p_return_topology_results     BOOLEAN
   ,IN  p_extra_catchments            BIGINT[]
   ,OUT out_records_inserted          INTEGER
   ,OUT out_return_code               NUMERIC
   ,OUT out_status_message            VARCHAR
)
VOLATILE
AS $BODY$
DECLARE
   int_inserted INTEGER;
   
BEGIN
   
   out_return_code := 0;
   out_records_inserted := 0;
   
   ----------------------------------------------------------------------------
   -- Step 10
   -- If no aggregation requested, then just load nad83 column
   ---------------------------------------------------------------------------- 
   IF NOT p_aggregation_flag
   THEN
      INSERT INTO tmp_catchments(
          nhdplusid
         ,sourcefc
         ,gridcode
         ,areasqkm
         ,vpuid
         ,hydroseq
         ,shape
      )
      SELECT
       b.nhdplusid
      ,b.sourcefc
      ,b.gridcode
      ,b.areasqkm
      ,b.vpuid
      ,a.hydrosequence
      ,CASE WHEN p_return_delineation_geometry THEN b.shape ELSE NULL::GEOMETRY END AS shape
      FROM
      tmp_navigation_results a
      JOIN
      cipsrv_nhdplus_h.nhdpluscatchment b
      ON
      b.nhdplusid = a.nhdplusid;

      GET DIAGNOSTICS int_inserted = ROW_COUNT;
      out_records_inserted := out_records_inserted + int_inserted;
      
      IF array_length(p_extra_catchments,1) > 0
      THEN
         INSERT INTO tmp_catchments(
             nhdplusid
            ,sourcefc
            ,gridcode
            ,areasqkm
            ,vpuid
            ,hydroseq
            ,shape
         )
         SELECT
          a.nhdplusid
         ,a.sourcefc
         ,a.gridcode
         ,a.areasqkm
         ,a.vpuid
         ,0
         ,CASE WHEN p_return_delineation_geometry THEN a.shape ELSE NULL::GEOMETRY END AS shape
         FROM
         cipsrv_nhdplus_h.nhdpluscatchment a
         WHERE
         a.nhdplusid = ANY(p_extra_catchments)
         ON CONFLICT DO NOTHING;

         GET DIAGNOSTICS int_inserted = ROW_COUNT;
         out_records_inserted := out_records_inserted + int_inserted;

      END IF;
      
   -----------------------------------------------------------------------------
   -- Step 20
   -- Load the temp table based on the grid locality value
   -----------------------------------------------------------------------------
   ELSE
      IF p_known_grid = 5070
      THEN
         INSERT INTO tmp_catchments(
             nhdplusid
            ,sourcefc
            ,gridcode
            ,areasqkm
            ,vpuid
            ,hydroseq
            ,shape_5070
         )
         SELECT
          b.nhdplusid
         ,b.sourcefc
         ,b.gridcode
         ,b.areasqkm
         ,b.vpuid
         ,a.hydrosequence
         ,CASE WHEN NOT p_return_topology_results THEN b.shape ELSE NULL::GEOMETRY END AS shape
         FROM
         tmp_navigation_results a
         JOIN
         cipsrv_nhdplus_h.catchment_5070_full b
         ON
         b.nhdplusid = a.nhdplusid;

         GET DIAGNOSTICS int_inserted = ROW_COUNT;
         out_records_inserted := out_records_inserted + int_inserted;
         
         IF array_length(p_extra_catchments,1) > 0
         THEN
            INSERT INTO tmp_catchments(
                nhdplusid
               ,sourcefc
               ,gridcode
               ,areasqkm
               ,vpuid
               ,hydroseq
               ,shape_5070
            )
            SELECT
             a.nhdplusid
            ,a.sourcefc
            ,a.gridcode
            ,a.areasqkm
            ,a.vpuid
            ,0
            ,CASE WHEN NOT p_return_topology_results THEN a.shape ELSE NULL::GEOMETRY END AS shape
            FROM
            cipsrv_nhdplus_h.catchment_5070_full a
            WHERE
            a.nhdplusid = ANY(p_extra_catchments)
            ON CONFLICT DO NOTHING;

            GET DIAGNOSTICS int_inserted = ROW_COUNT;
            out_records_inserted := out_records_inserted + int_inserted;

         END IF;
      
      ELSIF p_known_grid = 3338
      THEN
         INSERT INTO tmp_catchments(
             nhdplusid
            ,sourcefc
            ,gridcode
            ,areasqkm
            ,vpuid
            ,hydroseq
            ,shape_3338
         )
         SELECT
          b.nhdplusid
         ,b.sourcefc
         ,b.gridcode
         ,b.areasqkm
         ,b.vpuid
         ,a.hydrosequence
         ,CASE WHEN NOT p_return_topology_results THEN b.shape ELSE NULL::GEOMETRY END AS shape
         FROM
         tmp_navigation_results a
         JOIN
         cipsrv_nhdplus_h.catchment_3338_full b
         ON
         b.nhdplusid = a.nhdplusid;

         GET DIAGNOSTICS int_inserted = ROW_COUNT;
         out_records_inserted := out_records_inserted + int_inserted;
         
         IF array_length(p_extra_catchments,1) > 0
         THEN
            INSERT INTO tmp_catchments(
                nhdplusid
               ,sourcefc
               ,gridcode
               ,areasqkm
               ,vpuid
               ,hydroseq
               ,shape_3338
            )
            SELECT
             a.nhdplusid
            ,a.sourcefc
            ,a.gridcode
            ,a.areasqkm
            ,a.vpuid
            ,0
            ,CASE WHEN NOT p_return_topology_results THEN a.shape ELSE NULL::GEOMETRY END AS shape
            FROM
            cipsrv_nhdplus_h.catchment_3338_full a
            WHERE
            a.nhdplusid = ANY(p_extra_catchments)
            ON CONFLICT DO NOTHING;

            GET DIAGNOSTICS int_inserted = ROW_COUNT;
            out_records_inserted := out_records_inserted + int_inserted;

         END IF;
      
      ELSIF p_known_grid = 26904
      THEN
         INSERT INTO tmp_catchments(
             nhdplusid
            ,sourcefc
            ,gridcode
            ,areasqkm
            ,vpuid
            ,hydroseq
            ,shape_26904
         )
         SELECT
          b.nhdplusid
         ,b.sourcefc
         ,b.gridcode
         ,b.areasqkm
         ,b.vpuid
         ,a.hydrosequence
         ,CASE WHEN NOT p_return_topology_results THEN b.shape ELSE NULL::GEOMETRY END AS shape
         FROM
         tmp_navigation_results a
         JOIN
         cipsrv_nhdplus_h.catchment_26904_full b
         ON
         b.nhdplusid = a.nhdplusid;

         GET DIAGNOSTICS int_inserted = ROW_COUNT;
         out_records_inserted := out_records_inserted + int_inserted;
         
         IF array_length(p_extra_catchments,1) > 0
         THEN
            INSERT INTO tmp_catchments(
                nhdplusid
               ,sourcefc
               ,gridcode
               ,areasqkm
               ,vpuid
               ,hydroseq
               ,shape_26904
            )
            SELECT
             a.nhdplusid
            ,a.sourcefc
            ,a.gridcode
            ,a.areasqkm
            ,a.vpuid
            ,0
            ,CASE WHEN NOT p_return_topology_results THEN a.shape ELSE NULL::GEOMETRY END AS shape
            FROM
            cipsrv_nhdplus_h.catchment_26904_full a
            WHERE
            a.nhdplusid = ANY(p_extra_catchments)
            ON CONFLICT DO NOTHING;

            GET DIAGNOSTICS int_inserted = ROW_COUNT;
            out_records_inserted := out_records_inserted + int_inserted;

         END IF;
      
      ELSIF p_known_grid = 32161
      THEN
         INSERT INTO tmp_catchments(
             nhdplusid
            ,sourcefc
            ,gridcode
            ,areasqkm
            ,vpuid
            ,hydroseq
            ,shape_32161
         )
         SELECT
          b.nhdplusid
         ,b.sourcefc
         ,b.gridcode
         ,b.areasqkm
         ,b.vpuid
         ,a.hydrosequence
         ,CASE WHEN NOT p_return_topology_results THEN b.shape ELSE NULL::GEOMETRY END AS shape
         FROM
         tmp_navigation_results a
         JOIN
         cipsrv_nhdplus_h.catchment_32161_full b
         ON
         b.nhdplusid = a.nhdplusid;

         GET DIAGNOSTICS int_inserted = ROW_COUNT;
         out_records_inserted := out_records_inserted + int_inserted;
         
         IF array_length(p_extra_catchments,1) > 0
         THEN
            INSERT INTO tmp_catchments(
                nhdplusid
               ,sourcefc
               ,gridcode
               ,areasqkm
               ,vpuid
               ,hydroseq
               ,shape_32161
            )
            SELECT
             a.nhdplusid
            ,a.sourcefc
            ,a.gridcode
            ,a.areasqkm
            ,a.vpuid
            ,0
            ,CASE WHEN NOT p_return_topology_results THEN a.shape ELSE NULL::GEOMETRY END AS shape
            FROM
            cipsrv_nhdplus_h.catchment_32161_full a
            WHERE
            a.nhdplusid = ANY(p_extra_catchments)
            ON CONFLICT DO NOTHING;

            GET DIAGNOSTICS int_inserted = ROW_COUNT;
            out_records_inserted := out_records_inserted + int_inserted;

         END IF;
      
      ELSIF p_known_grid = 32655
      THEN
         INSERT INTO tmp_catchments(
             nhdplusid
            ,sourcefc
            ,gridcode
            ,areasqkm
            ,vpuid
            ,hydroseq
            ,shape_32655
         )
         SELECT
          b.nhdplusid
         ,b.sourcefc
         ,b.gridcode
         ,b.areasqkm
         ,b.vpuid
         ,a.hydrosequence
         ,CASE WHEN NOT p_return_topology_results THEN b.shape ELSE NULL::GEOMETRY END AS shape
         FROM
         tmp_navigation_results a
         JOIN
         cipsrv_nhdplus_h.catchment_32655_full b
         ON
         b.nhdplusid = a.nhdplusid;

         GET DIAGNOSTICS int_inserted = ROW_COUNT;
         out_records_inserted := out_records_inserted + int_inserted;
         
         IF array_length(p_extra_catchments,1) > 0
         THEN
            INSERT INTO tmp_catchments(
                nhdplusid
               ,sourcefc
               ,gridcode
               ,areasqkm
               ,vpuid
               ,hydroseq
               ,shape_32655
            )
            SELECT
             a.nhdplusid
            ,a.sourcefc
            ,a.gridcode
            ,a.areasqkm
            ,a.vpuid
            ,0
            ,CASE WHEN NOT p_return_topology_results THEN a.shape ELSE NULL::GEOMETRY END AS shape
            FROM
            cipsrv_nhdplus_h.catchment_32655_full a
            WHERE
            a.nhdplusid = ANY(p_extra_catchments)
            ON CONFLICT DO NOTHING;

            GET DIAGNOSTICS int_inserted = ROW_COUNT;
            out_records_inserted := out_records_inserted + int_inserted;

         END IF;
      
      ELSIF p_known_grid = 32702
      THEN
         INSERT INTO tmp_catchments(
             nhdplusid
            ,sourcefc
            ,gridcode
            ,areasqkm
            ,vpuid
            ,hydroseq
            ,shape_32702
         )
         SELECT
          b.nhdplusid
         ,b.sourcefc
         ,b.gridcode
         ,b.areasqkm
         ,b.vpuid
         ,a.hydrosequence
         ,CASE WHEN NOT p_return_topology_results THEN b.shape ELSE NULL::GEOMETRY END AS shape
         FROM
         tmp_navigation_results a
         JOIN
         cipsrv_nhdplus_h.catchment_32702_full b
         ON
         b.nhdplusid = a.nhdplusid;

         GET DIAGNOSTICS int_inserted = ROW_COUNT;
         out_records_inserted := out_records_inserted + int_inserted;
         
         IF array_length(p_extra_catchments,1) > 0
         THEN
            INSERT INTO tmp_catchments(
                nhdplusid
               ,sourcefc
               ,gridcode
               ,areasqkm
               ,vpuid
               ,hydroseq
               ,shape_32702
            )
            SELECT
             a.nhdplusid
            ,a.sourcefc
            ,a.gridcode
            ,a.areasqkm
            ,a.vpuid
            ,0
            ,CASE WHEN NOT p_return_topology_results THEN a.shape ELSE NULL::GEOMETRY END AS shape
            FROM
            cipsrv_nhdplus_h.catchment_32702_full a
            WHERE
            a.nhdplusid = ANY(p_extra_catchments)
            ON CONFLICT DO NOTHING;

            GET DIAGNOSTICS int_inserted = ROW_COUNT;
            out_records_inserted := out_records_inserted + int_inserted;

         END IF;
      
      ELSE
         RAISE EXCEPTION 'err';
         
      END IF;   
   
   END IF;

   ----------------------------------------------------------------------------
   -- Step 30
   -- I guess that went okay
   ----------------------------------------------------------------------------
   IF out_records_inserted IS NULL
   OR out_records_inserted = 0
   THEN
      out_return_code    := -101;
      out_status_message := 'No Catchments match navigation results';
      RETURN;

   END IF;
   
END;
$BODY$ 
LANGUAGE plpgsql;

ALTER FUNCTION cipsrv_nhdplus_h.delineation_preprocessing(
    BOOLEAN
   ,INTEGER
   ,BOOLEAN
   ,BOOLEAN
   ,BIGINT[]
) OWNER TO cipsrv;

GRANT EXECUTE ON FUNCTION cipsrv_nhdplus_h.delineation_preprocessing(
    BOOLEAN
   ,INTEGER
   ,BOOLEAN
   ,BOOLEAN
   ,BIGINT[]
) TO PUBLIC;

