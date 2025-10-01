DO $$DECLARE 
   a VARCHAR;b VARCHAR;
BEGIN
   SELECT p.oid::regproc,pg_get_function_identity_arguments(p.oid)
   INTO a,b FROM pg_catalog.pg_proc p LEFT JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace
   WHERE p.oid::regproc::text = 'cipsrv_nhdplus_m.get_flowline';
   IF b IS NOT NULL THEN 
   EXECUTE FORMAT('DROP FUNCTION IF EXISTS %s(%s)',a,b);ELSE
   IF a IS NOT NULL THEN EXECUTE FORMAT('DROP FUNCTION IF EXISTS %s',a);END IF;END IF;
END$$;

CREATE OR REPLACE FUNCTION cipsrv_nhdplus_m.get_flowline(
    IN  p_direction                    VARCHAR DEFAULT NULL
   ,IN  p_nhdplusid                    BIGINT  DEFAULT NULL
   ,IN  p_permanent_identifier         VARCHAR DEFAULT NULL
   ,IN  p_reachcode                    VARCHAR DEFAULT NULL
   ,IN  p_hydroseq                     BIGINT  DEFAULT NULL
   ,IN  p_measure                      NUMERIC DEFAULT NULL
   ,IN  p_known_region                 VARCHAR DEFAULT NULL
   ,OUT out_flowline                   cipsrv_nhdplus_m.flowline
   ,OUT out_region                     VARCHAR
   ,OUT out_return_code                INTEGER
   ,OUT out_status_message             VARCHAR
)
STABLE
AS $BODY$ 
DECLARE
   rec                RECORD;
   str_direction      VARCHAR(5) := UPPER(p_direction);
   num_difference     NUMERIC;
   num_end_of_line    NUMERIC := 0.0001;
   sdo_point          GEOMETRY;
   
BEGIN

   --------------------------------------------------------------------------
   -- Step 10
   -- Check over incoming parameters
   --------------------------------------------------------------------------
   out_return_code := 0;
   
   IF  p_nhdplusid IS NULL
   AND p_permanent_identifier IS NULL
   AND p_reachcode IS NULL
   AND p_hydroseq IS NULL
   THEN
      out_return_code    := -2;
      out_status_message := 'NHDPlusID, Permanent Identifier, Reach Code or Hydrosequence value is required.';
      RETURN;
      
   END IF;
   
   IF str_direction IN ('UT','UM')
   THEN
      str_direction := 'U';
      
   ELSIF str_direction IN ('DD','DM','PP','PPALL')
   THEN
      str_direction := 'D';
      
   END IF;
   
   IF str_direction NOT IN ('D','U')
   THEN
      str_direction := 'D';
   
   END IF;
   
   --------------------------------------------------------------------------
   -- Step 20
   -- Check when nhdplusid provided
   --------------------------------------------------------------------------
   IF p_nhdplusid IS NOT NULL
   THEN
      IF p_measure IS NULL
      THEN
         SELECT 
          a.nhdplusid
         ,a.hydroseq
         ,a.frommeas AS fmeasure
         ,a.tomeas   AS tmeasure
         ,a.levelpathi
         ,a.terminalpa
         ,a.uphydroseq
         ,a.dnhydroseq
         ,a.dnminorhyd
         ,a.divergence
         ,a.streamleve
         ,a.arbolatesu
         ,a.fromnode
         ,a.tonode
         ,a.vpuid
         /* ++++++++++ */
         ,a.permanent_identifier
         ,a.reachcode
         ,a.fcode
         /* ++++++++++ */
         ,a.lengthkm
         ,a.lengthkm / (a.tomeas - a.frommeas)
         ,CASE 
          WHEN a.totma IN (-9998,-9999)
          THEN
             CAST(NULL AS NUMERIC)
          ELSE
             a.totma
          END AS flowtimeday
         ,CASE 
          WHEN a.totma IN (-9998,-9999)
          THEN
             CAST(NULL AS NUMERIC)
          ELSE
             a.totma / (a.tomeas - a.frommeas)
          END AS flowtimeday_ratio
         /* ++++++++++ */
         ,a.pathlength
         ,a.pathtimema
         /* ++++++++++ */
         ,NULL::INTEGER
         ,NULL::NUMERIC
         ,NULL::NUMERIC
         ,NULL::NUMERIC
         ,NULL::NUMERIC
         ,NULL::NUMERIC
         ,NULL::BIGINT
         INTO STRICT
         out_flowline
         FROM 
         cipsrv_nhdplus_m.networknhdflowline a
         WHERE
         a.nhdplusid = p_nhdplusid;

         out_flowline.out_lengthkm           := out_flowline.lengthkm;
         out_flowline.out_flowtimeday        := out_flowline.flowtimeday;
         
         IF str_direction = 'D'
         THEN
            out_flowline.out_measure         := out_flowline.tmeasure;
            out_flowline.out_node            := out_flowline.tonode;
            out_flowline.out_pathlengthkm    := out_flowline.pathlengthkm     + out_flowline.lengthkm;
            out_flowline.out_pathflowtimeday := out_flowline.pathflowtimeday  + out_flowline.flowtimeday;
      
         ELSIF str_direction = 'U'
         THEN  
            out_flowline.out_measure         := out_flowline.fmeasure;
            out_flowline.out_node            := out_flowline.fromnode;
            out_flowline.out_pathlengthkm    := out_flowline.pathlengthkm;
            out_flowline.out_pathflowtimeday := out_flowline.pathflowtimeday;
      
         END IF;
         
      ELSE
         SELECT 
          a.nhdplusid
         ,a.hydroseq
         ,a.frommeas AS fmeasure
         ,a.tomeas   AS tmeasure
         ,a.levelpathi
         ,a.terminalpa
         ,a.uphydroseq
         ,a.dnhydroseq
         ,a.dnminorhyd
         ,a.divergence
         ,a.streamleve
         ,a.arbolatesu
         ,a.fromnode
         ,a.tonode
         ,a.vpuid
         /* ++++++++++ */
         ,a.permanent_identifier
         ,a.reachcode
         ,a.fcode
         /* ++++++++++ */
         ,a.lengthkm
         ,a.lengthkm / (a.tomeas - a.frommeas)
         ,CASE 
          WHEN a.totma IN (-9998,-9999)
          THEN
             CAST(NULL AS NUMERIC)
          ELSE
             a.totma
          END AS flowtimeday
         ,CASE 
          WHEN a.totma IN (-9998,-9999)
          THEN
             CAST(NULL AS NUMERIC)
          ELSE
             a.totma / (a.tomeas - a.frommeas)
          END AS flowtimeday_ratio
         /* ++++++++++ */
         ,a.pathlength
         ,a.pathtimema 
         /* ++++++++++ */
         ,NULL::INTEGER
         ,NULL::NUMERIC
         ,NULL::NUMERIC
         ,NULL::NUMERIC
         ,NULL::NUMERIC
         ,NULL::NUMERIC
         ,NULL::BIGINT
         INTO STRICT
         out_flowline
         FROM 
         cipsrv_nhdplus_m.networknhdflowline a
         WHERE 
             a.nhdplusid = p_nhdplusid
         AND (
            a.frommeas = p_measure
            OR
            (a.frommeas < p_measure AND a.tomeas >= p_measure)
         );
         
         out_flowline.out_measure := p_measure;
         
         IF str_direction = 'D'
         THEN
            IF  p_measure = out_flowline.fmeasure
            AND out_flowline.hydroseq IS NOT NULL
            AND out_flowline.hydroseq = out_flowline.terminalpa
            THEN
               out_flowline.out_measure := out_flowline.fmeasure + num_end_of_line;
            
            END IF;
            
            num_difference                   := out_flowline.out_measure - out_flowline.fmeasure;
            out_flowline.out_node            := out_flowline.tonode;
            
            out_flowline.out_lengthkm        := num_difference * out_flowline.lengthkm_ratio;
            out_flowline.out_flowtimeday     := num_difference * out_flowline.flowtimeday_ratio;
            
            out_flowline.out_pathlengthkm    := out_flowline.pathlengthkm + out_flowline.lengthkm;
            out_flowline.out_pathflowtimeday := out_flowline.pathflowtimeday  + out_flowline.flowtimeday;
            
         ELSIF str_direction = 'U'
         THEN  
            IF p_measure = out_flowline.tmeasure
            AND out_flowline.uphydroseq = 0
            THEN
               out_flowline.out_measure := out_flowline.tmeasure - num_end_of_line;
            
            END IF;
            
            num_difference                   := out_flowline.tmeasure - out_flowline.out_measure;
            out_flowline.out_node            := out_flowline.fromnode;
            
            out_flowline.out_lengthkm        := num_difference * out_flowline.lengthkm_ratio;
            out_flowline.out_flowtimeday     := num_difference * out_flowline.flowtimeday_ratio;
            
            out_flowline.out_pathlengthkm    := out_flowline.pathlengthkm + (( 100 - num_difference ) * out_flowline.lengthkm_ratio);
            out_flowline.out_pathflowtimeday := out_flowline.pathflowtimeday  + (( 100 - num_difference ) * out_flowline.flowtimeday_ratio);
      
         ELSE
            RAISE EXCEPTION 'err';
            
         END IF;

      END IF;
   
   ELSIF p_permanent_identifier IS NOT NULL
   THEN
      IF p_measure IS NULL
      THEN
         SELECT 
          a.nhdplusid
         ,a.hydroseq
         ,a.frommeas AS fmeasure
         ,a.tomeas   AS tmeasure
         ,a.levelpathi
         ,a.terminalpa
         ,a.uphydroseq
         ,a.dnhydroseq
         ,a.dnminorhyd
         ,a.divergence
         ,a.streamleve
         ,a.arbolatesu
         ,a.fromnode
         ,a.tonode
         ,a.vpuid
         /* ++++++++++ */
         ,a.permanent_identifier
         ,a.reachcode
         ,a.fcode
         /* ++++++++++ */
         ,a.lengthkm
         ,a.lengthkm / (a.tomeas - a.frommeas)
         ,CASE 
          WHEN a.totma IN (-9998,-9999)
          THEN
             CAST(NULL AS NUMERIC)
          ELSE
             a.totma 
          END AS flowtimeday
         ,CASE 
          WHEN a.totma IN (-9998,-9999)
          THEN
             CAST(NULL AS NUMERIC)
          ELSE
             a.totma / (a.tomeas - a.frommeas)
          END AS flowtimeday_ratio
         /* ++++++++++ */
         ,a.pathlength
         ,a.pathtimema
         /* ++++++++++ */
         ,NULL::INTEGER
         ,NULL::NUMERIC
         ,NULL::NUMERIC
         ,NULL::NUMERIC
         ,NULL::NUMERIC
         ,NULL::NUMERIC
         ,NULL::BIGINT
         INTO STRICT
         out_flowline
         FROM 
         cipsrv_nhdplus_m.networknhdflowline a
         WHERE
         a.permanent_identifier = p_permanent_identifier;

         out_flowline.out_lengthkm           := out_flowline.lengthkm;
         out_flowline.out_flowtimeday        := out_flowline.flowtimeday;
         
         IF str_direction = 'D'
         THEN
            out_flowline.out_measure         := out_flowline.tmeasure;
            out_flowline.out_node            := out_flowline.tonode;
            out_flowline.out_pathlengthkm    := out_flowline.pathlengthkm     + out_flowline.lengthkm;
            out_flowline.out_pathflowtimeday := out_flowline.pathflowtimeday  + out_flowline.flowtimeday;
      
         ELSIF str_direction = 'U'
         THEN  
            out_flowline.out_measure         := out_flowline.fmeasure;
            out_flowline.out_node            := out_flowline.fromnode;
            out_flowline.out_pathlengthkm    := out_flowline.pathlengthkm;
            out_flowline.out_pathflowtimeday := out_flowline.pathflowtimeday;
      
         END IF;
         
      ELSE
         SELECT 
          a.nhdplusid
         ,a.hydroseq
         ,a.frommeas AS fmeasure
         ,a.tomeas   AS tmeasure
         ,a.levelpathi
         ,a.terminalpa
         ,a.uphydroseq
         ,a.dnhydroseq
         ,a.dnminorhyd
         ,a.divergence
         ,a.streamleve
         ,a.arbolatesu
         ,a.fromnode
         ,a.tonode
         ,a.vpuid
         /* ++++++++++ */
         ,a.permanent_identifier
         ,a.reachcode
         ,a.fcode
         /* ++++++++++ */
         ,a.lengthkm
         ,a.lengthkm / (a.tomeas - a.frommeas)
         ,CASE 
          WHEN a.totma IN (-9998,-9999)
          THEN
             CAST(NULL AS NUMERIC)
          ELSE
             a.totma 
          END AS flowtimeday
         ,CASE 
          WHEN a.totma IN (-9998,-9999)
          THEN
             CAST(NULL AS NUMERIC)
          ELSE
             a.totma / (a.tomeas - a.frommeas)
          END AS flowtimeday_ratio
         /* ++++++++++ */
         ,a.pathlength
         ,a.pathtimema 
         /* ++++++++++ */
         ,NULL::INTEGER
         ,NULL::NUMERIC
         ,NULL::NUMERIC
         ,NULL::NUMERIC
         ,NULL::NUMERIC
         ,NULL::NUMERIC
         ,NULL::BIGINT
         INTO STRICT
         out_flowline
         FROM 
         cipsrv_nhdplus_m.networknhdflowline a
         WHERE
             a.permanent_identifier = p_permanent_identifier
         AND (
            a.frommeas = p_measure
            OR
            (a.frommeas < p_measure AND a.tomeas >= p_measure)
         );
         
         out_flowline.out_measure := p_measure;
         
         IF str_direction = 'D'
         THEN
            IF  p_measure = out_flowline.fmeasure
            AND out_flowline.hydroseq IS NOT NULL
            AND out_flowline.hydroseq = out_flowline.terminalpa
            THEN
               out_flowline.out_measure := out_flowline.fmeasure + num_end_of_line;
            
            END IF;
            
            num_difference                 := out_flowline.out_measure - out_flowline.fmeasure;
            out_flowline.out_node            := out_flowline.tonode;
            
            out_flowline.out_lengthkm        := num_difference * out_flowline.lengthkm_ratio;
            out_flowline.out_flowtimeday     := num_difference * out_flowline.flowtimeday_ratio;
            
            out_flowline.out_pathlengthkm    := out_flowline.pathlengthkm + out_flowline.lengthkm;
            out_flowline.out_pathflowtimeday := out_flowline.pathflowtimeday  + out_flowline.flowtimeday;
            
         ELSIF str_direction = 'U'
         THEN  
            IF p_measure = out_flowline.tmeasure
            AND out_flowline.uphydroseq = 0
            THEN
               out_flowline.out_measure := out_flowline.tmeasure - num_end_of_line;
            
            END IF;
            
            num_difference                   := out_flowline.tmeasure - out_flowline.out_measure;
            out_flowline.out_node            := out_flowline.fromnode;
            
            out_flowline.out_lengthkm        := num_difference * out_flowline.lengthkm_ratio;
            out_flowline.out_flowtimeday     := num_difference * out_flowline.flowtimeday_ratio;
            
            out_flowline.out_pathlengthkm    := out_flowline.pathlengthkm + (( 100 - num_difference ) * out_flowline.lengthkm_ratio);
            out_flowline.out_pathflowtimeday := out_flowline.pathflowtimeday  + (( 100 - num_difference ) * out_flowline.flowtimeday_ratio);
      
         ELSE
            RAISE EXCEPTION 'err';
            
         END IF;

      END IF;
         
   ELSIF p_hydroseq IS NOT NULL
   THEN
      IF p_measure IS NULL
      THEN
         SELECT 
          a.nhdplusid
         ,a.hydroseq
         ,a.frommeas AS fmeasure
         ,a.tomeas   AS tmeasure
         ,a.levelpathi
         ,a.terminalpa
         ,a.uphydroseq
         ,a.dnhydroseq
         ,a.dnminorhyd
         ,a.divergence
         ,a.streamleve
         ,a.arbolatesu
         ,a.fromnode
         ,a.tonode
         ,a.vpuid
         /* ++++++++++ */
         ,a.permanent_identifier
         ,a.reachcode
         ,a.fcode
         /* ++++++++++ */
         ,a.lengthkm
         ,a.lengthkm / (a.tomeas - a.frommeas)
         ,CASE 
          WHEN a.totma IN (-9998,-9999)
          THEN
             CAST(NULL AS NUMERIC)
          ELSE
             a.totma 
          END AS flowtimeday
         ,CASE 
          WHEN a.totma IN (-9998,-9999)
          THEN
             CAST(NULL AS NUMERIC)
          ELSE
             a.totma / (a.tomeas - a.frommeas)
          END AS flowtimeday_ratio
         /* ++++++++++ */
         ,a.pathlength
         ,a.pathtimema
         /* ++++++++++ */
         ,NULL::INTEGER
         ,NULL::NUMERIC
         ,NULL::NUMERIC
         ,NULL::NUMERIC
         ,NULL::NUMERIC
         ,NULL::NUMERIC
         ,NULL::BIGINT
         INTO STRICT
         out_flowline
         FROM 
         cipsrv_nhdplus_m.networknhdflowline a
         WHERE
         a.hydroseq = p_hydroseq;

         out_flowline.out_lengthkm           := out_flowline.lengthkm;
         out_flowline.out_flowtimeday        := out_flowline.flowtimeday;
         
         IF str_direction = 'D'
         THEN
            out_flowline.out_measure         := out_flowline.tmeasure;
            out_flowline.out_node            := out_flowline.tonode;
            out_flowline.out_pathlengthkm    := out_flowline.pathlengthkm     + out_flowline.lengthkm;
            out_flowline.out_pathflowtimeday := out_flowline.pathflowtimeday  + out_flowline.flowtimeday;
      
         ELSIF str_direction = 'U'
         THEN  
            out_flowline.out_measure         := out_flowline.fmeasure;
            out_flowline.out_node            := out_flowline.fromnode;
            out_flowline.out_pathlengthkm    := out_flowline.pathlengthkm;
            out_flowline.out_pathflowtimeday := out_flowline.pathflowtimeday;
      
         END IF;
         
      ELSE
         SELECT 
          a.nhdplusid
         ,a.hydroseq
         ,a.frommeas AS fmeasure
         ,a.tomeas   AS tmeasure
         ,a.levelpathi
         ,a.terminalpa
         ,a.uphydroseq
         ,a.dnhydroseq
         ,a.dnminorhyd
         ,a.divergence
         ,a.streamleve
         ,a.arbolatesu
         ,a.fromnode
         ,a.tonode
         ,a.vpuid
         /* ++++++++++ */
         ,a.permanent_identifier
         ,a.reachcode
         ,a.fcode
         /* ++++++++++ */
         ,a.lengthkm
         ,a.lengthkm / (a.tomeas - a.frommeas)
         ,CASE 
          WHEN a.totma IN (-9998,-9999)
          THEN
             CAST(NULL AS NUMERIC)
          ELSE
             a.totma 
          END AS flowtimeday
         ,CASE 
          WHEN a.totma IN (-9998,-9999)
          THEN
             CAST(NULL AS NUMERIC)
          ELSE
             a.totma / (a.tomeas - a.frommeas)
          END AS flowtimeday_ratio
         /* ++++++++++ */
         ,a.pathlength
         ,a.pathtimema 
         /* ++++++++++ */
         ,NULL::INTEGER
         ,NULL::NUMERIC
         ,NULL::NUMERIC
         ,NULL::NUMERIC
         ,NULL::NUMERIC
         ,NULL::NUMERIC
         ,NULL::BIGINT
         INTO STRICT
         out_flowline
         FROM 
         cipsrv_nhdplus_m.networknhdflowline a
         WHERE
             a.hydroseq = p_hydroseq
         AND (
            a.frommeas = p_measure
            OR
            (a.frommeas < p_measure AND a.tomeas >= p_measure)
         );
         
         out_flowline.out_measure := p_measure;
         
         IF str_direction = 'D'
         THEN
            IF  p_measure = out_flowline.fmeasure
            AND out_flowline.hydroseq IS NOT NULL
            AND out_flowline.hydroseq = out_flowline.terminalpa
            THEN
               out_flowline.out_measure := out_flowline.fmeasure + num_end_of_line;
            
            END IF;
            
            num_difference                 := out_flowline.out_measure - out_flowline.fmeasure;
            out_flowline.out_node            := out_flowline.tonode;
            
            out_flowline.out_lengthkm        := num_difference * out_flowline.lengthkm_ratio;
            out_flowline.out_flowtimeday     := num_difference * out_flowline.flowtimeday_ratio;
            
            out_flowline.out_pathlengthkm    := out_flowline.pathlengthkm + out_flowline.lengthkm;
            out_flowline.out_pathflowtimeday := out_flowline.pathflowtimeday  + out_flowline.flowtimeday;
            
         ELSIF str_direction = 'U'
         THEN  
            IF p_measure = out_flowline.tmeasure
            AND out_flowline.uphydroseq = 0
            THEN
               out_flowline.out_measure := out_flowline.tmeasure - num_end_of_line;
            
            END IF;
            
            num_difference                   := out_flowline.tmeasure - out_flowline.out_measure;
            out_flowline.out_node            := out_flowline.fromnode;
            
            out_flowline.out_lengthkm        := num_difference * out_flowline.lengthkm_ratio;
            out_flowline.out_flowtimeday     := num_difference * out_flowline.flowtimeday_ratio;
            
            out_flowline.out_pathlengthkm    := out_flowline.pathlengthkm + (( 100 - num_difference ) * out_flowline.lengthkm_ratio);
            out_flowline.out_pathflowtimeday := out_flowline.pathflowtimeday  + (( 100 - num_difference ) * out_flowline.flowtimeday_ratio);
      
         ELSE
            RAISE EXCEPTION 'err';
            
         END IF;

      END IF; 

   --------------------------------------------------------------------------
   -- Step 40
   -- Check when reach code provided
   --------------------------------------------------------------------------
   ELSIF p_reachcode IS NOT NULL
   THEN
      IF p_measure IS NULL
      THEN
         SELECT 
          a.nhdplusid
         ,a.hydroseq
         ,a.frommeas AS fmeasure
         ,a.tomeas   AS tmeasure
         ,a.levelpathi
         ,a.terminalpa
         ,a.uphydroseq
         ,a.dnhydroseq
         ,a.dnminorhyd
         ,a.divergence
         ,a.streamleve
         ,a.arbolatesu
         ,a.fromnode
         ,a.tonode
         ,a.vpuid
         /* ++++++++++ */
         ,a.permanent_identifier
         ,a.reachcode
         ,a.fcode
         /* ++++++++++ */
         ,a.lengthkm
         ,a.lengthkm / (a.tomeas - a.frommeas)
         ,CASE 
          WHEN a.totma IN (-9998,-9999)
          THEN
             CAST(NULL AS NUMERIC)
          ELSE
             a.totma 
          END AS flowtimeday
         ,CASE 
          WHEN a.totma IN (-9998,-9999)
          THEN
             CAST(NULL AS NUMERIC)
          ELSE
             a.totma / (a.tomeas - a.frommeas)
          END AS flowtimeday_ratio
         /* ++++++++++ */
         ,a.pathlength
         ,a.pathtimema 
         /* ++++++++++ */
         ,NULL::INTEGER
         ,NULL::NUMERIC
         ,NULL::NUMERIC
         ,NULL::NUMERIC
         ,NULL::NUMERIC
         ,NULL::NUMERIC
         ,NULL::BIGINT
         INTO STRICT
         out_flowline
         FROM 
         cipsrv_nhdplus_m.networknhdflowline a
         WHERE 
             a.reachcode = p_reachcode 
         AND (
               (str_direction = 'D' AND a.tomeas = 100)
            OR (str_direction = 'U' AND a.frommeas = 0 )
         );
         
         out_flowline.out_lengthkm           := out_flowline.lengthkm;
         out_flowline.out_flowtimeday        := out_flowline.flowtimeday;
         
         IF str_direction = 'D'
         THEN
            out_flowline.out_measure         := 100;
            out_flowline.out_node            := out_flowline.tonode;
            
            out_flowline.out_pathlengthkm    := out_flowline.pathlengthkm + out_flowline.lengthkm;
            out_flowline.out_pathflowtimeday := out_flowline.pathflowtimeday  + out_flowline.flowtimeday;
            
         ELSIF str_direction = 'U'
         THEN
            out_flowline.out_measure         := 0;
            out_flowline.out_node            := out_flowline.fromnode;
            
            out_flowline.out_pathlengthkm    := out_flowline.pathlengthkm;
            out_flowline.out_pathflowtimeday := out_flowline.pathflowtimeday;
         
         ELSE
            RAISE EXCEPTION 'err';
            
         END IF;
         
      ELSE
         SELECT 
          a.nhdplusid
         ,a.hydroseq
         ,a.frommeas AS fmeasure
         ,a.tomeas   AS tmeasure
         ,a.levelpathi
         ,a.terminalpa
         ,a.uphydroseq
         ,a.dnhydroseq
         ,a.dnminorhyd
         ,a.divergence
         ,a.streamleve
         ,a.arbolatesu
         ,a.fromnode
         ,a.tonode
         ,a.vpuid
         /* ++++++++++ */
         ,a.permanent_identifier
         ,a.reachcode
         ,a.fcode
         /* ++++++++++ */
         ,a.lengthkm
         ,a.lengthkm / (a.tomeas - a.frommeas)
         ,CASE 
          WHEN a.totma IN (-9998,-9999)
          THEN
             CAST(NULL AS NUMERIC)
          ELSE
             a.totma
          END AS flowtimeday
         ,CASE 
          WHEN a.totma IN (-9998,-9999)
          THEN
             CAST(NULL AS NUMERIC)
          ELSE
             a.totma / (a.tomeas - a.frommeas)
          END AS flowtimeday_ratio
         /* ++++++++++ */
         ,a.pathlength
         ,a.pathtimema
         /* ++++++++++ */
         ,NULL::INTEGER
         ,NULL::NUMERIC
         ,NULL::NUMERIC
         ,NULL::NUMERIC
         ,NULL::NUMERIC
         ,NULL::NUMERIC
         ,NULL::BIGINT
         INTO STRICT
         out_flowline
         FROM 
         cipsrv_nhdplus_m.networknhdflowline a
         WHERE 
             a.reachcode = p_reachcode 
         AND (
            (p_measure = 0 AND a.frommeas = 0)
            OR
            (a.frommeas < p_measure AND a.tomeas >= p_measure)
         );
         
         out_flowline.out_measure := p_measure;
         
         IF str_direction = 'D'
         THEN
            IF  p_measure = out_flowline.fmeasure
            AND out_flowline.hydroseq IS NOT NULL
            AND out_flowline.hydroseq = out_flowline.terminalpa
            THEN
               out_flowline.out_measure := out_flowline.fmeasure + num_end_of_line;
            
            END IF;
            
            num_difference                   := p_measure - out_flowline.fmeasure;
            out_flowline.out_node            := out_flowline.tonode;
            
            out_flowline.out_lengthkm        := num_difference * out_flowline.lengthkm_ratio;
            out_flowline.out_flowtimeday     := num_difference * out_flowline.flowtimeday_ratio;
            
            out_flowline.out_pathlengthkm    := out_flowline.pathlengthkm + out_flowline.lengthkm;
            out_flowline.out_pathflowtimeday := out_flowline.pathflowtimeday  + out_flowline.flowtimeday;
            
         ELSIF str_direction = 'U'
         THEN
            IF p_measure = out_flowline.tmeasure
            AND out_flowline.uphydroseq = 0
            THEN
               out_flowline.out_measure := out_flowline.tmeasure - num_end_of_line;
            
            END IF;
            
            num_difference                   := out_flowline.tmeasure - p_measure;
            out_flowline.out_node            := out_flowline.fromnode;
            
            out_flowline.out_lengthkm        := num_difference * out_flowline.lengthkm_ratio;
            out_flowline.out_flowtimeday     := num_difference * out_flowline.flowtimeday_ratio;
            
            out_flowline.out_pathlengthkm    := out_flowline.pathlengthkm + (( 100 - num_difference ) * out_flowline.lengthkm_ratio);
            out_flowline.out_pathflowtimeday := out_flowline.pathflowtimeday  + (( 100 - num_difference ) * out_flowline.flowtimeday_ratio);
      
         ELSE
            RAISE EXCEPTION 'err';
            
         END IF;
 
      END IF;
    
   END IF;
   
   --------------------------------------------------------------------------
   -- Step 50
   -- Determine grid srid
   --------------------------------------------------------------------------
   IF p_known_region IS NOT NULL
   THEN
      out_region                 := p_known_region::VARCHAR;
      out_flowline.out_grid_srid := p_known_region::INTEGER;
      
   ELSE
      SELECT 
      ST_StartPoint(a.shape)
      INTO
      sdo_point
      FROM 
      cipsrv_nhdplus_m.networknhdflowline a
      WHERE
      a.nhdplusid = out_flowline.nhdplusid;
      
      rec := cipsrv_nhdplus_m.determine_grid_srid(
          p_geometry      := sdo_point
         ,p_known_region  := p_known_region
      );
      out_region                 := rec.out_srid::VARCHAR;
      out_return_code            := rec.out_return_code;
      out_status_message         := rec.out_status_message;
      out_flowline.out_grid_srid := rec.out_srid;
 
   END IF;
   
EXCEPTION

   WHEN NO_DATA_FOUND
   THEN
      out_return_code    := -10;
      out_status_message := 'no results found in cipsrv_nhdplus_m nhdflowline';
      RETURN;

   WHEN OTHERS
   THEN
      RAISE;   

END;
$BODY$
LANGUAGE plpgsql;

DO $$DECLARE 
   a VARCHAR;b VARCHAR;
BEGIN
   SELECT p.oid::regproc,pg_get_function_identity_arguments(p.oid)
   INTO a,b FROM pg_catalog.pg_proc p LEFT JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace
   WHERE p.oid::regproc::text = 'cipsrv_nhdplus_m.get_flowline';
   IF b IS NOT NULL THEN 
   EXECUTE FORMAT('ALTER FUNCTION %s(%s) OWNER TO cipsrv',a,b);
   EXECUTE FORMAT('GRANT EXECUTE ON FUNCTION %s(%s) TO PUBLIC',a,b);
   ELSE
   IF a IS NOT NULL THEN 
   EXECUTE FORMAT('ALTER FUNCTION %s OWNER TO cipsrv',a);
   EXECUTE FORMAT('GRANT EXECUTE ON FUNCTION %s TO PUBLIC',a);
   ELSE RAISE EXCEPTION 'prob'; 
   END IF;END IF;
END$$;

