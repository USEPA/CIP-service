CREATE OR REPLACE FUNCTION cipsrv_nhdplus_m.get_flowline(
    IN  p_direction            VARCHAR DEFAULT NULL
   ,IN  p_nhdplusid            BIGINT  DEFAULT NULL
   ,IN  p_permanent_identifier VARCHAR DEFAULT NULL
   ,IN  p_reachcode            VARCHAR DEFAULT NULL
   ,IN  p_hydroseq             BIGINT  DEFAULT NULL
   ,IN  p_measure              NUMERIC DEFAULT NULL
   ,OUT out_flowline           cipsrv_nhdplus_m.flowline
   ,OUT out_return_code        INTEGER
   ,OUT out_status_message     VARCHAR
)
STABLE
AS $BODY$ 
DECLARE
   str_direction      VARCHAR(5) := UPPER(p_direction);
   num_difference     NUMERIC;
   num_end_of_line    NUMERIC := 0.0001;
   
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
   IF p_nhdplusid            IS NOT NULL
   OR p_permanent_identifier IS NOT NULL
   OR p_hydroseq             IS NOT NULL
   THEN
      IF p_measure IS NULL
      THEN
         SELECT 
          a.nhdplusid
         ,b.hydroseq
         ,a.fmeasure
         ,a.tmeasure
         ,b.levelpathi
         ,b.terminalpa
         ,b.uphydroseq
         ,b.dnhydroseq
         ,b.dnminorhyd
         ,b.divergence
         ,b.streamleve
         ,b.arbolatesu
         ,b.fromnode
         ,b.tonode
         ,a.vpuid
         /* ++++++++++ */
         ,a.permanent_identifier
         ,a.reachcode
         ,a.fcode
         /* ++++++++++ */
         ,a.lengthkm
         ,a.lengthkm / (a.tmeasure - a.fmeasure)
         ,b.totma AS flowtimeday
         ,b.totma / (a.tmeasure - a.fmeasure)
         /* ++++++++++ */
         ,b.pathlength
         ,b.pathtimema
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
         cipsrv_nhdplus_m.nhdflowline a
         LEFT JOIN
         cipsrv_nhdplus_m.nhdplusflowlinevaa b
         ON
         a.nhdplusid = b.nhdplusid
         WHERE
            a.nhdplusid            = p_nhdplusid
         OR a.permanent_identifier = p_permanent_identifier
         OR b.hydroseq             = p_hydroseq;

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
         ,b.hydroseq
         ,a.fmeasure
         ,a.tmeasure
         ,b.levelpathi
         ,b.terminalpa
         ,b.uphydroseq
         ,b.dnhydroseq
         ,b.dnminorhyd
         ,b.divergence
         ,b.streamleve
         ,b.arbolatesu
         ,b.fromnode
         ,b.tonode
         ,a.vpuid
         /* ++++++++++ */
         ,a.permanent_identifier
         ,a.reachcode
         ,a.fcode
         /* ++++++++++ */
         ,a.lengthkm
         ,a.lengthkm / (a.tmeasure - a.fmeasure)
         ,b.totma AS flowtimeday
         ,b.totma / (a.tmeasure - a.fmeasure)
         /* ++++++++++ */
         ,b.pathlength
         ,b.pathtimema 
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
         cipsrv_nhdplus_m.nhdflowline a
         LEFT JOIN
         cipsrv_nhdplus_m.nhdplusflowlinevaa b
         ON
         a.nhdplusid = b.nhdplusid
         WHERE (
               a.nhdplusid            = p_nhdplusid
            OR a.permanent_identifier = p_permanent_identifier
            OR b.hydroseq             = p_hydroseq
         ) AND (
            a.fmeasure = p_measure
            OR
            (a.fmeasure < p_measure AND a.tmeasure >= p_measure)
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
         ,b.hydroseq
         ,a.fmeasure
         ,a.tmeasure
         ,b.levelpathi
         ,b.terminalpa
         ,b.uphydroseq
         ,b.dnhydroseq
         ,b.dnminorhyd
         ,b.divergence
         ,b.streamleve
         ,b.arbolatesu
         ,b.fromnode
         ,b.tonode
         ,a.vpuid
         /* ++++++++++ */
         ,a.permanent_identifier
         ,a.reachcode
         ,a.fcode
         /* ++++++++++ */
         ,a.lengthkm
         ,a.lengthkm / (a.tmeasure - a.fmeasure)
         ,b.totma AS flowtimeday
         ,b.totma / (a.tmeasure - a.fmeasure)
         /* ++++++++++ */
         ,b.pathlength
         ,b.pathtimema 
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
         cipsrv_nhdplus_m.nhdflowline a
         LEFT JOIN
         cipsrv_nhdplus_m.nhdplusflowlinevaa b
         ON
         a.nhdplusid = b.nhdplusid
         WHERE 
             a.reachcode = p_reachcode 
         AND (
               (str_direction = 'D' AND a.tmeasure = 100)
            OR (str_direction = 'U' AND a.fmeasure = 0 )
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
         ,b.hydroseq
         ,a.fmeasure
         ,a.tmeasure
         ,b.levelpathi
         ,b.terminalpa
         ,b.uphydroseq
         ,b.dnhydroseq
         ,b.dnminorhyd
         ,b.divergence
         ,b.streamleve
         ,b.arbolatesu
         ,b.fromnode
         ,b.tonode
         ,a.vpuid
         /* ++++++++++ */
         ,a.permanent_identifier
         ,a.reachcode
         ,a.fcode
         /* ++++++++++ */
         ,a.lengthkm
         ,a.lengthkm / (a.tmeasure - a.fmeasure)
         ,b.totma AS flowtimeday
         ,b.totma / (a.tmeasure - a.fmeasure)
         /* ++++++++++ */
         ,b.pathlength
         ,b.pathtimema
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
         cipsrv_nhdplus_m.nhdflowline a
         LEFT JOIN
         cipsrv_nhdplus_m.nhdplusflowlinevaa b
         ON
         a.nhdplusid = b.nhdplusid
         WHERE 
             a.reachcode = p_reachcode 
         AND (
            (p_measure = 0 AND a.fmeasure = 0)
            OR
            (a.fmeasure < p_measure AND a.tmeasure >= p_measure)
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
   IF out_flowline.vpuid = '20'
   OR SUBSTR(out_flowline.vpuid,1,2) = '20'
   THEN
      out_flowline.out_grid_srid := 26904;
      
   ELSIF out_flowline.vpuid = '21'
   OR SUBSTR(out_flowline.vpuid,1,2) = '21'
   THEN
      out_flowline.out_grid_srid := 32161;
      
   ELSIF out_flowline.vpuid IN ('22G','22M')
   OR SUBSTR(out_flowline.vpuid,1,4) IN ('2201','2202')
   THEN
      out_flowline.out_grid_srid := 32655;
      
   ELSIF out_flowline.vpuid = '22A'
   OR SUBSTR(out_flowline.vpuid,1,4) = '2203'
   THEN
      out_flowline.out_grid_srid := 32702;
   
   ELSE
      out_flowline.out_grid_srid := 5070;
      
   END IF;
   
EXCEPTION

   WHEN NO_DATA_FOUND
   THEN
      out_return_code    := -10;
      out_status_message := 'no results found in NHDPlus';
      RETURN;

   WHEN OTHERS
   THEN
      RAISE;   

END;
$BODY$
LANGUAGE plpgsql;

ALTER FUNCTION cipsrv_nhdplus_m.get_flowline(
    VARCHAR
   ,BIGINT
   ,VARCHAR
   ,VARCHAR
   ,BIGINT
   ,NUMERIC
) OWNER TO cipsrv;

GRANT EXECUTE ON FUNCTION cipsrv_nhdplus_m.get_flowline(
    VARCHAR
   ,BIGINT
   ,VARCHAR
   ,VARCHAR
   ,BIGINT
   ,NUMERIC
) TO PUBLIC;

