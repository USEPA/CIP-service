DO $$DECLARE 
   a VARCHAR;b VARCHAR;
BEGIN
   SELECT p.oid::regproc,pg_get_function_identity_arguments(p.oid)
   INTO a,b FROM pg_catalog.pg_proc p LEFT JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace
   WHERE p.oid::regproc::text = 'cipsrv_nhdplus_h.get_measure';
   IF b IS NOT NULL THEN 
   EXECUTE FORMAT('DROP FUNCTION IF EXISTS %s(%s)',a,b);ELSE
   IF a IS NOT NULL THEN EXECUTE FORMAT('DROP FUNCTION IF EXISTS %s',a);END IF;END IF;
END$$;

CREATE OR REPLACE FUNCTION cipsrv_nhdplus_h.get_measure(
    IN  p_direction                    VARCHAR DEFAULT NULL
   ,IN  p_nhdplusid                    BIGINT  DEFAULT NULL
   ,IN  p_permanent_identifier         VARCHAR DEFAULT NULL
   ,IN  p_reachcode                    VARCHAR DEFAULT NULL
   ,IN  p_hydroseq                     BIGINT  DEFAULT NULL
   ,IN  p_measure                      NUMERIC DEFAULT NULL
   ,OUT out_measure                    NUMERIC
   ,OUT out_nhdplusid                  BIGINT
   ,OUT out_permanent_identifier       VARCHAR
   ,OUT out_reachcode                  VARCHAR
   ,OUT out_fmeasure                   NUMERIC
   ,OUT out_tmeasure                   NUMERIC
   ,OUT out_hydroseq                   BIGINT
   ,OUT out_uphydroseq                 BIGINT
   ,OUT out_terminalpa                 BIGINT
   ,OUT out_return_code                INTEGER
   ,OUT out_status_message             VARCHAR
)
STABLE
AS $BODY$ 
DECLARE
   rec                RECORD;
   str_direction      VARCHAR(5) := UPPER(p_direction);
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
   
   IF str_direction IN ('UT','UM','UTNMD')
   THEN
      str_direction := 'U';
      
   ELSIF str_direction IN ('DD','DM','PP','PPALL')
   THEN
      str_direction := 'D';
      
   ELSE
      RAISE EXCEPTION 'err %',str_direction;
      
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
         ,a.permanent_identifier 
         ,a.reachcode
         ,ROUND(a.frommeas::NUMERIC,5) AS fmeasure
         ,ROUND(a.tomeas::NUMERIC,5)   AS tmeasure
         ,a.hydroseq
         ,a.uphydroseq
         ,a.terminalpa
         INTO
          out_nhdplusid
         ,out_permanent_identifier 
         ,out_reachcode
         ,out_fmeasure
         ,out_tmeasure
         ,out_hydroseq
         ,out_uphydroseq
         ,out_terminalpa
         FROM 
         cipsrv_nhdplus_h.networknhdflowline a
         WHERE
         a.nhdplusid = p_nhdplusid;

         IF str_direction = 'D'
         THEN
            out_measure         := out_tmeasure - num_end_of_line;
            
         ELSIF str_direction = 'U'
         THEN  
            out_measure         := out_fmeasure + num_end_of_line;
            
         END IF;
         
      ELSE
         SELECT 
          a.nhdplusid
         ,a.permanent_identifier 
         ,a.reachcode
         ,ROUND(a.frommeas::NUMERIC,5) AS fmeasure
         ,ROUND(a.tomeas::NUMERIC,5)   AS tmeasure
         ,a.hydroseq
         ,a.uphydroseq
         ,a.terminalpa
         INTO
          out_nhdplusid
         ,out_permanent_identifier 
         ,out_reachcode
         ,out_fmeasure
         ,out_tmeasure
         ,out_hydroseq
         ,out_uphydroseq
         ,out_terminalpa
         FROM 
         cipsrv_nhdplus_h.networknhdflowline a
         WHERE 
             a.nhdplusid = p_nhdplusid
         AND (
            a.frommeas = p_measure
            OR
            (a.frommeas < p_measure AND a.tomeas >= p_measure)
         );
         
         out_measure := p_measure;
         
         IF str_direction = 'D'
         THEN
            IF  p_measure = out_fmeasure
            AND out_hydroseq IS NOT NULL
            AND out_hydroseq = out_terminalpa
            THEN
               out_measure := out_fmeasure + num_end_of_line;
            
            END IF;
            
         ELSIF str_direction = 'U'
         THEN  
            IF p_measure = out_tmeasure
            AND out_uphydroseq = 0
            THEN
               out_measure := out_tmeasure - num_end_of_line;
            
            END IF;

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
         ,a.permanent_identifier 
         ,a.reachcode
         ,ROUND(a.frommeas::NUMERIC,5) AS fmeasure
         ,ROUND(a.tomeas::NUMERIC,5)   AS tmeasure
         ,a.hydroseq
         ,a.uphydroseq
         ,a.terminalpa
         INTO
          out_nhdplusid
         ,out_permanent_identifier 
         ,out_reachcode
         ,out_fmeasure
         ,out_tmeasure
         ,out_hydroseq
         ,out_uphydroseq
         ,out_terminalpa
         FROM 
         cipsrv_nhdplus_h.networknhdflowline a
         WHERE
         a.permanent_identifier = p_permanent_identifier;
         
         IF str_direction = 'D'
         THEN
            out_measure         := out_tmeasure - num_end_of_line;
      
         ELSIF str_direction = 'U'
         THEN  
            out_measure         := out_fmeasure + num_end_of_line;
      
         END IF;
         
      ELSE
         SELECT 
          a.nhdplusid
         ,a.permanent_identifier 
         ,a.reachcode
         ,ROUND(a.frommeas::NUMERIC,5) AS fmeasure
         ,ROUND(a.tomeas::NUMERIC,5)   AS tmeasure
         ,a.hydroseq
         ,a.uphydroseq
         ,a.terminalpa
         INTO
          out_nhdplusid
         ,out_permanent_identifier 
         ,out_reachcode
         ,out_fmeasure
         ,out_tmeasure
         ,out_hydroseq
         ,out_uphydroseq
         ,out_terminalpa
         FROM 
         cipsrv_nhdplus_h.networknhdflowline a
         WHERE
             a.permanent_identifier = p_permanent_identifier
         AND (
            a.frommeas = p_measure
            OR
            (a.frommeas < p_measure AND a.tomeas >= p_measure)
         );
         
         out_measure := p_measure;
         
         IF str_direction = 'D'
         THEN
            IF  p_measure = out_fmeasure
            AND out_hydroseq IS NOT NULL
            AND out_hydroseq = out_terminalpa
            THEN
               out_measure := out_fmeasure + num_end_of_line;
            
            END IF;

         ELSIF str_direction = 'U'
         THEN  
            IF p_measure = out_tmeasure
            AND out_uphydroseq = 0
            THEN
               out_measure := out_tmeasure - num_end_of_line;
            
            END IF;
            
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
         ,a.permanent_identifier 
         ,a.reachcode
         ,ROUND(a.frommeas::NUMERIC,5) AS fmeasure
         ,ROUND(a.tomeas::NUMERIC,5)   AS tmeasure
         ,a.hydroseq
         ,a.uphydroseq
         ,a.terminalpa
         INTO
          out_nhdplusid
         ,out_permanent_identifier 
         ,out_reachcode
         ,out_fmeasure
         ,out_tmeasure
         ,out_hydroseq
         ,out_uphydroseq
         ,out_terminalpa
         FROM 
         cipsrv_nhdplus_h.networknhdflowline a
         WHERE
         b.hydroseq = p_hydroseq;
         
         IF str_direction = 'D'
         THEN
            out_measure         := out_tmeasure - num_end_of_line;
      
         ELSIF str_direction = 'U'
         THEN  
            out_measure         := out_fmeasure + num_end_of_line;
      
         END IF;
         
      ELSE
         SELECT 
          a.nhdplusid
         ,a.permanent_identifier 
         ,a.reachcode
         ,ROUND(a.frommeas::NUMERIC,5) AS fmeasure
         ,ROUND(a.tomeas::NUMERIC,5)   AS tmeasure
         ,a.hydroseq
         ,a.uphydroseq
         ,a.terminalpa
         INTO
          out_nhdplusid
         ,out_permanent_identifier 
         ,out_reachcode
         ,out_fmeasure
         ,out_tmeasure
         ,out_hydroseq
         ,out_uphydroseq
         ,out_terminalpa
         FROM 
         cipsrv_nhdplus_h.networknhdflowline a
         WHERE
             b.hydroseq = p_hydroseq
         AND (
            a.frommeas = p_measure
            OR
            (a.frommeas < p_measure AND a.tomeas >= p_measure)
         );
         
         out_measure := p_measure;
         
         IF str_direction = 'D'
         THEN
            IF  p_measure = out_fmeasure
            AND out_hydroseq IS NOT NULL
            AND out_hydroseq = out_terminalpa
            THEN
               out_measure := out_fmeasure + num_end_of_line;
            
            END IF;
            
         ELSIF str_direction = 'U'
         THEN  
            IF p_measure = out_tmeasure
            AND out_uphydroseq = 0
            THEN
               out_measure := out_tmeasure - num_end_of_line;
            
            END IF;
            
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
         ,a.permanent_identifier 
         ,a.reachcode
         ,ROUND(a.frommeas::NUMERIC,5) AS fmeasure
         ,ROUND(a.tomeas::NUMERIC,5)   AS tmeasure
         ,a.hydroseq
         ,a.uphydroseq
         ,a.terminalpa
         INTO
          out_nhdplusid
         ,out_permanent_identifier 
         ,out_reachcode
         ,out_fmeasure
         ,out_tmeasure
         ,out_hydroseq
         ,out_uphydroseq
         ,out_terminalpa
         FROM 
         cipsrv_nhdplus_h.networknhdflowline a
         WHERE 
             a.reachcode = p_reachcode 
         AND (
               (str_direction = 'D' AND a.tomeas = 100)
            OR (str_direction = 'U' AND a.frommeas = 0 )
         );
         
         IF str_direction = 'D'
         THEN
            out_measure         := 100;
            
         ELSIF str_direction = 'U'
         THEN
            out_measure         := 0;
         
         ELSE
            RAISE EXCEPTION 'err';
            
         END IF;
         
      ELSE
         SELECT 
          a.nhdplusid
         ,a.permanent_identifier 
         ,a.reachcode
         ,ROUND(a.frommeas::NUMERIC,5) AS fmeasure
         ,ROUND(a.tomeas::NUMERIC,5)   AS tmeasure
         ,a.hydroseq
         ,a.uphydroseq
         ,a.terminalpa
         INTO
          out_nhdplusid
         ,out_permanent_identifier 
         ,out_reachcode
         ,out_fmeasure
         ,out_tmeasure
         ,out_hydroseq
         ,out_uphydroseq
         ,out_terminalpa
         FROM 
         cipsrv_nhdplus_h.networknhdflowline a
         WHERE 
             a.reachcode = p_reachcode 
         AND (
            (p_measure = 0 AND a.frommeas = 0)
            OR
            (a.frommeas < p_measure AND a.tomeas >= p_measure)
         );
         
         out_measure := p_measure;
         
         IF str_direction = 'D'
         THEN
            IF  p_measure = out_fmeasure
            AND out_hydroseq IS NOT NULL
            AND out_hydroseq = out_terminalpa
            THEN
               out_measure := out_fmeasure + num_end_of_line;
            
            END IF;
            
         ELSIF str_direction = 'U'
         THEN
            IF p_measure = out_tmeasure
            AND out_uphydroseq = 0
            THEN
               out_measure := out_tmeasure - num_end_of_line;
            
            END IF;

         ELSE
            RAISE EXCEPTION 'err';
            
         END IF;
 
      END IF;
    
   END IF;
   
EXCEPTION

   WHEN NO_DATA_FOUND
   THEN
      out_return_code    := -10;
      out_status_message := 'no results found in cipsrv_nhdplus_h networknhdflowline';
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
   WHERE p.oid::regproc::text = 'cipsrv_nhdplus_h.get_measure';
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

