DO $$DECLARE 
   a VARCHAR;b VARCHAR;
BEGIN
   SELECT p.oid::regproc,pg_get_function_identity_arguments(p.oid)
   INTO a,b FROM pg_catalog.pg_proc p LEFT JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace
   WHERE p.oid::regproc::text = 'cipsrv_engine.get_measure';
   IF b IS NOT NULL THEN 
   EXECUTE FORMAT('DROP FUNCTION IF EXISTS %s(%s)',a,b);ELSE
   IF a IS NOT NULL THEN EXECUTE FORMAT('DROP FUNCTION IF EXISTS %s',a);END IF;END IF;
END$$;

CREATE OR REPLACE FUNCTION cipsrv_engine.get_measure(
    IN  p_nhdplus_version              VARCHAR
   ,IN  p_direction                    VARCHAR DEFAULT NULL
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
   
BEGIN

   --------------------------------------------------------------------------
   -- Step 10
   -- Check over incoming parameters
   --------------------------------------------------------------------------
   IF UPPER(p_nhdplus_version) IN ('MR','NHDPLUS_M')
   THEN
      rec := cipsrv_nhdplus_m.get_measure(
          p_direction             := p_direction
         ,p_nhdplusid             := p_nhdplusid
         ,p_permanent_identifier  := p_permanent_identifier
         ,p_reachcode             := p_reachcode
         ,p_hydroseq              := p_hydroseq
         ,p_measure               := p_measure
      );
      
   ELSIF UPPER(p_nhdplus_version) IN ('HR','NHDPLUS_H')
   THEN
      rec := cipsrv_nhdplus_h.get_measure(
          p_direction             := p_direction
         ,p_nhdplusid             := p_nhdplusid
         ,p_permanent_identifier  := p_permanent_identifier
         ,p_reachcode             := p_reachcode
         ,p_hydroseq              := p_hydroseq
         ,p_measure               := p_measure
      );
   
   ELSE
      RAISE EXCEPTION 'unknown nhdplus version %',p_nhdplus_version;
   
   END IF;
   
   out_measure              := rec.out_measure;
   out_nhdplusid            := rec.out_nhdplusid;
   out_permanent_identifier := rec.out_permanent_identifier;
   out_reachcode            := rec.out_reachcode;
   out_fmeasure             := rec.out_fmeasure;
   out_tmeasure             := rec.out_tmeasure;
   out_hydroseq             := rec.out_hydroseq;
   out_uphydroseq           := rec.out_uphydroseq;
   out_terminalpa           := rec.out_terminalpa;
   out_return_code          := rec.out_return_code;
   out_status_message       := rec.out_status_message;

END;
$BODY$
LANGUAGE plpgsql;

DO $$DECLARE 
   a VARCHAR;b VARCHAR;
BEGIN
   SELECT p.oid::regproc,pg_get_function_identity_arguments(p.oid)
   INTO a,b FROM pg_catalog.pg_proc p LEFT JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace
   WHERE p.oid::regproc::text = 'cipsrv_engine.get_measure';
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

