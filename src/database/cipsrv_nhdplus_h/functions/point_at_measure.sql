CREATE OR REPLACE FUNCTION cipsrv_nhdplus_h.point_at_measure(
    IN  p_nhdplusid               INTEGER
   ,IN  p_permanent_identifier    VARCHAR
   ,IN  p_reachcode               VARCHAR 
   ,IN  p_measure                 NUMERIC
   ,IN  p_2d_flag                 BOOLEAN DEFAULT TRUE
) RETURNS GEOMETRY
STABLE
AS
$BODY$ 
DECLARE
   sdo_output  GEOMETRY;
   boo_2d_flag BOOLEAN := p_2d_flag;
   
BEGIN

   ----------------------------------------------------------------------------
   -- Step 10
   -- Check over incoming parameters
   ----------------------------------------------------------------------------
   IF p_measure IS NULL
   OR p_measure < -1
   OR p_measure > 101
   THEN
      RAISE EXCEPTION 'measure cannot be null and must be between -1 and 101';
      
   END IF;
   
   IF boo_2d_flag IS NULL
   THEN
      boo_2d_flag := TRUE;
      
   END IF;
   
   ----------------------------------------------------------------------------
   -- Step 20
   -- Grab the point if a normal measure
   ----------------------------------------------------------------------------
   IF p_measure >= 0 AND p_measure <= 100
   THEN
      IF p_nhdplusid IS NOT NULL
      THEN
         SELECT
         ST_GeometryN(ST_LocateAlong(a.shape,p_measure,0),1) AS shape
         INTO
         sdo_output
         FROM
         cipsrv_nhdplus_h.nhdflowline a
         WHERE
         a.nhdplusid = p_nhdplusid
         AND (
            p_measure = a.fmeasure
            OR
            (a.fmeasure < p_measure AND a.tmeasure >= p_measure)
         );
      
      ELSIF p_permanent_identifier IS NOT NULL
      THEN
         SELECT
         ST_GeometryN(ST_LocateAlong(a.shape,p_measure,0),1) AS shape
         INTO
         sdo_output
         FROM
         cipsrv_nhdplus_h.nhdflowline a
         WHERE
         a.permanent_identifier = p_permanent_identifier
         AND (
            p_measure = a.fmeasure
            OR
            (a.fmeasure < p_measure AND a.tmeasure >= p_measure)
         );
      
      ELSIF p_reachcode IS NOT NULL
      THEN
         SELECT
         ST_GeometryN(ST_LocateAlong(a.shape,p_measure,0),1) AS shape
         INTO
         sdo_output
         FROM
         cipsrv_nhdplus_h.nhdflowline a
         WHERE
         a.reachcode = p_reachcode
         AND (
            p_measure = a.fmeasure
            OR
            (a.fmeasure < p_measure AND a.tmeasure >= p_measure)
         );
         
      ELSE
         RAISE EXCEPTION 'nhdplusid, permanent_identifier or reachcode required';
         
      END IF;
      
   END IF;
   
   ----------------------------------------------------------------------------
   -- Step 30
   -- Grab the start point if measure = 101
   ----------------------------------------------------------------------------
   IF p_measure = 101
   THEN
      IF p_nhdplusid IS NOT NULL
      THEN
         SELECT
         ST_PointN(a.shape,1)
         INTO
         sdo_output
         FROM
         nhdplus.nhdflowline_np21 a
         WHERE
         a.nhdplusid = p_nhdplusid;
      
      ELSIF p_permanent_identifier IS NOT NULL
      THEN
         SELECT
         ST_PointN(a.shape,1)
         INTO
         sdo_output
         FROM
         nhdplus.nhdflowline_np21 a
         WHERE
         a.permanent_identifier = p_permanent_identifier;
      
      ELSIF p_reachcode IS NOT NULL
      THEN
         SELECT
         ST_PointN(a.shape,1)
         INTO
         sdo_output
         FROM
         nhdplus.nhdflowline_np21 a
         WHERE
             a.reachcode = p_reachcode
         AND a.tmeasure = 100;
         
      ELSE
         RAISE EXCEPTION 'nhdplusid, permanent_identifier or reachcode required';
         
      END IF;
      
   END IF;
   
   ----------------------------------------------------------------------------
   -- Step 40
   -- Grab the end point if measure = -1
   ----------------------------------------------------------------------------
   IF p_measure = -1
   THEN
      IF p_nhdplusid IS NOT NULL
      THEN
         SELECT
         ST_PointN(a.shape,ST_NPoints(a.shape))
         INTO
         sdo_output
         FROM
         nhdplus.nhdflowline_np21 a
         WHERE
         a.nhdplusid = p_nhdplusid;
      
      ELSIF p_permanent_identifier IS NOT NULL
      THEN
         SELECT
         ST_PointN(a.shape,ST_NPoints(a.shape))
         INTO
         sdo_output
         FROM
         nhdplus.nhdflowline_np21 a
         WHERE
         a.permanent_identifier = p_permanent_identifier;
      
      ELSIF p_reachcode IS NOT NULL
      THEN
         SELECT
         ST_PointN(a.shape,ST_NPoints(a.shape))
         INTO
         sdo_output
         FROM
         nhdplus.nhdflowline_np21 a
         WHERE
             a.reachcode = p_reachcode
         AND a.fmeasure = 0;
         
      ELSE
         RAISE EXCEPTION 'nhdplusid, permanent_identifier or reachcode required';
         
      END IF;
      
   END IF;
   
   ----------------------------------------------------------------------------
   -- Step 50
   -- Return what we got
   ----------------------------------------------------------------------------
   IF boo_2d_flag
   THEN
      RETURN ST_Force2D(sdo_output);
      
   ELSE
      RETURN sdo_output;
   
   END IF;
   
END;
$BODY$
LANGUAGE plpgsql;

ALTER FUNCTION cipsrv_nhdplus_h.point_at_measure(
    INTEGER
   ,VARCHAR
   ,VARCHAR
   ,NUMERIC
   ,BOOLEAN
) OWNER TO cipsrv;

GRANT EXECUTE ON FUNCTION cipsrv_nhdplus_h.point_at_measure(
    INTEGER
   ,VARCHAR
   ,VARCHAR
   ,NUMERIC
   ,BOOLEAN
) TO PUBLIC;

