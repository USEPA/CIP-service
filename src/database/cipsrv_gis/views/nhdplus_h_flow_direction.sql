DROP VIEW IF EXISTS cipsrv_gis.nhdplus_h_flow_direction;

DO $$DECLARE 
BEGIN

   IF cipsrv_gis.resource_exists('cipsrv_nhdplus_h','networknhdflowline') 
   THEN

      EXECUTE $q$
      CREATE OR REPLACE VIEW cipsrv_gis.nhdplus_h_flow_direction
      AS
      SELECT
       a.objectid
      ,a.permanent_identifier
      ,CAST(DEGREES(
          ST_Azimuth(
             a.penul_point
            ,a.final_point
          )
       ) AS NUMERIC) AS angle_direction
      ,CAST(ROUND(DEGREES(
          ST_Azimuth(
             a.penul_point
            ,a.final_point
          )
       ) / 5 ) * 5 AS NUMERIC ) AS angle_rounded
      ,a.ftype
      ,a.fcode
      ,a.visibilityfilter
      ,a.nhdplusid
      ,a.final_point AS shape
      FROM (
         SELECT  
          aa.objectid
         ,aa.permanent_identifier
         ,aa.ftype
         ,aa.fcode
         ,aa.visibilityfilter
         ,aa.nhdplusid
         ,ST_PointN(aa.shape,-2) AS penul_point
         ,ST_PointN(aa.shape,-1) AS final_point
         FROM (
            SELECT
             aaa.objectid
            ,aaa.permanent_identifier
            ,aaa.ftype
            ,aaa.fcode
            ,aaa.visibilityfilter
            ,aaa.nhdplusid
            ,ST_Force2D(aaa.shape) AS shape
            FROM
            cipsrv_nhdplus_h.networknhdflowline aaa
         ) aa
      ) a;
      $q$;                                                                                                                          

      EXECUTE $q$
      ALTER TABLE cipsrv_gis.nhdplus_h_flow_direction OWNER TO cipsrv_gis;
      $q$;                                                                                                                          

      EXECUTE $q$
      GRANT SELECT ON cipsrv_gis.nhdplus_h_flow_direction TO public;
      $q$;
      
   ELSE
      RAISE WARNING 'skipping cipsrv_gis.nhdplus_h_flow_direction';
   
   END IF;

END$$;
