CREATE OR REPLACE FUNCTION cip20_nhdplus_m.index_point(
    IN  p_geometry             GEOMETRY
   ,IN  p_known_region         VARCHAR
   ,OUT p_return_code          INTEGER
   ,OUT p_status_message       VARCHAR
)
VOLATILE
AS $BODY$
DECLARE
   rec                 RECORD;
   str_known_region    VARCHAR;
   int_srid            INTEGER;
   geom_input          GEOMETRY;

BEGIN

   str_known_region := p_known_region;

   rec := cip20_nhdplus_m.determine_grid_srid(
       p_geometry      := p_geometry
      ,p_known_region  := p_known_region
   );
   int_srid         := rec.p_srid;
   p_return_code    := rec.p_return_code;
   p_status_message := rec.p_status_message;
   
   IF p_return_code != 0
   THEN
      RETURN;
      
   END IF;
   
   str_known_region := int_srid::VARCHAR;
      
   IF str_known_region = '5070'
   THEN
      geom_input := ST_Transform(p_geometry,5070);
      
      INSERT INTO tmp_cip(
         nhdplusid
      ) 
      SELECT 
      a.nhdplusid
      FROM
      cip20_nhdplus_m.catchment_5070 a
      WHERE
      ST_Intersects(
          a.shape
         ,geom_input
      )
      ON CONFLICT DO NOTHING;
   
   ELSIF str_known_region = '3338'
   THEN
      geom_input := ST_Transform(p_geometry,3338);
      
      INSERT INTO tmp_cip(
         nhdplusid
      ) 
      SELECT 
      a.nhdplusid
      FROM
      cip20_nhdplus_m.catchment_3338 a
      WHERE
      ST_Intersects(
          a.shape
         ,geom_input
      )
      ON CONFLICT DO NOTHING;
   
   ELSIF str_known_region = '26904'
   THEN
      geom_input := ST_Transform(p_geometry,26904);
      
      INSERT INTO tmp_cip(
         nhdplusid
      ) 
      SELECT 
      a.nhdplusid
      FROM
      cip20_nhdplus_m.catchment_26904 a
      WHERE
      ST_Intersects(
          a.shape
         ,geom_input
      )
      ON CONFLICT DO NOTHING;
      
   ELSIF str_known_region = '32161'
   THEN
      geom_input := ST_Transform(p_geometry,32161);
      
      INSERT INTO tmp_cip(
         nhdplusid
      ) 
      SELECT 
      a.nhdplusid
      FROM
      cip20_nhdplus_m.catchment_32161 a
      WHERE
      ST_Intersects(
          a.shape
         ,geom_input
      )
      ON CONFLICT DO NOTHING;
      
   ELSIF str_known_region = '32655'
   THEN
      geom_input := ST_Transform(p_geometry,32655);
      
      INSERT INTO tmp_cip(
         nhdplusid
      ) 
      SELECT 
      a.nhdplusid
      FROM
      cip20_nhdplus_m.catchment_32655 a
      WHERE
      ST_Intersects(
          a.shape
         ,geom_input
      )
      ON CONFLICT DO NOTHING;
      
   ELSIF str_known_region = '32702'
   THEN
      geom_input := ST_Transform(p_geometry,32702);
      
      INSERT INTO tmp_cip(
         nhdplusid
      ) 
      SELECT 
      a.nhdplusid
      FROM
      cip20_nhdplus_m.catchment_32702 a
      WHERE
      ST_Intersects(
          a.shape
         ,geom_input
      )
      ON CONFLICT DO NOTHING;
   
   ELSE
      p_return_code    := -10;
      p_status_message := 'err ' || str_known_region;
      
   END IF;
   
   RETURN;
   
END;
$BODY$
LANGUAGE plpgsql;

ALTER FUNCTION cip20_nhdplus_m.index_point(
    GEOMETRY
   ,VARCHAR
) OWNER TO cip20;

GRANT EXECUTE ON FUNCTION cip20_nhdplus_m.index_point(
    GEOMETRY
   ,VARCHAR
) TO PUBLIC;

