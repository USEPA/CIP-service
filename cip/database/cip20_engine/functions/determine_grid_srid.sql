CREATE OR REPLACE FUNCTION cip20_engine.determine_grid_srid(
    IN  p_geometry             GEOMETRY
   ,IN  p_nhdplus_version      VARCHAR
   ,IN  p_known_region         VARCHAR
   ,OUT out_srid               INTEGER
   ,OUT out_grid_size          NUMERIC
   ,OUT out_return_code        INTEGER
   ,OUT out_status_message     VARCHAR
)
STABLE
AS $BODY$ 
DECLARE
   rec RECORD;
   
BEGIN

   IF p_nhdplus_version = 'nhdplus_m'
   THEN
      rec := cip20_nhdplus_m.determine_grid_srid(
          p_geometry          := p_geometry
         ,p_known_region      := p_known_region
      );
      out_srid           := rec.out_srid;
      out_grid_size      := rec.out_grid_size;
      out_return_code    := rec.out_return_code;
      out_status_message := rec.out_status_message;
   
   ELSIF p_nhdplus_version = 'nhdplus_h'
   THEN
      rec := cip20_nhdplus_h.determine_grid_srid(
          p_geometry          := p_geometry
         ,p_known_region      := p_known_region
      );
      out_srid           := rec.out_srid;
      out_grid_size      := rec.out_grid_size;
      out_return_code    := rec.out_return_code;
      out_status_message := rec.out_status_message;

   ELSE
      RAISE EXCEPTION 'err %',p_nhdplus_version;

   END IF;   
   
END;
$BODY$
LANGUAGE plpgsql;

ALTER FUNCTION cip20_engine.determine_grid_srid(
    GEOMETRY
   ,VARCHAR
   ,VARCHAR
) OWNER TO cip20;

GRANT EXECUTE ON FUNCTION cip20_engine.determine_grid_srid(
    GEOMETRY
   ,VARCHAR
   ,VARCHAR
) TO PUBLIC;

