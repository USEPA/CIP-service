CREATE OR REPLACE FUNCTION cip20_engine.determine_grid_srid(
    IN  p_geometry             GEOMETRY
   ,IN  p_nhdplus_version      VARCHAR
   ,IN  p_known_region         VARCHAR
   ,OUT p_srid                 INTEGER
   ,OUT p_grid_size            NUMERIC
   ,OUT p_return_code          INTEGER
   ,OUT p_status_message       VARCHAR
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
      p_srid           := rec.p_srid;
      p_grid_size      := rec.p_grid_size;
      p_return_code    := rec.p_return_code;
      p_status_message := rec.p_status_message;
   
   ELSIF p_nhdplus_version = 'nhdplus_h'
   THEN
      rec := cip20_nhdplus_h.determine_grid_srid(
          p_geometry          := p_geometry
         ,p_known_region      := p_known_region
      );
      p_srid           := rec.p_srid;
      p_grid_size      := rec.p_grid_size;
      p_return_code    := rec.p_return_code;
      p_status_message := rec.p_status_message;

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

