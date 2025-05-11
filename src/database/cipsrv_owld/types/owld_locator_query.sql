DROP TYPE IF EXISTS cipsrv_owld.owld_locator_query CASCADE;

CREATE TYPE cipsrv_owld.owld_locator_query 
AS(
    program_id          VARCHAR
   ,program_name        VARCHAR
   ,program_short_name  VARCHAR
   ,program_description VARCHAR
   ,program_url         VARCHAR
   ,program_eventtype   INTEGER
   ,program_vintage     DATE
   ,program_resolutions VARCHAR[]
   ,program_precisions  VARCHAR[]
);

ALTER TYPE cipsrv_owld.owld_locator_query OWNER TO cipsrv;

GRANT USAGE ON TYPE cipsrv_owld.owld_locator_query TO PUBLIC;

