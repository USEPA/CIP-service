DROP TYPE IF EXISTS cipsrv_nhdplus_m.snapflowline CASCADE;

CREATE TYPE cipsrv_nhdplus_m.snapflowline 
AS(
    permanent_identifier        VARCHAR(40)
   ,fdate                       DATE
   ,resolution                  INTEGER
   ,gnis_id                     VARCHAR(10)
   ,gnis_name                   VARCHAR(65)
   ,lengthkm                    NUMERIC
   ,reachcode                   VARCHAR(14)
   ,flowdir                     INTEGER
   ,wbarea_permanent_identifier VARCHAR(40)
   ,ftype                       INTEGER
   ,fcode                       INTEGER
   ,mainpath                    INTEGER
   ,innetwork                   INTEGER
   ,visibilityfilter            INTEGER
   ,nhdplusid                   BIGINT
   ,vpuid                       VARCHAR(16)
   ,enabled                     INTEGER
   ,fmeasure                    NUMERIC
   ,tmeasure                    NUMERIC
   ,hydroseq                    BIGINT
   ,shape                       GEOMETRY
   -----
   ,snap_measure                NUMERIC
   ,snap_distancekm             NUMERIC
   ,snap_point                  GEOMETRY
);

ALTER TYPE cipsrv_nhdplus_m.snapflowline OWNER TO cipsrv;

GRANT USAGE ON TYPE cipsrv_nhdplus_m.snapflowline TO PUBLIC;

