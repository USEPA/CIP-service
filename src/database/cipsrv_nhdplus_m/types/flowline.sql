DROP TYPE IF EXISTS cipsrv_nhdplus_m.flowline CASCADE;

CREATE TYPE cipsrv_nhdplus_m.flowline 
AS(
    nhdplusid             BIGINT
   ,permanent_identifier  VARCHAR(40)
   ,reachcode             VARCHAR(14)
   ,fcode                 INTEGER
   ,fmeasure              NUMERIC
   ,tmeasure              NUMERIC
   ,hydroseq              BIGINT
   ,levelpathi            BIGINT
   ,dnhydroseq            BIGINT
   ,dnminorhyd            BIGINT
   ,uphydroseq            BIGINT
   ,divergence            INTEGER
   ,streamleve            INTEGER
   ,arbolatesu            NUMERIC
   ,terminalpa            BIGINT
   ,catchment_nhdplusid   BIGINT
   ,innetwork             BOOLEAN
   ,coastal               BOOLEAN
   ,navigable             BOOLEAN
   ,lengthkm              NUMERIC
   ,lengthkm_ratio        NUMERIC
   ,flowtimeday           NUMERIC
   ,flowtimeday_ratio     NUMERIC
   ,pathlengthkm          NUMERIC
   ,pathflowtimeday       NUMERIC
   ,fromnode              BIGINT
   ,tonode                BIGINT
   ,vpuid                 VARCHAR(8)
   ,out_grid_srid         INTEGER
   ,out_measure           NUMERIC
   ,out_lengthkm          NUMERIC
   ,out_flowtimeday       NUMERIC
   ,out_pathlengthkm      NUMERIC
   ,out_pathflowtimeday   NUMERIC
   ,out_node              BIGINT
);

ALTER TYPE cipsrv_nhdplus_m.flowline OWNER TO cipsrv;

GRANT USAGE ON TYPE cipsrv_nhdplus_m.flowline TO PUBLIC;

