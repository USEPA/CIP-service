DROP TYPE IF EXISTS cipsrv_nhdplus_h.flowline CASCADE;

CREATE TYPE cipsrv_nhdplus_h.flowline 
AS(
    nhdplusid               BIGINT
   ,hydroseq                BIGINT
   ,fmeasure                NUMERIC
   ,tmeasure                NUMERIC
   ,levelpathi              BIGINT
   ,terminalpa              BIGINT
   ,uphydroseq              BIGINT
   ,dnhydroseq              BIGINT
   ,dnminorhyd              BIGINT
   ,divergence              INTEGER
   ,streamleve              INTEGER
   ,arbolatesu              NUMERIC
   ,fromnode                BIGINT
   ,tonode                  BIGINT
   ,vpuid                   VARCHAR(8)
   /* ++++++++++ */
   ,permanent_identifier    VARCHAR(40)
   ,reachcode               VARCHAR(14)
   ,fcode                   INTEGER
   /* ++++++++++ */
   ,lengthkm                NUMERIC
   ,lengthkm_ratio          NUMERIC
   ,flowtimeday             NUMERIC
   ,flowtimeday_ratio       NUMERIC
   /* ++++++++++ */
   ,pathlengthkm            NUMERIC
   ,pathflowtimeday         NUMERIC
   /* ++++++++++ */
   ,out_grid_srid           INTEGER
   ,out_measure             NUMERIC
   ,out_lengthkm            NUMERIC
   ,out_flowtimeday         NUMERIC
   ,out_pathlengthkm        NUMERIC
   ,out_pathflowtimeday     NUMERIC
   ,out_node                BIGINT
   /* ++++++++++ */
   ,network_distancekm      NUMERIC
   ,network_flowtimeday     NUMERIC
   ,pathlength_adj          NUMERIC
   ,pathflowtime_adj        NUMERIC
   /* ++++++++++ */
   ,nav_order               INTEGER
);

ALTER TYPE cipsrv_nhdplus_h.flowline OWNER TO cipsrv;

GRANT USAGE ON TYPE cipsrv_nhdplus_h.flowline TO PUBLIC;

