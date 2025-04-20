DROP MATERIALIZED VIEW IF EXISTS cipsrv_nhdplus_h.nhdpluscatchment_26904 CASCADE;

CREATE MATERIALIZED VIEW cipsrv_nhdplus_h.nhdpluscatchment_26904(
    objectid
   ,nhdplusid
   ,sourcefc
   ,gridcode
   ,areasqkm
   ,vpuid
   ,globalid
   ,shape
)
AS
SELECT
 ROW_NUMBER() OVER()                  AS objectid
,a.nhdplusid::BIGINT                  AS nhdplusid
,a.sourcefc
,a.gridcode
,a.areasqkm
,a.vpuid
,'{' || uuid_generate_v1() || '}'     AS globalid
,a.shape
FROM (
   SELECT
    aa.nhdplusid
   ,aa.sourcefc
   ,aa.gridcode
   ,aa.areasqkm
   ,aa.vpuid
   ,ST_TRANSFORM(aa.shape,26904) AS shape
   FROM   
   cipsrv_nhdplus_h.nhdpluscatchment aa
   WHERE
   aa.shape && cipsrv_nhdplus_h.generic_common_mbr('26904')
   AND cipsrv_nhdplus_h.determine_grid_srid_f(aa.shape) = 26904
) a;

ALTER TABLE cipsrv_nhdplus_h.nhdpluscatchment_26904 OWNER TO cipsrv;
GRANT SELECT ON cipsrv_nhdplus_h.nhdpluscatchment_26904 TO public;

CREATE UNIQUE INDEX IF NOT EXISTS nhdpluscatchment_26904_pk
ON cipsrv_nhdplus_h.nhdpluscatchment_26904(nhdplusid);

CREATE UNIQUE INDEX IF NOT EXISTS nhdpluscatchment_26904_u01
ON cipsrv_nhdplus_h.nhdpluscatchment_26904(objectid);

CREATE UNIQUE INDEX IF NOT EXISTS nhdpluscatchment_26904_u02
ON cipsrv_nhdplus_h.nhdpluscatchment_26904(globalid);

CREATE INDEX IF NOT EXISTS nhdpluscatchment_26904_spx
ON cipsrv_nhdplus_h.nhdpluscatchment_26904 USING gist(shape);

ANALYZE cipsrv_nhdplus_h.nhdpluscatchment_26904;

--VACUUM FREEZE ANALYZE cipsrv_nhdplus_h.nhdpluscatchment_26904;

