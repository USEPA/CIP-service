DROP MATERIALIZED VIEW IF EXISTS cipsrv_nhdplus_h.nhdpluscatchment_5070 CASCADE;

CREATE MATERIALIZED VIEW cipsrv_nhdplus_h.nhdpluscatchment_5070(
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
   ,ST_TRANSFORM(aa.shape,5070) AS shape
   FROM   
   cipsrv_nhdplus_h.nhdpluscatchment aa
   WHERE
   aa.shape && cipsrv_nhdplus_h.generic_common_mbr('5070')
   AND cipsrv_nhdplus_h.determine_grid_srid_f(aa.shape) = 5070
) a;

ALTER TABLE cipsrv_nhdplus_h.nhdpluscatchment_5070 OWNER TO cipsrv;
GRANT SELECT ON cipsrv_nhdplus_h.nhdpluscatchment_5070 TO public;

CREATE UNIQUE INDEX IF NOT EXISTS nhdpluscatchment_5070_pk
ON cipsrv_nhdplus_h.nhdpluscatchment_5070(nhdplusid);

CREATE UNIQUE INDEX IF NOT EXISTS nhdpluscatchment_5070_u01
ON cipsrv_nhdplus_h.nhdpluscatchment_5070(objectid);

CREATE UNIQUE INDEX IF NOT EXISTS nhdpluscatchment_5070_u02
ON cipsrv_nhdplus_h.nhdpluscatchment_5070(globalid);

CREATE INDEX IF NOT EXISTS nhdpluscatchment_5070_spx
ON cipsrv_nhdplus_h.nhdpluscatchment_5070 USING gist(shape);

ANALYZE cipsrv_nhdplus_h.nhdpluscatchment_5070;

--VACUUM FREEZE ANALYZE cipsrv_nhdplus_h.nhdpluscatchment_5070;

