DROP MATERIALIZED VIEW IF EXISTS cipsrv_nhdplus_h.nhdwaterbody_26904 CASCADE;

DROP SEQUENCE IF EXISTS cipsrv_nhdplus_h.nhdwaterbody_26904_seq;
CREATE SEQUENCE cipsrv_nhdplus_h.nhdwaterbody_26904_seq START WITH 1;

CREATE MATERIALIZED VIEW cipsrv_nhdplus_h.nhdwaterbody_26904(
    objectid
   ,permanent_identifier
   ,fdate
   ,resolution
   ,gnis_id
   ,gnis_name
   ,areasqkm
   ,elevation
   ,reachcode
   ,ftype
   ,fcode
   ,visibilityfilter
   ,nhdplusid
   ,vpuid
   ,onoffnet
   ,purpcode
   ,burn
   ,globalid
   ,shape
)
AS
SELECT
 NEXTVAL('cipsrv_nhdplus_h.nhdwaterbody_26904_seq') AS objectid
,a.permanent_identifier
,a.fdate
,a.resolution
,a.gnis_id
,a.gnis_name
,a.areasqkm
,a.elevation
,a.reachcode
,a.ftype
,a.fcode
,a.visibilityfilter
,a.nhdplusid
,a.vpuid
,a.onoffnet
,a.purpcode
,a.burn
,'{' || uuid_generate_v1() || '}'     AS globalid
,a.shape
FROM (
   SELECT
    aa.permanent_identifier
   ,aa.fdate
   ,aa.resolution
   ,aa.gnis_id
   ,aa.gnis_name
   ,aa.areasqkm
   ,aa.elevation
   ,aa.reachcode
   ,aa.ftype
   ,aa.fcode
   ,aa.visibilityfilter
   ,aa.nhdplusid
   ,aa.vpuid
   ,aa.onoffnet
   ,aa.purpcode
   ,aa.burn
   ,ST_TRANSFORM(aa.shape,26904) AS shape
   FROM   
   cipsrv_nhdplus_h.nhdwaterbody aa
   WHERE
   aa.shape && cipsrv_nhdplus_h.generic_common_mbr('26904')
   AND cipsrv_nhdplus_h.determine_grid_srid_f(aa.shape) = 26904
) a;

ALTER TABLE cipsrv_nhdplus_h.nhdwaterbody_26904 OWNER TO cipsrv;
GRANT SELECT ON cipsrv_nhdplus_h.nhdwaterbody_26904 TO public;

CREATE UNIQUE INDEX IF NOT EXISTS nhdwaterbody_26904_pk
ON cipsrv_nhdplus_h.nhdwaterbody_26904(nhdplusid);

CREATE UNIQUE INDEX IF NOT EXISTS nhdwaterbody_26904_u01
ON cipsrv_nhdplus_h.nhdwaterbody_26904(objectid);

CREATE UNIQUE INDEX IF NOT EXISTS nhdwaterbody_26904_u02
ON cipsrv_nhdplus_h.nhdwaterbody_26904(globalid);

CREATE INDEX IF NOT EXISTS nhdwaterbody_26904_spx
ON cipsrv_nhdplus_h.nhdwaterbody_26904 USING gist(shape);

ANALYZE cipsrv_nhdplus_h.nhdwaterbody_26904;

--VACUUM FREEZE ANALYZE cipsrv_nhdplus_h.nhdwaterbody_26904;

