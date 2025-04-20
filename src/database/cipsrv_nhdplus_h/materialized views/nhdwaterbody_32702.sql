DROP MATERIALIZED VIEW IF EXISTS cipsrv_nhdplus_h.nhdwaterbody_32702 CASCADE;

DROP SEQUENCE IF EXISTS cipsrv_nhdplus_h.nhdwaterbody_32702_seq;
CREATE SEQUENCE cipsrv_nhdplus_h.nhdwaterbody_32702_seq START WITH 1;

CREATE MATERIALIZED VIEW cipsrv_nhdplus_h.nhdwaterbody_32702(
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
 NEXTVAL('cipsrv_nhdplus_h.nhdwaterbody_32702_seq') AS objectid
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
   ,ST_TRANSFORM(aa.shape,32702) AS shape
   FROM   
   cipsrv_nhdplus_h.nhdwaterbody aa
   WHERE
   aa.shape && cipsrv_nhdplus_h.generic_common_mbr('32702')
   AND cipsrv_nhdplus_h.determine_grid_srid_f(aa.shape) = 32702
) a;

ALTER TABLE cipsrv_nhdplus_h.nhdwaterbody_32702 OWNER TO cipsrv;
GRANT SELECT ON cipsrv_nhdplus_h.nhdwaterbody_32702 TO public;

CREATE UNIQUE INDEX IF NOT EXISTS nhdwaterbody_32702_pk
ON cipsrv_nhdplus_h.nhdwaterbody_32702(nhdplusid);

CREATE UNIQUE INDEX IF NOT EXISTS nhdwaterbody_32702_u01
ON cipsrv_nhdplus_h.nhdwaterbody_32702(objectid);

CREATE UNIQUE INDEX IF NOT EXISTS nhdwaterbody_32702_u02
ON cipsrv_nhdplus_h.nhdwaterbody_32702(globalid);

CREATE INDEX IF NOT EXISTS nhdwaterbody_32702_spx
ON cipsrv_nhdplus_h.nhdwaterbody_32702 USING gist(shape);

ANALYZE cipsrv_nhdplus_h.nhdwaterbody_32702;

--VACUUM FREEZE ANALYZE cipsrv_nhdplus_h.nhdwaterbody_32702;

